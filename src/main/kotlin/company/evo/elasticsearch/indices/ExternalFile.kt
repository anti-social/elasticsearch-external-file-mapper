package company.evo.elasticsearch.indices

import java.io.IOException
import java.net.SocketTimeoutException
import java.nio.channels.FileChannel
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.FileTime
import java.nio.file.NoSuchFileException

import gnu.trove.map.hash.TLongDoubleHashMap

import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients

import org.apache.logging.log4j.Logger
import org.apache.logging.log4j.LogManager

import net.uaprom.htable.HashTable
import net.uaprom.htable.TrieHashTable


internal data class FileKey(
        val indexName: String,
        val fieldName: String
)

interface FileValues {
    interface Provider {
        fun lastModified(): FileTime?
        fun get(): FileValues
    }
    fun get(key: Long, defaultValue: Double): Double
    fun contains(key: Long): Boolean
}

class EmptyFileValues : FileValues {
    override fun get(key: Long, defaultValue: Double): Double {
        return defaultValue
    }

    override fun contains(key: Long): Boolean {
        return false
    }
}

class MemoryFileValues(
        private val values: TLongDoubleHashMap
) : FileValues {

    class Provider(
            private val values: TLongDoubleHashMap,
            private val lastModified: FileTime
    ) : FileValues.Provider {
        override fun lastModified(): FileTime? {
            return lastModified
        }

        override fun get(): FileValues {
            return MemoryFileValues(values)
        }
    }

    override fun get(key: Long, defaultValue: Double): Double {
        val v = values.get(key)
        if (v.isNaN()) {
            return defaultValue
        }
        return v
    }

    override fun contains(key: Long): Boolean {
        return values.containsKey(key)
    }
}

class MappedFileValues(
        private val values: HashTable.Reader
) : FileValues {

    class Provider(
            private val data: ByteBuffer,
            private val lastModified: FileTime
    ) : FileValues.Provider {
        override fun lastModified(): FileTime? {
            return lastModified
        }

        override fun get(): FileValues {
            return MappedFileValues(TrieHashTable.Reader(data.slice()))
        }
    }

    override fun get(key: Long, defaultValue: Double): Double {
        return values.getDouble(key, defaultValue)
    }

    override fun contains(key: Long): Boolean {
        return values.exists(key)
    }
}

enum class ValuesStoreType {
    RAM,
    FILE,
}

data class FileSettings(
        val valuesStoreType: ValuesStoreType,
        val updateInterval: Long,
        val url: String?,
        val timeout: Int?
)

class ExternalFile(
        private val dir: Path,
        private val name: String,
        private val indexName: String,
        val settings: FileSettings,
        private val logger: Logger)
{
    constructor(dir: Path, name: String, indexName: String, settings: FileSettings) :
            this(dir, name, indexName, settings, LogManager.getLogger(ExternalFile::class.java))

    fun download(): Boolean {
        val requestConfigBuilder = RequestConfig.custom()
        if (settings.timeout != null) {
            val timeout = settings.timeout * 1000
            requestConfigBuilder
                    .setConnectTimeout(timeout)
                    .setSocketTimeout(timeout)
        }
        val requestConfig = requestConfigBuilder.build()
        val client = HttpClients.custom()
                .setDefaultRequestConfig(requestConfig)
                .build()
        val httpGet = HttpGet(settings.url)
        val ver = getCurrentVersion()
        if (ver != null) {
            httpGet.addHeader("If-Modified-Since", ver)
        }
        try {
            val resp = client.execute(httpGet)
            resp.use {
                when (resp.statusLine.statusCode) {
                    304 -> return false
                    200 -> {
                        val lastModified = resp.getLastHeader("Last-Modified").value
                        if (resp.entity == null) {
                            logger.warn("Missing content when downloading [${settings.url}]")
                            return false
                        }
                        val tmpPath = Files.createTempFile(dir, name, null)
                        try {
                            resp.entity?.content?.use { inStream ->
                                Files.copy(inStream, tmpPath, StandardCopyOption.REPLACE_EXISTING)
                                Files.move(tmpPath, getExternalFilePath(), StandardCopyOption.ATOMIC_MOVE)
                            }
                        } finally {
                            Files.deleteIfExists(tmpPath)
                        }
                        updateVersion(lastModified)
                        return true
                    }
                    else -> {
                        logger.warn("Failed to download [${settings.url}] with status: ${resp.statusLine}")
                        return false
                    }
                }
            }
        } catch (e: SocketTimeoutException) {
            logger.warn("Timeout when downloading [${settings.url}]: $e")
        } catch (e: IOException) {
            logger.warn("IO error when downloading [${settings.url}]: $e")
        }
        return false
    }

    fun loadValues(lastModified: FileTime?): FileValues.Provider? {
        val extFilePath = getExternalFilePath()
        try {
            val fileLastModified = Files.getLastModifiedTime(extFilePath)
            if (fileLastModified > (lastModified ?: FileTime.fromMillis(0))) {
                when (settings.valuesStoreType) {
                    ValuesStoreType.RAM -> {
                        val (keys, values) = parse(extFilePath)
                        val map = TLongDoubleHashMap(keys.size, 0.85F, -1, Double.NaN)
                        for ((ix, k) in keys.withIndex()) {
                            map.put(k, values[ix])
                        }
                        return MemoryFileValues.Provider(map, fileLastModified)
                    }
                    ValuesStoreType.FILE -> {
                        val indexFilePath = getBinaryFilePath()
                        val indexLastModified = try {
                            Files.getLastModifiedTime(indexFilePath)
                        } catch (e: NoSuchFileException) {
                            FileTime.fromMillis(0)
                        }
                        if (indexLastModified < fileLastModified) {
                            val (keys, values) = parse(extFilePath)
                            val writer = TrieHashTable.Writer(
                                    HashTable.ValueSize.LONG, TrieHashTable.BitmaskSize.LONG)
                            val data = writer.dumpDoubles(keys, values)
                            val tmpPath = Files.createTempFile(dir, name, null)
                            try {
                                Files.newOutputStream(tmpPath).use {
                                    it.write(data)
                                }
                                Files.move(tmpPath, indexFilePath, StandardCopyOption.ATOMIC_MOVE)
                            } finally {
                                Files.deleteIfExists(tmpPath)
                            }
                            logger.debug("Dumped ${keys.size} values (${data.size} bytes) " +
                                    "into file [${indexFilePath}]")
                        }
                        val mappedData = FileChannel.open(indexFilePath, StandardOpenOption.READ).use {
                            it.map(FileChannel.MapMode.READ_ONLY, 0, it.size())
                        }
                        logger.debug("Loaded values from file [$indexFilePath]")
                        return MappedFileValues.Provider(mappedData, fileLastModified)
                    }
                }
            }
        } catch (e: NoSuchFileException) {
        } catch (e: IOException) {
            logger.warn("Cannot read file [$extFilePath]: $e")
        }
        return null
    }

    private fun parse(path: Path): Pair<LongArray, DoubleArray> {
        val keys = ArrayList<Long>()
        val values = ArrayList<Double>()
        Files.newBufferedReader(path).use {
            for (rawLine in it.lines()) {
                val line = rawLine.trim()
                if (line == "") {
                    continue
                }
                if (line.startsWith("#")) {
                    continue
                }
                val delimiterIx = line.indexOf('=')
                val key = line.substring(0, delimiterIx).trim()
                val value = line.substring(delimiterIx + 1).trim()
                keys.add(key.toLong())
                values.add(value.toDouble())
            }
        }
        logger.info("Parsed ${keys.size} values " +
                "for [$name] field of [${indexName}] index " +
                "from file [$path]")
        return Pair(keys.toLongArray(), values.toDoubleArray())
    }

    internal fun getCurrentVersion(): String? {
        val versionPath = getVersionFilePath()
        try {
            Files.newBufferedReader(versionPath).use {
                return it.readLine()
            }
        } catch (e: NoSuchFileException) {
            return null
        } catch (e: IOException) {
            logger.warn("Cannot read file [$versionPath]: $e")
            return null
        }
    }

    internal fun updateVersion(ver: String) {
        val versionPath = getVersionFilePath()
        try {
            Files.newBufferedWriter(versionPath).use {
                it.write(ver)
            }
        } catch (e: IOException) {
            logger.warn("Cannot write file [$versionPath]: $e")
        }
    }

    internal fun getExternalFilePath(): Path {
        return dir.resolve("$name.txt")
    }

    internal fun getVersionFilePath(): Path {
        return dir.resolve("$name.ver")
    }

    internal fun getBinaryFilePath(): Path {
        return dir.resolve("$name.amt")
    }
}
