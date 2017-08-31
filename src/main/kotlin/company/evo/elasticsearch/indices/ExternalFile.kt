package company.evo.elasticsearch.indices

import java.io.IOException
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.FileTime
import java.nio.file.NoSuchFileException

import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients

import org.elasticsearch.common.logging.Loggers
import org.elasticsearch.index.Index

import net.uaprom.htable.HashTable
import net.uaprom.htable.TrieHashTable
import java.nio.ByteBuffer


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
        private val values: Map<Long, Double>
) : FileValues {

    class Provider(
            private val values: Map<Long, Double>,
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
        return values.getOrDefault(key, defaultValue)
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
        private val dataDir: Path,
        private val index: Index,
        private val fieldName: String,
        private val settings: FileSettings)
{
    private val logger = Loggers.getLogger(ExternalFileService::class.java)

    internal fun download(): Boolean {
        val requestConfigBuilder = RequestConfig.custom()
        if (settings.timeout != null) {
            requestConfigBuilder
                    .setConnectTimeout(settings.timeout)
                    .setSocketTimeout(settings.timeout)
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
                    val tmpPath = Files.createTempFile(
                            getExternalFileDir(), fieldName, null)
                    resp.entity?.content?.use { inStream ->
                        Files.copy(inStream, tmpPath, StandardCopyOption.REPLACE_EXISTING)
                        Files.move(tmpPath, getExternalFilePath(), StandardCopyOption.ATOMIC_MOVE)
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
    }

    internal fun loadValues(lastModified: FileTime?): FileValues.Provider? {
        val extFilePath = getExternalFilePath()
        try {
            val fileLastModified = Files.getLastModifiedTime(extFilePath)
            if (fileLastModified > (lastModified ?: FileTime.fromMillis(0))) {
                when (settings.valuesStoreType) {
                    ValuesStoreType.RAM -> {
                        val (keys, values) = parse(extFilePath)
                        val map = HashMap<Long, Double>(keys.size)
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
                            val tmpPath = Files.createTempFile(dataDir, fieldName, null)
                            Files.newOutputStream(tmpPath).use {
                                it.write(data)
                            }
                            Files.move(tmpPath, indexFilePath, StandardCopyOption.ATOMIC_MOVE)
                            logger.info("Dumped ${keys.size} values (${data.size} bytes) " +
                                    "into file [${indexFilePath}]")
                        }
                        val mappedData = FileChannel.open(indexFilePath, StandardOpenOption.READ).use {
                            it.map(FileChannel.MapMode.READ_ONLY, 0, it.size())
                        }
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
                "for [$fieldName] field of [${index.name}] index " +
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

    internal fun getExternalFileDir(): Path {
        val dir = dataDir
                .resolve("external_files")
                .resolve(index.name)
        // TODO Move that into constructor
        Files.createDirectories(dir)
        return dir
    }

    internal fun getExternalFilePath(): Path {
        return getExternalFileDir()
                .resolve(fieldName + ".txt")
    }

    internal fun getVersionFilePath(): Path {
        return getExternalFileDir()
                .resolve(fieldName + ".ver")
    }

    internal fun getBinaryFilePath(): Path {
        return getExternalFileDir()
                .resolve(fieldName + ".amt")
    }
}
