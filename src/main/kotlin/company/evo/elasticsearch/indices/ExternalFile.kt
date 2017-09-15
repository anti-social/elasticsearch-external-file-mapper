package company.evo.elasticsearch.indices

import java.io.IOException
import java.net.SocketTimeoutException
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

import org.apache.logging.log4j.Logger
import org.apache.logging.log4j.LogManager

import net.uaprom.htable.HashTable
import net.uaprom.htable.TrieHashTable


const val MAP_LOAD_FACTOR: Float = 0.75F

internal data class FileKey(
        val indexName: String,
        val fieldName: String
)

enum class ValuesStoreType {
    RAM,
    FILE,
}

data class FileSettings(
        val valuesStoreType: ValuesStoreType,
        val updateInterval: Long,
        val scalingFactor: Long?,
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
    private class ParsedValues(
            val keys: LongArray,
            val values: DoubleArray,
            val size: Int,
            val maxKey: Long,
            val minValue: Double,
            val maxValue: Double
    )

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
                logger.info("values store type: ${settings.valuesStoreType}")
                when (settings.valuesStoreType) {
                    ValuesStoreType.RAM -> {
                        val parsedValues = parse(extFilePath)
                        if (settings.scalingFactor != null) {
                            val scalingFactor = settings.scalingFactor
                            val minValue = (parsedValues.minValue * scalingFactor).toLong()
                            val maxValue = (parsedValues.maxValue * scalingFactor).toLong()
                            if (parsedValues.maxKey < Int.MAX_VALUE) {
                                if (maxValue - minValue < Short.MAX_VALUE) {
                                    return MemoryIntShortFileValues.Provider(
                                            parsedValues.keys, parsedValues.values,
                                            minValue, scalingFactor, fileLastModified)
                                } else if (maxValue - minValue < Int.MAX_VALUE) {
                                    return MemoryIntIntFileValues.Provider(
                                            parsedValues.keys, parsedValues.values,
                                            minValue, scalingFactor, fileLastModified)
                                }
                            }
                            if (maxValue - minValue < Short.MAX_VALUE) {
                                return MemoryLongShortFileValues.Provider(
                                        parsedValues.keys, parsedValues.values,
                                        minValue, scalingFactor, fileLastModified)
                            } else if (maxValue - minValue < Int.MAX_VALUE) {
                                return MemoryLongIntFileValues.Provider(
                                        parsedValues.keys, parsedValues.values,
                                        minValue, scalingFactor, fileLastModified)
                            }
                            return MemoryIntDoubleFileValues.Provider(
                                    parsedValues.keys, parsedValues.values, fileLastModified)
                        }
                        return MemoryLongDoubleFileValues.Provider(
                                parsedValues.keys, parsedValues.values, fileLastModified)
                    }
                    ValuesStoreType.FILE -> {
                        val indexFilePath = getBinaryFilePath()
                        val indexLastModified = try {
                            Files.getLastModifiedTime(indexFilePath)
                        } catch (e: NoSuchFileException) {
                            FileTime.fromMillis(0)
                        }
                        if (indexLastModified < fileLastModified) {
                            val parsedValues = parse(extFilePath)
                            val writer = TrieHashTable.Writer(
                                    HashTable.ValueSize.LONG, TrieHashTable.BitmaskSize.LONG)
                            val data = writer.dumpDoubles(parsedValues.keys, parsedValues.values)
                            val tmpPath = Files.createTempFile(dir, name, null)
                            try {
                                Files.newOutputStream(tmpPath).use {
                                    it.write(data)
                                }
                                Files.move(tmpPath, indexFilePath, StandardCopyOption.ATOMIC_MOVE)
                            } finally {
                                Files.deleteIfExists(tmpPath)
                            }
                            logger.debug("Dumped ${parsedValues.size} values (${data.size} bytes) " +
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

    private fun parse(path: Path): ParsedValues {
        var maxKey = Long.MIN_VALUE
        var minValue = Double.POSITIVE_INFINITY
        var maxValue = Double.NEGATIVE_INFINITY
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
                val key = line.substring(0, delimiterIx).trim().toLong()
                val value = line.substring(delimiterIx + 1).trim().toDouble()
                keys.add(key)
                values.add(value)
                if (key > maxKey) {
                    maxKey = key
                }
                if (value < minValue) {
                    minValue = value
                }
                if (value > maxValue) {
                    maxValue = value
                }
            }
        }
        logger.info("Parsed ${keys.size} values " +
                "for [$name] field of [${indexName}] index " +
                "from file [$path]")
        return ParsedValues(keys.toLongArray(), values.toDoubleArray(),
                keys.size, maxKey, minValue, maxValue)
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
