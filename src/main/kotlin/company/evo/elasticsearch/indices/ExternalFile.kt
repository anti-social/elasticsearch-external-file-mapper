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
import java.util.*
import java.util.stream.LongStream

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

class ScheduleIntervals {
    private val intervals: Iterator<Long>

    constructor(initialInterval: Long, interval: Long, scatter: Long) {
        val halfScatter = scatter / 2
        val minInitialInterval = maxOf(initialInterval - halfScatter, 0)
        val firstInterval = Random()
                .longs(minInitialInterval, minInitialInterval + scatter + 1)
                .iterator()
                .next()
        val intervals = Random()
                .longs(maxOf(interval - halfScatter, 0),
                        interval + halfScatter + 1)
        this.intervals = LongStream.concat(LongStream.of(firstInterval), intervals)
                .iterator()
    }

    fun next(): Long {
        return this.intervals.next()
    }
}

data class FileSettings(
        val valuesStoreType: ValuesStoreType,
        val updateInterval: Long,
        val updateScatter: Long?,
        val scalingFactor: Long?,
        val url: String?,
        val timeout: Int?
) {
    fun isUpdateChanged(other: FileSettings): Boolean {
        return other.updateInterval != updateInterval ||
                other.updateScatter != updateScatter ||
                other.url != url ||
                other.timeout != timeout
    }

    fun isStoreChanged(other: FileSettings): Boolean {
        return other.scalingFactor != scalingFactor ||
                other.valuesStoreType != valuesStoreType
    }
}

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
                return when (settings.valuesStoreType) {
                    ValuesStoreType.RAM -> {
                        getMemoryValuesProvider(
                                fileLastModified, settings.scalingFactor)
                    }
                    ValuesStoreType.FILE -> {
                        getMappedFileValuesProvider(fileLastModified)
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
        val startAt = System.nanoTime()
        val maxRows = Files.newBufferedReader(path).use {
            var maxRows: Int? = null
            val header = it.readLine().trim()
            if (header.startsWith("#")) {
                for (attr in header.split(' ')) {
                    if (attr.isEmpty()) {
                        continue
                    }
                    val pair = attr.split('=', limit = 2)
                    if (pair.size == 2 && pair[0] == "rows") {
                        maxRows = pair[1].toInt()
                    }
                }
            }
            maxRows
        }

        var numRows = 0
        var maxKey = Long.MIN_VALUE
        var minValue = Double.POSITIVE_INFINITY
        var maxValue = Double.NEGATIVE_INFINITY
        var keys = LongArray(maxRows ?: 1000)
        var values = DoubleArray(maxRows ?: 1000)
        Files.newBufferedReader(path).use {
            it.lines()
                    .map { it.trim() }
                    .filter { !it.isEmpty() }
                    .filter { !it.startsWith("#") }
                    .iterator().withIndex().forEach {
                val i = it.index
                val line = it.value
                if (maxRows == null) {
                    if (i >= keys.size) {
                        keys = keys.copyOf(keys.size * 2)
                        values = values.copyOf(keys.size * 2)
                    }
                } else if (i >= maxRows) {
                    return@forEach
                }
                val delimiterIx = line.indexOf('=')
                val key = line.substring(0, delimiterIx).trim().toLong()
                val value = line.substring(delimiterIx + 1).trim().toDouble()
                keys[i] = key
                values[i] = value
                numRows = i + 1
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
        if (maxRows == null) {
            keys = keys.copyOf(numRows)
            values = values.copyOf(numRows)
        }
        val duration = (System.nanoTime() - startAt) / 1000_000
        logger.info("Parsed ${keys.size} values " +
                "for [$name] field of [${indexName}] index " +
                "from file [$path] for ${duration}ms")
        return ParsedValues(keys, values,
                numRows, maxKey, minValue, maxValue)
    }

    private fun getMemoryValuesProvider(
            lastModified: FileTime, scalingFactor: Long?): FileValues.Provider
    {
        val parsedValues = parse(getExternalFilePath())
        val valuesProvider = if (scalingFactor != null) {
            val minValue = (parsedValues.minValue * scalingFactor).toLong()
            val maxValue = (parsedValues.maxValue * scalingFactor).toLong()
            if (parsedValues.maxKey < Int.MAX_VALUE) {
                if (maxValue - minValue < Short.MAX_VALUE) {
                    MemoryIntShortFileValues.Provider(
                            parsedValues.keys, parsedValues.values,
                            minValue, scalingFactor, lastModified)
                } else if (maxValue - minValue < Int.MAX_VALUE) {
                    MemoryIntIntFileValues.Provider(
                            parsedValues.keys, parsedValues.values,
                            minValue, scalingFactor, lastModified)
                } else {
                    MemoryIntDoubleFileValues.Provider(
                            parsedValues.keys, parsedValues.values, lastModified)
                }
            } else {
                if (maxValue - minValue < Short.MAX_VALUE) {
                    MemoryLongShortFileValues.Provider(
                            parsedValues.keys, parsedValues.values,
                            minValue, scalingFactor, lastModified)
                } else if (maxValue - minValue < Int.MAX_VALUE) {
                    MemoryLongIntFileValues.Provider(
                            parsedValues.keys, parsedValues.values,
                            minValue, scalingFactor, lastModified)
                } else {
                    MemoryLongDoubleFileValues.Provider(
                            parsedValues.keys, parsedValues.values, lastModified)
                }
            }
        } else {
            MemoryLongDoubleFileValues.Provider(
                    parsedValues.keys, parsedValues.values, lastModified)
        }
        logger.debug("Values size is ${valuesProvider.sizeBytes} bytes")
        return valuesProvider
    }

    private fun getMappedFileValuesProvider(lastModified: FileTime): FileValues.Provider {
        val indexFilePath = getBinaryFilePath()
        val indexLastModified = try {
            Files.getLastModifiedTime(indexFilePath)
        } catch (e: NoSuchFileException) {
            FileTime.fromMillis(0)
        }
        if (indexLastModified < lastModified) {
            val parsedValues = parse(getExternalFilePath())
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
        val (mappedData, dataSize) = FileChannel.open(indexFilePath, StandardOpenOption.READ).use {
            Pair(it.map(FileChannel.MapMode.READ_ONLY, 0, it.size()), it.size())
        }
        logger.debug("Loaded values from file [$indexFilePath]")
        return MappedFileValues.Provider(mappedData, dataSize, lastModified)
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
