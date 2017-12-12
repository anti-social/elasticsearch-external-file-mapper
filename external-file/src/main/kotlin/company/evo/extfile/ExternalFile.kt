package company.evo.extfile

import java.io.IOException
import java.net.SocketTimeoutException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.attribute.FileTime
import java.nio.file.NoSuchFileException
import java.util.*
import java.util.stream.LongStream

import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import company.evo.extfile.trove.create as troveCreate


const val MAP_LOAD_FACTOR: Float = 0.75F

internal data class FileKey(
        val indexName: String,
        val fieldName: String
)

enum class ValuesStoreType {
    RAM,
    FILE,
}

class ScheduleIntervals(
        initialInterval: Long, interval: Long, scatter: Long
) {
    private val intervals: Iterator<Long>

    init {
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
        val backend: FileValues.Backend,
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

    fun isStoreBackendChanged(other: FileSettings): Boolean {
        return other.scalingFactor != scalingFactor ||
                other.backend != backend
    }
}

class ExternalFile(
        private val dir: Path,
        private val name: String,
        private val indexName: String,
        val settings: FileSettings
) {
    private val logger = LoggerFactory.getLogger(ExternalFile::class.java)

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
                return getValuesProvider(
                        fileLastModified
                )
            }
        } catch (e: NoSuchFileException) {
        } catch (e: IOException) {
            logger.warn("Cannot read file [$extFilePath]: $e")
        }
        return null
    }

//    private fun parseHeader(path: Path) {}

    private fun parse(path: Path, valuesProvider: FileValues.Provider) {
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
            maxRows ?: Int.MAX_VALUE
        }

        var processedRows = 0
        Files.newBufferedReader(path).use {
            it.lines()
                    .map { it.trim() }
                    .filter { !it.isEmpty() }
                    .filter { !it.startsWith("#") }
                    .iterator().withIndex().forEach {
                val line = it.value
                if (processedRows >= maxRows) {
                    return@forEach
                }
                val delimiterIx = line.indexOf('=')
                val key = line.substring(0, delimiterIx).trim().toLong()
                val valueStr = line.substring(delimiterIx + 1).trim()
                if (valueStr.isEmpty()) {
                    valuesProvider.remove(key)
                } else {
                    valuesProvider.put(key, valueStr.toDouble())
                }
                processedRows++
            }
        }
        val duration = (System.nanoTime() - startAt) / 1000_000
        logger.info("Parsed ${processedRows} values " +
                "for [$name] field of [${indexName}] index " +
                "from file [$path] for ${duration}ms")
    }

    private fun getValuesProvider(
            lastModified: FileTime): FileValues.Provider
    {
        val config = FileValues.Config()
        val valuesProvider = troveCreate(config, lastModified)
        parse(getExternalFilePath(), valuesProvider)
        logger.debug("Values capacity is ${valuesProvider.sizeBytes} bytes")
        return valuesProvider
    }

//    private fun getMappedFileValuesProvider(lastModified: FileTime): FileValues.Provider {
//        val indexFilePath = getBinaryFilePath()
//        val indexLastModified = try {
//            Files.getLastModifiedTime(indexFilePath)
//        } catch (e: NoSuchFileException) {
//            FileTime.fromMillis(0)
//        }
//        if (indexLastModified < lastModified) {
//            val parsedValues = parse(getExternalFilePath())
//            val tmpPath = Files.createTempFile(dir, name, null)
//            try {
//                ChronicleMap
//                        .of(java.lang.Long::class.java, java.lang.Double::class.java)
//                        .entries(parsedValues.keys.capacity * 2L)
//                        .createPersistedTo(tmpPath.toFile())
//                        .use { map ->
//                    parsedValues.keys.asSequence()
//                            .zip(parsedValues.values.asSequence())
//                            .forEach { (k, v) ->
//                                map.put(java.lang.Long(k), java.lang.Double(v))
//                            }
//                }
//                Files.move(tmpPath, indexFilePath, StandardCopyOption.ATOMIC_MOVE)
//                logger.debug("Dumped ${parsedValues.capacity} values " +
//                        "(${indexFilePath.toFile().length()} bytes) into file [${indexFilePath}]")
//            } finally {
//                Files.deleteIfExists(tmpPath)
//            }
//        }
//        val map = ChronicleMap
//                .of(java.lang.Long::class.java, java.lang.Double::class.java)
//                .recoverPersistedTo(indexFilePath.toFile(), false)
//        logger.debug("Loaded values from file [$indexFilePath]")
//        return LongDoubleFileValues.Provider(map, map.offHeapMemoryUsed(), lastModified)
//    }

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
        return dir.resolve("$name.dat")
    }
}
