package company.evo.elasticsearch.indices

import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.attribute.FileTime

import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients

import org.elasticsearch.common.logging.Loggers
import org.elasticsearch.index.Index


internal data class FileKey(
        val indexName: String,
        val fieldName: String
)

interface FileValues {
    fun lastModified(): FileTime?
    fun size(): Int
    fun get(key: String, defaultValue: Double): Double
    fun contains(key: String): Boolean
}

class EmptyFileValues : FileValues {
    override fun size(): Int {
        return 0
    }

    override fun lastModified(): FileTime? {
        return null
    }
    override fun get(key: String, defaultValue: Double): Double {
        return defaultValue
    }

    override fun contains(key: String): Boolean {
        return false
    }
}

class MapFileValues(
        private val values: Map<String, Double>,
        private val lastModified: FileTime
) : FileValues {
    override fun size(): Int {
        return values.size
    }

    override fun lastModified(): FileTime? {
        return lastModified
    }

    override fun get(key: String, defaultValue: Double): Double {
        return values.getOrDefault(key, defaultValue)
    }

    override fun contains(key: String): Boolean {
        return values.containsKey(key)
    }
}

data class FileSettings(
        val updateInterval: Long,
        val url: String?
)

class ExternalFileUpdater(
        private val dataDir: Path,
        private val index: Index,
        private val fieldName: String,
        private val settings: FileSettings)
{
    private val logger = Loggers.getLogger(ExternalFileService::class.java)

    internal fun download(): Boolean {
        val client = HttpClients.createDefault()
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
                        tmpPath.toFile().renameTo(
                                getExternalFilePath().toFile())
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

    internal fun loadValues(lastModified: FileTime?): FileValues? {
        val extFilePath = getExternalFilePath()
        try {
            val fileLastModified = Files.getLastModifiedTime(extFilePath)
            if (fileLastModified > (lastModified ?: FileTime.fromMillis(0))) {
                val values = parse(extFilePath)
                logger.info("Loaded ${values.size} values " +
                        "for [${fieldName}] field of [${index.name}] index " +
                        "from file [${extFilePath}]")
                return MapFileValues(values, fileLastModified)
            }
        } catch (e: NoSuchFileException) {
        } catch (e: IOException) {
            logger.warn("Cannot read file [$extFilePath]: $e")
        }
        return null
    }

    internal fun parse(path: Path): Map<String, Double> {
        val values = HashMap<String, Double>()
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
                values[key] = value.toDouble()
            }
        }
        return values
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
}
