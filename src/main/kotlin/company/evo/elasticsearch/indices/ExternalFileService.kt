/*
* Copyright 2017 Alexander Koval
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package company.evo.elasticsearch.indices

import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.attribute.FileTime
import java.util.concurrent.ConcurrentHashMap

import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients

import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.component.AbstractLifecycleComponent
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.env.Environment
import org.elasticsearch.env.NodeEnvironment
import org.elasticsearch.index.Index
import org.elasticsearch.threadpool.ThreadPool


class ExternalFileService : AbstractLifecycleComponent {

    private val nodeDir: Path
    private val threadPool: ThreadPool
    private val values: MutableMap<FileKey, FileValue> = ConcurrentHashMap()
    private val tasks: MutableMap<FileKey, Task> = HashMap()

    companion object {
        lateinit var instance: ExternalFileService
        var started: Boolean = false
    }

    private data class FieldSettings(
            val updateInterval: Long,
            val url: String?
    )

    private data class FileKey(
            val indexName: String,
            val fieldName: String
    )

    private data class FileValue(
            val values: Map<String, Double>,
            val lastModified: FileTime
    )

    private data class Task(
            val future: ThreadPool.Cancellable,
            val updateInterval: Long
    )

    constructor(settings: Settings, nodeDir: Path, threadPool: ThreadPool) : super(settings) {
        this.nodeDir = nodeDir
        this.threadPool = threadPool
    }

    override public fun doStart() {
        if (started) {
            throw IllegalStateException("Already started")
        }
        instance = this
    }

    override public fun doStop() {
        started = false
    }

    override fun doClose() {}

    @Synchronized
    fun addField(index: Index, fieldName: String, updateInterval: Long, url: String?) {
        val key = FileKey(index.name, fieldName)
        val existingTask = this.tasks[key]
        if (existingTask == null) {
            tryLoad(index.name, fieldName)
        }
        if (existingTask != null && existingTask.updateInterval != updateInterval) {
            existingTask.future.cancel()
            this.tasks.remove(key)
        }
        val task = this.tasks.getOrPut(key) {
            val future = threadPool.scheduleWithFixedDelay(
                    {
                        if (url != null) {
                            download(index.name, fieldName, url)
                        }
                        tryLoad(index.name, fieldName)
                    },
                    TimeValue.timeValueSeconds(updateInterval),
                    ThreadPool.Names.SAME)
            Task(future, updateInterval)
        }
        this.tasks.put(key, task)
    }

    @Synchronized
    internal fun getUpdateInterval(index: Index, fieldName: String): Long? {
        val key = FileKey(index.name, fieldName)
        return this.tasks[key]?.updateInterval
    }

    fun getValues(index: Index, fieldName: String): Map<String, Double> {
        return getValues(index.getName(), fieldName)
    }

    fun getValues(indexName: String, fieldName: String): Map<String, Double> {
        val key = FileKey(indexName, fieldName)
        return this.values.get(key)?.values.orEmpty()
    }

    internal fun getExternalFileDir(indexName: String): Path {
        return nodeDir
                .resolve("external_files")
                .resolve(indexName)
    }

    internal fun getExternalFilePath(indexName: String, fieldName: String): Path {
        return getExternalFileDir(indexName)
                .resolve(fieldName + ".txt")
    }

    internal fun getVersionFilePath(indexName: String, fieldName: String): Path {
        return getExternalFileDir(indexName)
                .resolve(fieldName + ".ver")
    }

    fun tryLoad(indexName: String, fieldName: String) {
        val key = FileKey(indexName, fieldName)
        val extFilePath = getExternalFilePath(indexName, fieldName)
        try {
            val lastModified = Files.getLastModifiedTime(extFilePath)
            val fieldValues = this.values[key]
            if (lastModified > (fieldValues?.lastModified ?: FileTime.fromMillis(0))) {
                val fValues = parse(extFilePath)
                this.values.put(key, FileValue(fValues, lastModified))
                logger.info("Loaded ${fValues.size} values " +
                        "for [${key.fieldName}] field of [${key.indexName}] index " +
                        "from file [${extFilePath}]")
            }
        } catch (e: NoSuchFileException) {
        } catch (e: IOException) {
            logger.warn("Cannot read file [$extFilePath]: $e")
        }
    }

    internal fun download(indexName: String, fieldName: String, url: String): Boolean {
        val client = HttpClients.createDefault()
        val httpGet = HttpGet(url)
        val ver = getCurrentVersion(indexName, fieldName)
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
                        logger.warn("Missing content when downloading [$url]")
                        return false
                    }
                    Files.createDirectories(getExternalFileDir(indexName))
                    val tmpPath = Files.createTempFile(
                            getExternalFileDir(indexName), fieldName, null)
                    resp.entity?.content?.use { inStream ->
                        Files.copy(inStream, tmpPath, StandardCopyOption.REPLACE_EXISTING)
                        tmpPath.toFile().renameTo(
                                getExternalFilePath(indexName, fieldName).toFile())
                    }
                    updateVersion(indexName, fieldName, lastModified)
                    return true
                }
                else -> {
                    logger.warn("Failed to download [$url] with status: ${resp.statusLine}")
                    return false
                }
            }
        }
    }

    internal fun getCurrentVersion(indexName: String, fieldName: String): String? {
        val versionPath = getVersionFilePath(indexName, fieldName)
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

    internal fun updateVersion(indexName: String, fieldName: String, ver: String) {
        val versionPath = getVersionFilePath(indexName, fieldName)
        try {
            Files.newBufferedWriter(versionPath).use {
                it.write(ver)
            }
        } catch (e: IOException) {
            logger.warn("Cannot write file [$versionPath]: $e")
        }
    }

    private fun parse(path: Path): Map<String, Double> {
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
}
