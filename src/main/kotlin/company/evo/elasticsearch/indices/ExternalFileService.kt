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
import java.nio.file.attribute.FileTime
import java.util.concurrent.Executors
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

import org.apache.logging.log4j.Logger

import org.elasticsearch.common.logging.Loggers
import org.elasticsearch.env.Environment
import org.elasticsearch.index.Index
import org.joda.time.base.AbstractInterval


class ExternalFileService {
    private val env: Environment
    private val values: MutableMap<FileKey, FileValue> = ConcurrentHashMap()
    private val tasks: MutableMap<FileKey, Task> = HashMap()
    private val logger: Logger = Loggers.getLogger(ExternalFileService::class.java)
    private val scheduler: ScheduledExecutorService

    constructor(env: Environment) : this(env, 1)

    constructor(env: Environment, schedulerPoolSize: Int) {
        this.env = env
        this.scheduler = Executors.newScheduledThreadPool(schedulerPoolSize)
    }

    private data class FileKey(
            val indexName: String,
            val fieldName: String
    )

    private data class FileValue(
            val values: Map<String, Double>,
            val lastModified: FileTime
    )

    private data class Task(
            val future: ScheduledFuture<*>,
            val updateInterval: Long
    )

    @Synchronized
    fun addField(index: Index, fieldName: String, updateInterval: Long) {
        val key = FileKey(index.name, fieldName)
        val existingTask = this.tasks[key]
        if (existingTask == null) {
            tryLoad(index.name, fieldName)
        }
        if (existingTask != null && existingTask.updateInterval != updateInterval) {
            existingTask.future.cancel(false)
            this.tasks.remove(key)
        }
        val task = this.tasks.getOrPut(key) {
            val future = scheduler.scheduleAtFixedRate(
                    { tryLoad(index.name, fieldName) },
                    updateInterval, updateInterval, TimeUnit.SECONDS)
            Task(future, updateInterval)
        }
        this.tasks.put(key, task)
    }

    @Synchronized
    fun getUpdateInterval(index: Index, fieldName: String): Long? {
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

    fun getExternalFilePath(indexName: String, fieldName: String): Path {
        // TODO check and make it right
        return env.dataFiles()[0]
                .resolve("external_files")
                .resolve(indexName)
                .resolve(fieldName + ".txt")
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
        } catch (e: IOException) {
            logger.warn("Cannot read file: " + e.message)
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
