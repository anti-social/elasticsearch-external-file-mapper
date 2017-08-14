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
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

import org.apache.logging.log4j.Logger

import org.elasticsearch.common.logging.Loggers
import org.elasticsearch.env.Environment
import org.elasticsearch.index.Index
import java.nio.file.attribute.FileTime
import java.util.*


class ExternalFileService {
    private val logger: Logger
    private val env: Environment
    private val values: MutableMap<FileKey, FileValues>
    private val scheduler: ScheduledExecutorService

//    companion object {
//        val EMPTY_VALUES: Map<String, Double> = Collections.emptyMap()
//    }

    private data class FileKey(
            val indexName: String,
            val fileName: String)

    private data class FileValues(
            val values: Map<String, Double>,
            val lastModified: FileTime)

    constructor(env: Environment) {
        this.env = env
        this.values = HashMap()
        this.scheduler = Executors.newScheduledThreadPool(1)
        this.logger = Loggers.getLogger(ExternalFileService::class.java)
    }

    fun addField(index: Index, fieldName: String, period: Long) {
        tryLoad(index, fieldName)
        scheduler.scheduleAtFixedRate(
                { tryLoad(index, fieldName) },
                period, period, TimeUnit.SECONDS)
    }

    fun getValues(index: Index, fieldName: String): Map<String, Double> {
        return getValues(index.getName(), fieldName)
    }

    fun getValues(indexName: String, fieldName: String): Map<String, Double> {
        val key = FileKey(indexName, fieldName)
        return this.values.get(key)?.values.orEmpty()
    }

    fun getIndexDir(indexName: String): Path {
        // TODO check and make it right
        return env.dataFiles()[0]
                .resolve("external_files")
                .resolve(indexName)
    }

    @Synchronized
    private fun tryLoad(index: Index, fieldName: String) {
        logger.warn(">>> Trying to load values for [${index.name}] [$fieldName]")
        val key = FileKey(index.name, fieldName)
        val extFilePath = getIndexDir(index.name).resolve(fieldName + ".txt")
        val lastModified = Files.getLastModifiedTime(extFilePath)
        logger.warn("LastModified: $lastModified")
        val fieldValues = this.values.get(key)
        if (lastModified > (fieldValues?.lastModified ?: FileTime.fromMillis(0))) {
            val fValues = parse(extFilePath)
            if (fValues != null) {
                this.values.put(key, fValues)
                logger.info("Loaded ${fValues.values.size} values " +
                        "for [${fieldName}] field of [${index.name}] index from file [${extFilePath}]")
            }
        }
    }

    private fun parse(path: Path): FileValues? {
        try {
            val values = HashMap<String, Double>()
            val lastModified = Files.getLastModifiedTime(path)
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
                return FileValues(values, lastModified)
            }
        } catch (e: IOException) {
            logger.warn("Cannot read file: " + e.message)
        }
        return null
    }
}
