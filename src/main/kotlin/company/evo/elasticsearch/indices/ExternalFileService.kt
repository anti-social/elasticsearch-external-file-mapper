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
import java.util.HashMap

import org.apache.logging.log4j.Logger

import org.elasticsearch.common.logging.Loggers
import org.elasticsearch.env.Environment
import org.elasticsearch.index.Index


class ExternalFileService {
    private val logger: Logger
    private val env: Environment

    constructor(env: Environment) {
        this.env = env
        this.logger = Loggers.getLogger(ExternalFileService::class.java)
    }

    fun getValues(index: Index, fieldName: String): Map<String, Double> {
        return getValues(index.getName(), fieldName)
    }

    fun getValues(indexName: String, fieldName: String): Map<String, Double> {
        val extFilePath = getIndexDir(indexName).resolve(fieldName + ".txt")
        val values = parse(extFilePath)
        logger.info("Loaded ${values.size} values for [${fieldName}] field of [${indexName}] index from file [${extFilePath}]")
        return values
    }

    fun getIndexDir(indexName: String): Path {
        // TODO check and make it right
        return env.dataFiles()[0].resolve(indexName)
    }

    private fun parse(path: Path): Map<String, Double> {
        val values = HashMap<String, Double>()
        try {
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
        } catch (e: IOException) {
            logger.warn("Cannot read file: " + e.message)
        }
        return values
    }
}
