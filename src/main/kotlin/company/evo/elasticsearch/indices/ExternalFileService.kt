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

import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap

import org.apache.lucene.util.IOUtils

import org.elasticsearch.common.component.AbstractLifecycleComponent
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.logging.Loggers
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.env.NodeEnvironment
import org.elasticsearch.index.Index
import org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason
import org.elasticsearch.threadpool.ThreadPool


class ExternalFileService : AbstractLifecycleComponent {

    private val nodeDir: Path
    private val threadPool: ThreadPool
    private val files = HashMap<FileKey, ExternalFileField>()
    private val values: MutableMap<FileKey, FileValues.Provider?> = ConcurrentHashMap()

    companion object {
        val EXTERNAL_DIR_NAME = "external_files"
        val EMPTY_FILE_VALUES: FileValues = EmptyFileValues()
        lateinit var instance: ExternalFileService
    }

    private data class ExternalFileField(
            val file: ExternalFile,
            var task: ThreadPool.Cancellable?
    )

    @Inject
    internal constructor(
            settings: Settings,
            threadPool: ThreadPool,
            nodeEnv: NodeEnvironment) : super(settings) {
        this.nodeDir = nodeEnv.nodeDataPaths()[0]
        this.threadPool = threadPool
        instance = this
    }

    override public fun doStart() {}

    override public fun doStop() {}

    override fun doClose() {}

    @Synchronized
    fun addField(index: Index, fieldName: String, fileSettings: FileSettings) {
        logger.debug("Adding external file field: [${index.name}] [$fieldName]")
        val key = FileKey(index.name, fieldName)
        val existingFileField = this.files[key]
        if (existingFileField != null) {
            if (existingFileField.file.settings != fileSettings) {
                logger.debug("Cancelling update task: [${index.name}] [$fieldName]")
                existingFileField.task?.cancel()
                existingFileField.task = null
            }
            this.values.computeIfAbsent(key) {
                existingFileField.file.loadValues(null)
            }
        }
        this.files.getOrPut(key) {
            val extDir = getDirForIndex(index)
            Files.createDirectories(extDir)
            val extFile = ExternalFile(
                    extDir, fieldName, index.name, fileSettings,
                    Loggers.getLogger(ExternalFile::class.java)
            )
            logger.debug("Scheduling update task every " +
                    "${fileSettings.updateInterval} seconds: [${index.name}] [$fieldName]")
            val future = threadPool.scheduleWithFixedDelay(
                    {
                        logger.debug("Started updating: [${index.name}] [$fieldName]")
                        if (fileSettings.url != null) {
                            extFile.download()
                        }
                        this.values.compute(key) { _, oldValues ->
                            extFile.loadValues(oldValues?.lastModified()) ?: oldValues
                        }
                        logger.debug("Finished updating: [${index.name}] [$fieldName]")
                    },
                    TimeValue.timeValueSeconds(fileSettings.updateInterval),
                    ThreadPool.Names.SAME)
            ExternalFileField(extFile, future)
        }
    }

    @Synchronized
    fun removeIndex(index: Index, reason: IndexRemovalReason) {
        for ((key, fileField) in this.files) {
            if (key.indexName != index.name) {
                continue
            }
            fileField.task?.cancel()
            this.files.remove(key)
            this.values.remove(key)
        }
        // FIXME Possibly we also should clean up external files on NO_LONGER_ASSIGNED
        // In other case external files are not deleted if closed index was deleted
        if (reason == IndexRemovalReason.DELETED) {
            IOUtils.rm(getDirForIndex(index))
        }
    }

    @Synchronized
    internal fun getUpdateInterval(index: Index, fieldName: String): Long? {
        val key = FileKey(index.name, fieldName)
        return this.files[key]?.file?.settings?.updateInterval
    }

    fun getValues(index: Index, fieldName: String): FileValues {
        return getValues(index.name, fieldName)
    }

    fun getValues(indexName: String, fieldName: String): FileValues {
        val key = FileKey(indexName, fieldName)
        return this.values[key]?.get() ?: EMPTY_FILE_VALUES
    }

    private fun getDirForIndex(index: Index): Path {
        return this.nodeDir
                .resolve(EXTERNAL_DIR_NAME)
                .resolve(index.name)
    }
}
