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

import java.lang.Exception
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import java.util.concurrent.ConcurrentHashMap

import org.apache.logging.log4j.Logger

import org.apache.lucene.util.IOUtils

import org.elasticsearch.common.component.AbstractLifecycleComponent
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.logging.Loggers
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.util.concurrent.AbstractRunnable
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException
import org.elasticsearch.env.NodeEnvironment
import org.elasticsearch.index.Index
import org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason
import org.elasticsearch.threadpool.Scheduler
import org.elasticsearch.threadpool.ThreadPool


class ExternalFileService : AbstractLifecycleComponent {

    private val nodeDir: Path
    private val threadPool: ThreadPool
    private val files = HashMap<FileKey, ExternalFileField>()
    // TODO Eliminate nullability of the value type
    private val values = ConcurrentHashMap<FileKey, FileValues.Provider?>()

    companion object {
        val EXTERNAL_DIR_NAME = "external_files"
        val EMPTY_FILE_VALUES: FileValues = EmptyFileValues()
        lateinit var instance: ExternalFileService
    }

    private data class ExternalFileField(
            val file: ExternalFile,
            var task: Scheduler.Cancellable
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
        logger.info(UnsafeAccess.ARRAY_BYTE_BASE_OFFSET)
        logger.info(org.agrona.UnsafeAccess.ARRAY_BYTE_BASE_OFFSET)

        logger.debug("Adding external file field: [${index.name}] [$fieldName]")
        val extDir = getDirForIndex(index)
        Files.createDirectories(extDir)
        val extFile = ExternalFile(
                extDir, fieldName, index.name, fileSettings,
                Loggers.getLogger(ExternalFile::class.java)
        )
        val key = FileKey(index.name, fieldName)
        val existingFileField = this.files[key]
        if (existingFileField != null) {
            val currentFileSettings = existingFileField.file.settings
            if (currentFileSettings.isUpdateChanged(fileSettings)) {
                logger.debug("Cancelling update task: [${index.name}] [$fieldName]")
                existingFileField.task.cancel()
                this.files.remove(key)
            }
            if (currentFileSettings.isStoreChanged(fileSettings)) {
                this.values.compute(key) { _, _ ->
                    extFile.loadValues(null)
                }
                this.files.computeIfPresent(key) { _, oldFileField ->
                    ExternalFileField(extFile, oldFileField.task)
                }
            }
        } else {
            this.values.computeIfAbsent(key) {
                extFile.loadValues(null)
            }
        }
        this.files.computeIfAbsent(key) {
            logger.debug("Scheduling update task every " +
                    "${fileSettings.updateInterval} ± ${fileSettings.updateScatter ?: 0 / 2} seconds: " +
                    "[${index.name}] [$fieldName]")
            val future = ScatteredReschedulingRunnable(
                    ScheduleIntervals(
                            0,
                            fileSettings.updateInterval,
                            fileSettings.updateScatter ?: 0),
                    ThreadPool.Names.SAME,
                    threadPool,
                    Loggers.getLogger(ScatteredReschedulingRunnable::class.java)
            ) {
                logger.debug("Started updating: [${index.name}] [$fieldName]")
                if (fileSettings.url != null) {
                    extFile.download()
                }
                this.values.compute(key) { _, oldValues ->
                    extFile.loadValues(oldValues?.lastModified) ?: oldValues
                }
                logger.debug("Finished updating: [${index.name}] [$fieldName]")
            }
            ExternalFileField(extFile, future)
        }
    }

    @Synchronized
    fun removeIndex(index: Index, reason: IndexRemovalReason) {
        val files = this.files.iterator()
        for ((key, fileField) in files) {
            if (key.indexName != index.name) {
                continue
            }
            fileField.task.cancel()
            files.remove()
            this.values.remove(key)
        }
        // FIXME Possibly we also should clean up external files on NO_LONGER_ASSIGNED
        // In other case external files are not deleted if closed index was deleted
        if (reason == IndexRemovalReason.DELETED) {
            IOUtils.rm(getDirForIndex(index))
        }
    }

    @Synchronized
    internal fun getFileSettings(index: Index, fieldName: String): FileSettings? {
        val key = FileKey(index.name, fieldName)
        return this.files[key]?.file?.settings
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

class ScatteredReschedulingRunnable: AbstractRunnable, Scheduler.Cancellable {
    private val runnable: () -> Unit
    private val executor: String
    private val threadPool: ThreadPool
    private val logger: Logger
    private val intervals: ScheduleIntervals

    @Volatile
    private var run = true

    constructor(
            intervals: ScheduleIntervals,
            executor: String,
            threadPool: ThreadPool,
            logger: Logger,
            runnable: () -> Unit) {
        this.intervals = intervals
        this.runnable = runnable
        this.executor = executor
        this.threadPool = threadPool
        this.logger = logger

        threadPool.schedule(
                TimeValue.timeValueSeconds(intervals.next()),
                executor,
                this)
    }

    override fun cancel() {
        run = false
    }

    override fun isCancelled(): Boolean = !run

    override fun doRun() {
        if (run) {
            runnable()
        }
    }

    override fun onFailure(e: Exception?) {
        logger.warn("failed to run scheduled task [$runnable] on thread pool [$executor]", e)
    }

    override fun onRejection(e: Exception?) {
        run = false
        if (logger.isDebugEnabled) {
            logger.debug("scheduled task [$runnable] was rejected on thread pool [$executor]", e)
        }
        super.onRejection(e)
    }

    override fun onAfter() {
        if (run) {
            try {
                threadPool.schedule(
                        TimeValue.timeValueSeconds(intervals.next()),
                        executor,
                        this)
            } catch (e: EsRejectedExecutionException) {
                onRejection(e)
            }
        }
    }
}