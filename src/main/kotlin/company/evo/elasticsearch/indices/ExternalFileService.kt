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

import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

import org.apache.logging.log4j.LogManager

import org.elasticsearch.common.component.AbstractLifecycleComponent
import org.elasticsearch.env.NodeEnvironment

import company.evo.rc.RefCounted
import company.evo.rc.AtomicRefCounted


class ExternalFileService internal constructor(
        nodeEnv: NodeEnvironment
) : AbstractLifecycleComponent() {

    private val logger = LogManager.getLogger(this::javaClass)
    private val externalDataDir = nodeEnv.sharedDataPath().resolve(EXTERNAL_DIR_NAME)
    private val mapFiles = ConcurrentHashMap<String, RefCounted<FileValues.Provider>>()

    companion object {
        const val EXTERNAL_DIR_NAME = "external_files"
        private var lateInstance = AtomicReference<ExternalFileService>()
        val instance: ExternalFileService
            get() {
                return lateInstance.get()
                    ?: throw IllegalStateException("ExternalFileService is not initialized")
            }
    }

    init {
        if (!lateInstance.compareAndSet(null, this)) {
            throw IllegalStateException("ExternalFileService must exist in a single copy")
        }
    }

    override fun doStart() {}

    override fun doStop() {}

    override fun doClose() {
        mapFiles.forEach { (_, valuesProvider) ->
            valuesProvider.release()
        }
        mapFiles.clear()
    }

    fun addFile(indexName: String, fieldName: String, mapName: String, sharding: Boolean, numShards: Int) {
        val extDir = getExternalFileDir(mapName)

        // We don't need synchronization as compute function invocation performs atomically
        mapFiles.compute(mapName) { _, v ->
            val curProvider = v?.get()
            if (curProvider == null ||
                    curProvider.dir != extDir || curProvider.sharding != sharding || curProvider.numShards != numShards) {
                logger.info("Adding external file field: {index=$indexName, field=$fieldName, path=$extDir, sharding=$sharding, numShards=$numShards}")
                v?.release()
                AtomicRefCounted(IntDoubleFileValues.Provider(extDir, sharding, numShards)) { it.close() }
            } else {
                v
            }
        }
    }

    fun getValues(mapName: String, shardId: Int?): FileValues {
        repeat(100) {
            val v = mapFiles[mapName] ?: return EmptyFileValues
            val valuesProvider = v.retain()
            if (valuesProvider != null) {
                try {
                    return valuesProvider.getValues(shardId)
                } finally {
                    v.release()
                }
            }
        }
        throw IllegalStateException("Cannot get values for map: ${mapName}")
    }

    fun getExternalFileDir(name: String): Path {
        return externalDataDir.resolve(name)
    }
}
