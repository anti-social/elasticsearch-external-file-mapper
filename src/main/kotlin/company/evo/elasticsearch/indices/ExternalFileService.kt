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

import company.evo.persistent.hashmap.simple.SimpleHashMapEnv_Int_Float

import org.elasticsearch.common.component.AbstractLifecycleComponent
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.env.NodeEnvironment

class ExternalFileService @Inject internal constructor(
        settings: Settings,
        nodeEnv: NodeEnvironment
) : AbstractLifecycleComponent(settings) {

    private val nodeDir: Path = nodeEnv.nodeDataPaths()[0]
    private val mapFiles = ConcurrentHashMap<String, FileValues.Provider>()

    companion object {
        const val EXTERNAL_DIR_NAME = "external_files"
        val EMPTY_FILE_VALUES: FileValues = EmptyFileValues()
        lateinit var instance: ExternalFileService
    }

    init {
        instance = this
    }

    override fun doStart() {}

    override fun doStop() {}

    override fun doClose() {
        mapFiles.forEach { (_, valuesProvider) ->
            valuesProvider.close()
        }
        mapFiles.clear()
    }

    @Synchronized
    fun addFile(indexName: String, fieldName: String, mapName: String) {
        logger.debug("Adding external file field: [$indexName] [$fieldName]")
        val extDir = getExternalFileDir(mapName)

        val mapEnv = SimpleHashMapEnv_Int_Float.Builder()
                .useUnmapHack(true)
                .openReadOnly(extDir)
        mapFiles.putIfAbsent(mapName, IntDoubleFileValues.Provider(mapEnv))
    }

    fun getValues(mapName: String): FileValues {
        return mapFiles[mapName]?.get() ?: EMPTY_FILE_VALUES
    }

    fun getExternalFileDir(name: String): Path {
        return nodeDir
                .resolve(EXTERNAL_DIR_NAME)
                .resolve(name)
    }
}
