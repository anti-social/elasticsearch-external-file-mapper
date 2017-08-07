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

package company.evo.elasticsearch.plugin.mapper

import java.nio.file.Path
import java.util.Collections

import org.apache.logging.log4j.Logger

import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.logging.Loggers
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.env.Environment
import org.elasticsearch.index.mapper.Mapper
import org.elasticsearch.plugins.MapperPlugin
import org.elasticsearch.plugins.Plugin
import org.elasticsearch.script.ScriptService
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.watcher.ResourceWatcherService

import company.evo.elasticsearch.index.mapper.external.ExternalFileFieldMapper
import company.evo.elasticsearch.indices.ExternalFileService


class ExternalMapperPlugin : Plugin, MapperPlugin {

    companion object {
        val logger: Logger = Loggers.getLogger(ExternalMapperPlugin::class.java)
    }
    private val extFileService: ExternalFileService

    constructor(settings: Settings) {
        val env = Environment(settings)
        this.extFileService = ExternalFileService(env)
    }

    override fun createComponents(
            client: Client,
            clusterService: ClusterService,
            threadPool: ThreadPool,
            resourceWatcherService: ResourceWatcherService,
            scriptService: ScriptService,
            xContentRegistry: NamedXContentRegistry
    ): Collection<Any> {
        logger.warn(">>> ExternalMapperPlugin.createComponents <<<")
        val node = clusterService.localNode()
        logger.warn(">>> ${node.getId()}")
        return Collections.emptyList()
    }

    override fun getMappers(): Map<String, Mapper.TypeParser> {
        logger.warn(">>> ExternalMapperPlugin.getMappers <<<")
        return Collections.singletonMap(
                ExternalFileFieldMapper.CONTENT_TYPE,
                ExternalFileFieldMapper.TypeParser(extFileService))
    }
}
