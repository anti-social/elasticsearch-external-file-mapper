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

import org.elasticsearch.common.settings.Settings
import org.elasticsearch.env.Environment
import org.elasticsearch.index.mapper.Mapper
import org.elasticsearch.plugins.MapperPlugin
import org.elasticsearch.plugins.Plugin

import company.evo.elasticsearch.index.mapper.external.ExternalFileFieldMapper
import company.evo.elasticsearch.indices.ExternalFileService


class ExternalMapperPlugin : Plugin, MapperPlugin {

    private val extFileService: ExternalFileService

    constructor(settings: Settings) {
        val env = Environment(settings)
        this.extFileService = ExternalFileService(env.dataFiles()[0])
    }

    override fun getMappers(): Map<String, Mapper.TypeParser> {
        return Collections.singletonMap(
                ExternalFileFieldMapper.CONTENT_TYPE,
                ExternalFileFieldMapper.TypeParser(extFileService))
    }
}
