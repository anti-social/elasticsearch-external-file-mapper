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

package company.evo.elasticsearch.index.mapper.external

import org.elasticsearch.index.IndexService
import org.elasticsearch.index.mapper.Mapper.TypeParser
import org.elasticsearch.indices.mapper.MapperRegistry
import org.elasticsearch.test.ESSingleNodeTestCase
import org.junit.Before

import java.util.Collections


public class ExternalFieldMapperTests : ESSingleNodeTestCase() {

    lateinit var indexService: IndexService
    lateinit var mapperRegistry: MapperRegistry

    @Before fun setup() {
        indexService = this.createIndex("test")
        mapperRegistry = MapperRegistry(
            Collections.singletonMap(
                ExternalFileFieldMapper.CONTENT_TYPE,
                ExternalFileFieldMapper.TypeParser() as TypeParser),
            Collections.emptyMap())
    }

    public fun test() {
    }
}
