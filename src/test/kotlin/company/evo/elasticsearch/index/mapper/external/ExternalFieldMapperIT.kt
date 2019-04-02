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

import java.nio.file.Files
import java.nio.file.Path
import java.util.Collections

import org.elasticsearch.client.Requests.searchRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder
import org.elasticsearch.env.Environment
import org.elasticsearch.env.NodeEnvironment
import org.elasticsearch.index.query.QueryBuilders.functionScoreQuery
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.fieldValueFactorFunction
import org.elasticsearch.plugins.Plugin
import org.elasticsearch.search.builder.SearchSourceBuilder.searchSource
import org.elasticsearch.search.sort.SortOrder
import org.elasticsearch.test.ESIntegTestCase
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*

import org.hamcrest.Matchers.*

import org.junit.Before

import company.evo.elasticsearch.indices.ExternalFileService
// import company.evo.elasticsearch.indices.MemoryIntShortFileValues
import company.evo.elasticsearch.plugin.mapper.ExternalFileMapperPlugin
import company.evo.persistent.hashmap.simple.SimpleHashMapEnv_Int_Float


@ESIntegTestCase.ClusterScope(scope=ESIntegTestCase.Scope.TEST, numDataNodes=0)
class ExternalFieldMapperIT : ESIntegTestCase() {

    lateinit var extFileService: ExternalFileService

    override fun nodePlugins(): Collection<Class<out Plugin>> {
        return Collections.singleton(ExternalFileMapperPlugin::class.java)
    }

    override fun ignoreExternalCluster(): Boolean { return true }

    @Before
    fun setDirectories() {
        val dataPath = createTempDir()
        val homePath = createTempDir()
        val settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.key, homePath)
                .put(Environment.PATH_DATA_SETTING.key, dataPath)
                .build()
        val node = internalCluster().startNode(settings)
        extFileService = internalCluster()
                .getInstance(ExternalFileService::class.java, node)
        val nodePaths = internalCluster()
                .getInstance(NodeEnvironment::class.java, node)
                .nodeDataPaths()
        assertEquals(1, nodePaths.size)
    }

    private fun initMap(name: String) {
        SimpleHashMapEnv_Int_Float.Builder()
                .useUnmapHack(true)
                .open(
                        extFileService.getExternalFileDir(name)
                                .also { Files.createDirectories(it) }
                )
                .use { mapEnv ->
                    mapEnv.openMap().use { map ->
                        map.put(1, 1.1F)
                        map.put(2, 1.2F)
                        map.put(3, 1.3F)
                    }
                }
    }

    fun testDefaults() {
        val indexName = "test"
        initMap("ext_price")

        val mapping = jsonBuilder().obj {
            obj("product") {
                obj("properties") {
                    obj("id") {
                        field("type", "integer")
                    }
                    obj("name") {
                        field("type", "text")
                    }
                    obj("ext_price") {
                        field("type", "external_file")
                        field("key_field", "id")
                        field("map_name", "ext_price")
                    }
                }
            }
        }
        client().admin()
                .indices()
                .prepareCreate(indexName)
                .addMapping("product", mapping)
                .get()

        indexTestDocuments(indexName)

        checkHits()
    }

    // fun testScalingFactor() {
    //     val indexName = "test"
    //     copyTestResources(indexName)
    //
    //     val mapping = jsonBuilder().obj {
    //         obj("product") {
    //             obj("properties") {
    //                 obj("name") {
    //                     field("type", "text")
    //                 }
    //                 obj("ext_price") {
    //                     field("type", "external_file")
    //                     field("update_interval", 600)
    //                     field("scaling_factor", 100)
    //                 }
    //             }
    //         }
    //     }
    //     client().admin()
    //             .indices()
    //             .prepareCreate(indexName)
    //             .addMapping("product", mapping)
    //             .get()
    //
    //     val index = resolveIndex(indexName)
    //     // val fileSettings = extFileService.getFileSettings(index, "ext_price")
    //     // assertThat(fileSettings?.scalingFactor, equalTo(100L))
    //     // assertThat(extFileService.getValues(index, "ext_price"),
    //     //         `is`(instanceOf(MemoryIntShortFileValues::class.java)))
    //     //
    //     // indexTestDocuments(indexName)
    //     //
    //     // checkHits()
    // }
    //
    // fun testSorting() {
    //     val indexName = "test"
    //     copyTestResources(indexName)
    //
    //     val mapping = jsonBuilder().obj {
    //         obj("product") {
    //             obj("name") {
    //                 field("type", "text")
    //             }
    //             obj("ext_price") {
    //                 field("type", "external_file")
    //                 field("update_interval", 600)
    //             }
    //         }
    //     }
    //     client().admin()
    //             .indices()
    //             .prepareCreate(indexName)
    //             .addMapping("product", mapping)
    //             .get()
    //
    //     indexTestDocuments(indexName)
    //
    //     val response = client().search(
    //             searchRequest()
    //                     .source(
    //                             searchSource()
    //                                     .sort("ext_price", SortOrder.DESC)
    //                                     .explain(false)))
    //             .actionGet()
    //     assertNoFailures(response)
    //     val hits = response.hits
    //     assertThat(hits.hits.size, equalTo(4))
    //     assertSearchHits(response, "3", "2", "1", "4")
    //     assertSortValues(response, arrayOf(1.3), arrayOf(1.2), arrayOf(1.1), arrayOf(Double.NEGATIVE_INFINITY))
    // }
    //
    // fun testCustomKeyField() {
    //     val indexName = "test"
    //     copyTestResources(indexName)
    //
    //     val mapping = jsonBuilder().obj {
    //         obj("product") {
    //             obj("properties") {
    //                 obj("id") {
    //                     field("type", "long")
    //                 }
    //                 obj("name") {
    //                     field("type", "text")
    //                 }
    //                 obj("ext_price") {
    //                     field("type", "external_file")
    //                     field("key_field", "id")
    //                     field("update_interval", 600)
    //                 }
    //             }
    //         }
    //     }
    //     client().admin()
    //             .indices()
    //             .prepareCreate(indexName)
    //             .addMapping("product", mapping)
    //             .get()
    //
    //     client().prepareIndex("test", "product", "p1")
    //             .setSource(
    //                     jsonBuilder().obj {
    //                         field("id", 1)
    //                         field("name", "Bergamont")
    //                     }
    //             )
    //             .get()
    //     client().prepareIndex("test", "product", "p2")
    //             .setSource(
    //                     jsonBuilder().obj {
    //                         field("id", 2)
    //                         field("name", "Specialized")
    //                     }
    //             )
    //             .get()
    //     client().prepareIndex("test", "product", "p3")
    //             .setSource(
    //                     jsonBuilder().obj {
    //                         field("id", 3)
    //                         field("name", "Cannondale")
    //                     }
    //             )
    //             .get()
    //     client().prepareIndex("test", "product", "p4")
    //             .setSource(
    //                     jsonBuilder().obj {
    //                         field("id", 4)
    //                         field("name", "Honda")
    //                     }
    //             )
    //             .get()
    //
    //     client().admin().indices().prepareRefresh().get()
    //
    //     val response = client().search(
    //             searchRequest().source(
    //                     searchSource().query(
    //                             functionScoreQuery(
    //                                     fieldValueFactorFunction("ext_price").missing(0.0)))
    //                             .explain(false)))
    //             .actionGet()
    //     assertNoFailures(response)
    //     val hits = response.hits
    //     assertThat(hits.hits.size, equalTo(4))
    //     assertThat(hits.getAt(0).id, equalTo("p3"))
    //     assertThat(hits.getAt(0).score, equalTo(1.3f))
    //     assertThat(hits.getAt(1).id, equalTo("p2"))
    //     assertThat(hits.getAt(1).score, equalTo(1.2f))
    //     assertThat(hits.getAt(2).id, equalTo("p1"))
    //     assertThat(hits.getAt(2).score, equalTo(1.1f))
    //     assertThat(hits.getAt(3).id, equalTo("p4"))
    //     assertThat(hits.getAt(3).score, equalTo(0.0f))
    // }
    //
    // fun testTemplate() {
    //     val indexName = "test_index"
    //     copyTestResources(indexName)
    //
    //     client().admin().indices().prepareDeleteTemplate("*").get()
    //
    //     val mapping = jsonBuilder().obj {
    //         obj("product") {
    //             obj("properties") {
    //                 obj("name") {
    //                     field("type", "text")
    //                 }
    //                 obj("ext_price") {
    //                     field("type", "external_file")
    //                     field("update_interval", 600)
    //                 }
    //             }
    //         }
    //     }
    //     client().admin().indices().preparePutTemplate("test_template")
    //             .setPatterns(listOf("test_*"))
    //             .setSettings(indexSettings())
    //             .setOrder(0)
    //             .addMapping("product", mapping)
    //             .get()
    //
    //     val tmplResp = client().admin().indices().prepareGetTemplates().get()
    //     assertThat(tmplResp.indexTemplates, hasSize(1))
    //
    //     indexTestDocuments(indexName)
    //
    //     checkHits()
    // }
    //
    // fun testDeleteIndex() {
    //     val indexName = "test"
    //     copyTestResources(indexName)
    //
    //     val mapping = jsonBuilder().obj {
    //         obj("product") {
    //             obj("properties") {
    //                 obj("name") {
    //                     field("type", "text")
    //                 }
    //                 obj("ext_price") {
    //                     field("type", "external_file")
    //                     field("update_interval", 600)
    //                 }
    //             }
    //         }
    //     }
    //     client().admin()
    //             .indices()
    //             .prepareCreate(indexName)
    //             .get()
    //     // External files should be cleaned up when deleting index
    //     client().admin()
    //             .indices()
    //             .prepareDelete(indexName)
    //             .get()
    //     client().admin()
    //             .indices()
    //             .prepareCreate(indexName)
    //             .addMapping("product", mapping)
    //             .get()
    //
    //     indexTestDocuments(indexName)
    //
    //     val response = client().search(
    //             searchRequest()
    //                     .source(
    //                             searchSource()
    //                                     .query(functionScoreQuery(
    //                                             fieldValueFactorFunction("ext_price")
    //                                                     .missing(0.0)))
    //                                     .explain(false)))
    //             .actionGet()
    //     assertNoFailures(response)
    //     val hits = response.hits
    //     assertThat(hits.hits.size, equalTo(4))
    //     assertThat(hits.getAt(0).score, equalTo(0.0f))
    //     assertThat(hits.getAt(1).score, equalTo(0.0f))
    //     assertThat(hits.getAt(2).score, equalTo(0.0f))
    //     assertThat(hits.getAt(3).score, equalTo(0.0f))
    // }
    //
    // fun testUpdateMapping() {
    //     val indexName = "test"
    //     copyTestResources(indexName)
    //
    //     val mapping = jsonBuilder().obj {
    //         obj("product") {
    //             obj("properties") {
    //                 obj("name") {
    //                     field("type", "text")
    //                 }
    //                 obj("ext_price") {
    //                     field("type", "external_file")
    //                     field("update_interval", 600)
    //                 }
    //             }
    //         }
    //     }
    //     client().admin()
    //             .indices()
    //             .prepareCreate(indexName)
    //             .addMapping("product", mapping)
    //             .get()
    //
    //     // assertThat(
    //     //         extFileService
    //     //                 .getFileSettings(resolveIndex(indexName), "ext_price")
    //     //                 ?.updateInterval,
    //     //         equalTo(600L))
    //
    //     val newMapping = jsonBuilder().obj {
    //         obj("product") {
    //             obj("properties") {
    //                 obj("name") {
    //                     field("type", "text")
    //                 }
    //                 obj("ext_price") {
    //                     field("type", "external_file")
    //                     field("update_interval", 60)
    //                 }
    //             }
    //         }
    //     }
    //     client().admin()
    //             .indices()
    //             .preparePutMapping(indexName)
    //             .setType("product")
    //             .setSource(newMapping)
    //             .get()
    //
    //     // assertThat(
    //     //         extFileService
    //     //                 .getFileSettings(resolveIndex(indexName), "ext_price")
    //     //                 ?.updateInterval,
    //     //         equalTo(60L))
    // }
    //
    // private fun copyTestResources(indexName: String) {
    //     val extFilePath = nodeDataDir
    //             .resolve("external_files")
    //             .resolve(indexName)
    //             .resolve("ext_price.txt")
    //     Files.createDirectories(extFilePath.parent)
    //     val resourcePath = getDataPath("/indices")
    //     Files.newInputStream(resourcePath.resolve("ext_price.txt")).use {
    //         logger.warn(">>> Copied external file to: $extFilePath")
    //         Files.copy(it, extFilePath)
    //     }
    // }

    private fun indexTestDocuments(indexName: String) {
        client().prepareIndex(indexName, "product", "1")
                .setSource(
                        jsonBuilder().obj {
                            field("id", 1)
                            field("name", "Bergamont")
                        }
                )
                .get()
        client().prepareIndex(indexName, "product", "2")
                .setSource(
                        jsonBuilder().obj {
                            field("id", 2)
                            field("name", "Specialized")
                        }
                )
                .get()
        client().prepareIndex(indexName, "product", "3")
                .setSource(
                        jsonBuilder().obj {
                            field("id", 3)
                            field("name", "Cannondale")
                        }
                )
                .get()
        client().prepareIndex(indexName, "product", "4")
                .setSource(
                        jsonBuilder().obj {
                            field("id", 4)
                            field("name", "Honda")
                        }
                )
                .get()

        client().admin().indices().prepareRefresh().get()

    }

    private fun checkHits() {
        val response = client().search(
                searchRequest()
                        .source(
                                searchSource()
                                        .query(functionScoreQuery(
                                                fieldValueFactorFunction("ext_price")
                                                        .missing(0.0)))
                                        .explain(false)))
                .actionGet()
        assertNoFailures(response)
        val hits = response.hits
        assertThat(hits.hits.size, equalTo(4))
        assertThat(hits.getAt(0).id, equalTo("3"))
        assertThat(hits.getAt(0).score, equalTo(1.3f))
        assertThat(hits.getAt(1).id, equalTo("2"))
        assertThat(hits.getAt(1).score, equalTo(1.2f))
        assertThat(hits.getAt(2).id, equalTo("1"))
        assertThat(hits.getAt(2).score, equalTo(1.1f))
        assertThat(hits.getAt(3).id, equalTo("4"))
        assertThat(hits.getAt(3).score, equalTo(0.0f))
    }
}
