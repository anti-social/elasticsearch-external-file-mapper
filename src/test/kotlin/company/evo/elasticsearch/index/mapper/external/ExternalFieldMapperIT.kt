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
import java.util.Collections

import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.Requests.searchRequest
import org.elasticsearch.cluster.routing.Murmur3HashFunction
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

import org.junit.Assert
import org.junit.Before

import company.evo.elasticsearch.indices.ExternalFileService
import company.evo.elasticsearch.plugin.mapper.ExternalFileMapperPlugin
import company.evo.persistent.hashmap.straight.StraightHashMapEnv
import company.evo.persistent.hashmap.straight.StraightHashMapType_Int_Float

inline fun <T: AutoCloseable?, R> List<T>.use(block: (List<T>) -> R): R {
    var exception: Throwable? = null
    try {
        return block(this)
    } catch (e: Throwable) {
        exception = e
        throw e
    } finally {
        for (v in this) {
            when {
                v == null -> {}
                exception == null -> {
                    v.close()
                }
                else -> {
                    try {
                        v.close()
                    } catch (e: Throwable) {
                        exception.addSuppressed(e)
                    }
                }
            }
        }
    }
}

@ESIntegTestCase.ClusterScope(scope=ESIntegTestCase.Scope.TEST, numDataNodes=0)
class ExternalFieldMapperIT : ESIntegTestCase() {

    private lateinit var extFileService: ExternalFileService

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

    private fun initMap(name: String, numShards: Int? = null, entries: Map<Int, Float>? = null) {
        val baseExtFileDir = extFileService.getExternalFileDir(name)
        val envs = (0 until (numShards ?: 1)).map { shardId ->
            val extFileDir = if (numShards != null) {
                baseExtFileDir.resolve(shardId.toString())
            } else {
                baseExtFileDir
            }
            StraightHashMapEnv.Builder(StraightHashMapType_Int_Float)
                    .useUnmapHack(true)
                    .open(extFileDir.also { Files.createDirectories(it) })
        }
        if (entries != null) {
            envs.use { mapEnvs ->
                mapEnvs.map { it.openMap() }.use { maps ->
                    entries.forEach { (k , v) ->
                        val shardId = Math.floorMod(Murmur3HashFunction.hash(k.toString()), numShards ?: 1)
                        val map = maps[shardId]
                        map.put(k, v)
                    }
                }
            }
        }
    }

    fun testDefaults() {
        val indexName = "test"
        initMap("ext_price", null, mapOf(1 to 1.1F, 2 to 1.2F, 3 to 1.3F))

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

        assertHits(search(), listOf("3" to 1.3F, "2" to 1.2F, "1" to 1.1F, "4" to 0.0F))
    }

    fun testSharding() {
        val indexName = "test"
        val numShards = 4
        initMap("ext_price", numShards, mapOf(1 to 1.1F, 2 to 1.2F, 3 to 1.3F))

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
                        field("sharding", true)
                    }
                }
            }
        }
        client().admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(Settings.builder().put("index.number_of_shards", numShards))
                .addMapping("product", mapping)
                .get()

        indexTestDocuments(indexName)

        assertHits(search(), listOf("3" to 1.3F, "2" to 1.2F, "1" to 1.1F, "4" to 0.0F))
    }

    fun testSorting() {
        val indexName = "test"
        initMap("ext_price", null, mapOf(1 to 1.1F, 2 to 1.2F, 3 to 1.3F))

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

        assertHitsWithSort(
                searchWithSort(),
                listOf("3" to arrayOf(1.3), "2" to arrayOf(1.2), "1" to arrayOf(1.1), "4" to arrayOf(Double.NEGATIVE_INFINITY))
        )
    }

    fun testTemplate() {
        val indexName = "test_index"
        initMap("ext_price", null, mapOf(1 to 1.1F, 2 to 1.2F, 3 to 1.3F))

        client().admin().indices().prepareDeleteTemplate("*").get()

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
        client().admin().indices().preparePutTemplate("test_template")
                .setPatterns(listOf("test_*"))
                .setSettings(indexSettings())
                .setOrder(0)
                .addMapping("product", mapping)
                .get()

        val tmplResp = client().admin().indices().prepareGetTemplates().get()
        assertThat(tmplResp.indexTemplates, hasSize(1))

        indexTestDocuments(indexName)

        assertHits(search(), listOf("3" to 1.3F, "2" to 1.2F, "1" to 1.1F, "4" to 0.0F))
    }

    fun testUpdateMapping() {
        val indexName = "test"
        initMap("ext_price", null, mapOf(1 to 1.1F, 2 to 1.2F, 3 to 1.3F))
        initMap("new_ext_price", null, mapOf(1 to -1.1F, 2 to -1.2F, 3 to -1.3F))

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
        assertHits(search(), listOf("3" to 1.3F, "2" to 1.2F, "1" to 1.1F, "4" to 0.0F))

        val newMapping = jsonBuilder().obj {
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
                        field("map_name", "new_ext_price")
                    }
                }
            }
        }
        client().admin()
                .indices()
                .preparePutMapping(indexName)
                .setType("product")
                .setSource(newMapping)
                .get()

        assertHits(search(), listOf("4" to 0.0F, "1" to -1.1F, "2" to -1.2F, "3" to -1.3F))
    }

    fun testPickUpExternalFileOnTheFly() {
        val indexName = "test"
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

        assertHits(search(), listOf("2" to 0.0F, "4" to 0.0F, "1" to 0.0F, "3" to 0.0F))

        initMap("ext_price", null, mapOf(1 to 1.1F, 2 to 1.2F, 3 to 1.3F))

        assertHits(search(), listOf("3" to 1.3F, "2" to 1.2F, "1" to 1.1F, "4" to 0.0F))
    }

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

    private fun search(): SearchResponse {
        val query = functionScoreQuery(
                fieldValueFactorFunction("ext_price").missing(0.0)
        )
        return client().search(
                searchRequest().source(
                        searchSource()
                                .query(query)
                                .explain(false)
                )
        )
                .actionGet()
                .also(::assertNoFailures)
    }

    private fun assertHits(response: SearchResponse, scores: List<Pair<String, Float>>) {
        val hits = response.hits
        assertThat(hits.hits.size, equalTo(scores.size))
        scores.forEachIndexed { ix, (id, score) ->
            val hit = hits.getAt(ix)
            assertThat(hit.id, equalTo(id))
            assertThat(hit.score, equalTo(score))
        }
    }

    private fun searchWithSort(): SearchResponse {
        return client().search(
                searchRequest()
                        .source(
                                searchSource()
                                        .sort("ext_price", SortOrder.DESC)
                                        .explain(false)))
                .actionGet()
                .also(::assertNoFailures)
    }

    private fun assertHitsWithSort(response: SearchResponse, sorts: List<Pair<String, Array<Double>>>) {
        val hits = response.hits
        assertThat(hits.hits.size, equalTo(sorts.size))
        sorts.forEachIndexed { ix, (id, sortValues) ->
            val hit = hits.getAt(ix)
            assertThat(hit.id, equalTo(id))
            assertThat(hit.sortValues.size, equalTo(sortValues.size))
            sortValues.zip(hit.sortValues).forEach { (expected, value) ->
                if (value is Double) {
                    Assert.assertEquals(value, expected, 1e-6)
                } else {
                    throw AssertionError(
                            "Expected ${Double::class.java} but was ${value::class.java}: $value"
                    )
                }
            }
        }
    }
}
