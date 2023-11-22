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

import company.evo.elasticsearch.indices.ExternalFileService
import company.evo.elasticsearch.plugin.mapper.ExternalFileMapperPlugin
import company.evo.persistent.hashmap.straight.StraightHashMapEnv
import company.evo.persistent.hashmap.straight.StraightHashMapType_Int_Float
import company.evo.persistent.hashmap.straight.StraightHashMapType_Long_Float

import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.Requests.searchRequest
import org.elasticsearch.cluster.routing.Murmur3HashFunction
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder
import org.elasticsearch.core.internal.io.IOUtils
import org.elasticsearch.index.IndexService
import org.elasticsearch.index.query.QueryBuilders.functionScoreQuery
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.fieldValueFactorFunction
import org.elasticsearch.plugins.Plugin
import org.elasticsearch.search.builder.SearchSourceBuilder.searchSource
import org.elasticsearch.search.sort.SortOrder
import org.elasticsearch.test.ESSingleNodeTestCase
import org.elasticsearch.test.InternalSettingsPlugin
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*

import org.hamcrest.Matchers.*

import org.junit.After
import org.junit.Assert

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

class ExternalFieldMapperTests : ESSingleNodeTestCase() {
    override fun getPlugins(): Collection<Class<out Plugin>> {
        return pluginList(
            InternalSettingsPlugin::class.java,
            ExternalFileMapperPlugin::class.java
        )
    }

    @After
    fun cleanupSharedData() {
        ExternalFileService.instance.reset()
        IOUtils.rm(ExternalFileService.instance.externalDataDir)
    }

    private fun initMap(name: String, numShards: Int? = null, entries: Map<Int, Float>? = null) {
        val baseExtFileDir = ExternalFileService.instance.getExternalFileDir(name)
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

    private fun initMapLongFloat(name: String, numShards: Int? = null, entries: Map<Long, Float>? = null) {
        val baseExtFileDir = ExternalFileService.instance.getExternalFileDir(name)
        val envs = (0 until (numShards ?: 1)).map { shardId ->
            val extFileDir = if (numShards != null) {
                baseExtFileDir.resolve(shardId.toString())
            } else {
                baseExtFileDir
            }
            StraightHashMapEnv.Builder(StraightHashMapType_Long_Float)
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
        createIndex(indexName, 1, "product", mapping)

        val mappingsResponse = client().admin()
            .indices()
            .prepareGetMappings(indexName)
            .get()
        val productMapping = mappingsResponse.mappings()["test"]["product"]
        assertThat(productMapping.type(), equalTo("product"))
        val productFields = productMapping.sourceAsMap()["properties"] as Map<*, *>
        val extPriceField = productFields["ext_price"] as Map<*, *>
        assertThat(extPriceField["type"] as String, equalTo("external_file"))
        assertThat(extPriceField["key_field"] as String, equalTo("id"))
        assertThat(extPriceField["map_name"] as String, equalTo("ext_price"))
        assertThat(extPriceField.size, equalTo(3))

        indexTestDocuments(indexName)

        assertHits(search(), listOf("3" to 1.3F, "2" to 1.2F, "1" to 1.1F, "4" to 0.0F))
    }

    fun testLongKey() {
        val indexName = "test"
        initMapLongFloat("ext_price", null, mapOf(1L to 1.1F, 2L to 1.2F, Int.MAX_VALUE.toLong() + 1L to 1.3F))

        val mapping = jsonBuilder().obj {
            obj("product") {
                obj("properties") {
                    obj("id") {
                        field("type", "long")
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
        createIndex(indexName, 1, "product", mapping)

        val mappingsResponse = client().admin()
            .indices()
            .prepareGetMappings(indexName)
            .get()
        val productMapping = mappingsResponse.mappings()["test"]["product"]
        assertThat(productMapping.type(), equalTo("product"))
        val productFields = productMapping.sourceAsMap()["properties"] as Map<*, *>
        val extPriceField = productFields["ext_price"] as Map<*, *>
        assertThat(extPriceField["type"] as String, equalTo("external_file"))
        assertThat(extPriceField["key_field"] as String, equalTo("id"))
        assertThat(extPriceField["map_name"] as String, equalTo("ext_price"))
        assertThat(extPriceField.size, equalTo(3))

        indexTestDocumentsWithLongKeys(indexName)

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
        createIndex(indexName, numShards, "product", mapping)

        val mappingsResponse = client().admin()
            .indices()
            .prepareGetMappings(indexName)
            .get()
        val productMapping = mappingsResponse.mappings()["test"]["product"]
        assertThat(productMapping.type(), equalTo("product"))
        val productFields = productMapping.sourceAsMap()["properties"] as Map<*, *>
        val extPriceField = productFields["ext_price"] as Map<*, *>
        assertThat(extPriceField["type"] as String, equalTo("external_file"))
        assertThat(extPriceField["key_field"] as String, equalTo("id"))
        assertThat(extPriceField["map_name"] as String, equalTo("ext_price"))
        assertThat(extPriceField["sharding"] as Boolean, equalTo(true))
        assertThat(extPriceField.size, equalTo(4))

        indexTestDocuments(indexName)

        assertHits(search(), listOf("3" to 1.3F, "2" to 1.2F, "1" to 1.1F, "4" to 0.0F))
    }

    fun testUpdateSharding() {
        val indexName = "test"
        val numShards = 8
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
                    }
                }
            }
        }
        createIndex(indexName, numShards, "product", mapping)

        indexTestDocuments(indexName)

        assertHits(search(), listOf("1" to 0.0F, "2" to 0.0F, "3" to 0.0F, "4" to 0.0F))

        val mappingWithSharding = jsonBuilder().obj {
            obj("properties") {
                obj("ext_price") {
                    field("type", "external_file")
                    field("key_field", "id")
                    field("map_name", "ext_price")
                    field("sharding", true)
                }
            }
        }
        client().admin()
            .indices()
            .preparePutMapping(indexName)
            .setType("product")
            .setSource(mappingWithSharding)
            .get()

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
        createIndex(indexName, 1, "product", mapping)

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
            .setSettings(indexSettings(1))
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
        initMap("new_ext_price", null, mapOf(1 to 0.9F, 2 to 0.8F, 3 to 0.7F, 4 to 1.4F))

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
        createIndex(indexName, 1, "product", mapping)

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

        assertHits(search(), listOf("4" to 1.4F, "1" to 0.9F, "2" to 0.8F, "3" to 0.7F))
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
        createIndex(indexName, 1, "product", mapping)

        indexTestDocuments(indexName)

        assertHits(search(), listOf("1" to 0.0F, "2" to 0.0F, "3" to 0.0F, "4" to 0.0F))

        initMap("ext_price", null, mapOf(1 to 1.1F, 2 to 1.2F, 3 to 1.3F))

        assertHits(search(), listOf("3" to 1.3F, "2" to 1.2F, "1" to 1.1F, "4" to 0.0F))
    }

    private fun createIndex(
        indexName: String, numShards: Int, type: String, mapping: XContentBuilder
    ): IndexService {
        val createIndexRequest = client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(indexSettings(numShards))
            .addMapping(type, mapping)
        return createIndex(indexName, createIndexRequest)
    }

    private fun indexSettings(numShards: Int): Settings {
        return Settings.builder()
            .put("index.number_of_shards", numShards)
            .put("index.number_of_routing_shards", numShards)
            .build()
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

    private fun indexTestDocumentsWithLongKeys(indexName: String) {
        client().prepareIndex(indexName, "product", "1")
            .setSource(
                jsonBuilder().obj {
                    field("id", 1L)
                    field("name", "Bergamont")
                }
            )
            .get()
        client().prepareIndex(indexName, "product", "2")
            .setSource(
                jsonBuilder().obj {
                    field("id", 2L)
                    field("name", "Specialized")
                }
            )
            .get()
        client().prepareIndex(indexName, "product", "3")
            .setSource(
                jsonBuilder().obj {
                    field("id", Int.MAX_VALUE.toLong() + 1L)
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
                    .sort("_score")
                    .sort("id", SortOrder.ASC)
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
