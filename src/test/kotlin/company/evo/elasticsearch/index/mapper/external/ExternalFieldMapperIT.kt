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

// import org.apache.lucene.util.LuceneTestCase

import org.elasticsearch.client.Requests.searchRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder
import org.elasticsearch.env.Environment
import org.elasticsearch.env.NodeEnvironment
import org.elasticsearch.plugins.Plugin
import org.elasticsearch.index.Index
import org.elasticsearch.index.query.QueryBuilders.functionScoreQuery
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.fieldValueFactorFunction
import org.elasticsearch.search.builder.SearchSourceBuilder.searchSource
import org.elasticsearch.test.ESIntegTestCase
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures

import org.hamcrest.Matchers.equalTo

import company.evo.elasticsearch.indices.ExternalFileService
import company.evo.elasticsearch.plugin.mapper.ExternalMapperPlugin


@ESIntegTestCase.ClusterScope(scope=ESIntegTestCase.Scope.TEST, numDataNodes=0)
@ESIntegTestCase.SuppressLocalMode
public class ExternalFieldMapperIT : ESIntegTestCase() {

    override fun nodePlugins(): Collection<Class<out Plugin>> {
        return Collections.singleton(ExternalMapperPlugin::class.java)
    }

    override fun ignoreExternalCluster(): Boolean { return true }

    public fun test() {
        val dataPath = createTempDir()
        val homePath = createTempDir()
        val settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), homePath)
                .put(Environment.PATH_DATA_SETTING.getKey(), dataPath)
                .build()
        val node = internalCluster().startNode(settings)
        val nodePaths = internalCluster()
                .getInstance(NodeEnvironment::class.java, node)
                .nodeDataPaths()
        println("> nodePath: ${nodePaths[0]}")
        assertEquals(1, nodePaths.size)

        val nodesResponse = client().admin().cluster().prepareNodesInfo().execute().actionGet()
        assertEquals(1, nodesResponse.getNodes().size);

        val extFileService = ExternalFileService(Environment(settings))

        val indexName = "test"
        client().admin()
                .indices()
                .prepareCreate(indexName)
                .addMapping(
                        "product",
                        jsonBuilder()
                        .startObject().startObject("product").startObject("properties")
                            .startObject("name")
                                .field("type", "text")
                            .endObject()
                            .startObject("ext_price")
                                .field("type", "external_file")
                            .endObject()
                        .endObject().endObject().endObject())
                .get()

        val indexResp = client().admin()
                .indices()
                .prepareGetIndex()
                .setIndices(indexName)
                .get()
        val indexUUID = indexResp.settings()
                .get(indexName)
                .getAsSettings("index")
                .get("uuid")
        val index = Index(indexName, indexUUID)
        copyTestResources(extFileService, index)

        Files.walk(dataPath).forEach { println("> $it") }

        client().prepareIndex("test", "product", "1")
                .setSource(
                        jsonBuilder()
                        .startObject()
                            .field("name", "Bergamont")
                        .endObject())
                .get()
        client().prepareIndex("test", "product", "2")
                .setSource(
                        jsonBuilder()
                        .startObject()
                            .field("name", "Specialized")
                        .endObject())
                .get()
        client().prepareIndex("test", "product", "3")
                .setSource(
                        jsonBuilder()
                        .startObject()
                            .field("name", "Cannondale")
                        .endObject())
                        .get()
        client().prepareIndex("test", "product", "4")
                .setSource(
                        jsonBuilder()
                        .startObject()
                            .field("name", "Honda")
                        .endObject())
                .get()

        client().admin().indices().prepareRefresh().execute().actionGet()

        val response = client().search(
                searchRequest()
                // .searchType(SearchType.QUERY_THEN_FETCH)
                .source(
                        searchSource()
                        .query(functionScoreQuery(
                                fieldValueFactorFunction("ext_price")
                                .missing(0.0)))
                        .explain(false)))
                .actionGet()
        assertNoFailures(response)
        val hits = response.getHits()
        assertThat(hits.getHits().size, equalTo(4))
        assertThat(hits.getAt(0).getId(), equalTo("3"))
        assertThat(hits.getAt(0).getScore(), equalTo(1.3f))
        assertThat(hits.getAt(1).getId(), equalTo("2"))
        assertThat(hits.getAt(1).getScore(), equalTo(1.2f))
        assertThat(hits.getAt(2).getId(), equalTo("1"))
        assertThat(hits.getAt(2).getScore(), equalTo(1.1f))
        assertThat(hits.getAt(3).getId(), equalTo("4"))
        assertThat(hits.getAt(3).getScore(), equalTo(0.0f))
    }

    private fun copyTestResources(extFileService: ExternalFileService, index: Index) {
        val indexPath = extFileService.getIndexDir(index)
        Files.createDirectories(indexPath)
        val resourcePath = getDataPath("/indices")
        Files.newInputStream(resourcePath.resolve("ext_price.txt")).use {
            val extFilePath = indexPath.resolve("ext_price.txt")
            logger.warn(">>> Copied external file to: $extFilePath")
            Files.copy(it, extFilePath)
        }
    }
}
