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

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope

// import org.apache.lucene.util.LuceneTestCase

import org.elasticsearch.client.Requests.searchRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder
import org.elasticsearch.env.Environment
import org.elasticsearch.env.NodeEnvironment
import org.elasticsearch.plugins.Plugin
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
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
class ExternalFieldMapperIT : ESIntegTestCase() {

    override fun nodePlugins(): Collection<Class<out Plugin>> {
        return Collections.singleton(ExternalMapperPlugin::class.java)
    }

    override fun ignoreExternalCluster(): Boolean { return true }

    fun test() {
        val dataPath = createTempDir()
        val homePath = createTempDir()
        val settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.key, homePath)
                .put(Environment.PATH_DATA_SETTING.key, dataPath)
                .build()
        val node = internalCluster().startNode(settings)
        val nodePaths = internalCluster()
                .getInstance(NodeEnvironment::class.java, node)
                .nodeDataPaths()
        assertEquals(1, nodePaths.size)

        val nodesResponse = client().admin().cluster()
                .prepareNodesInfo()
                .get()
        assertEquals(1, nodesResponse.nodes.size)

        val extFileService = ExternalFileService(Environment(settings))
        copyTestResources(extFileService)

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

        client().admin().indices().prepareRefresh().get()

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

//        extFileService.stopScheduler()
    }

    private fun copyTestResources(extFileService: ExternalFileService) {
        val indexPath = extFileService.getIndexDir("test")
        Files.createDirectories(indexPath)
        val resourcePath = getDataPath("/indices")
        Files.newInputStream(resourcePath.resolve("ext_price.txt")).use {
            val extFilePath = indexPath.resolve("ext_price.txt")
            logger.warn(">>> Copied external file to: $extFilePath")
            Files.copy(it, extFilePath)
        }
    }
}
