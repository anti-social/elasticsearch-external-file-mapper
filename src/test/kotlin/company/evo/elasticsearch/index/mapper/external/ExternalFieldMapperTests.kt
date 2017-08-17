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
import java.util.Arrays
import java.util.Collections

import org.elasticsearch.common.compress.CompressedXContent
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.env.Environment
import org.elasticsearch.index.IndexService
import org.elasticsearch.index.mapper.DocumentMapperParser
import org.elasticsearch.index.mapper.Mapper.TypeParser
import org.elasticsearch.index.mapper.MapperParsingException
import org.elasticsearch.indices.mapper.MapperRegistry
import org.elasticsearch.plugins.Plugin
import org.elasticsearch.test.ESSingleNodeTestCase
import org.elasticsearch.test.InternalSettingsPlugin

import org.hamcrest.Matchers.containsString

import org.junit.Before

import company.evo.elasticsearch.indices.ExternalFileService
import java.io.PrintWriter
import java.nio.file.StandardOpenOption


class ExternalFieldMapperTests : ESSingleNodeTestCase() {

    lateinit var indexService: IndexService
    lateinit var extFileService: ExternalFileService
    lateinit var mapperRegistry: MapperRegistry
    lateinit var parser: DocumentMapperParser

    @Before fun setup() {
        this.indexService = this.createIndex("test")
        val nodeSettings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.key, createTempDir())
                .build()
        this.extFileService = ExternalFileService(
                nodeSettings, indexService.threadPool, node().nodeEnvironment)
        this.extFileService.doStart()
        this.mapperRegistry = MapperRegistry(
            Collections.singletonMap(
                ExternalFileFieldMapper.CONTENT_TYPE,
                ExternalFileFieldMapper.TypeParser() as TypeParser),
            Collections.emptyMap())
        this.parser = DocumentMapperParser(
            indexService.indexSettings, indexService.mapperService(),
            indexService.indexAnalyzers, indexService.xContentRegistry(),
            indexService.similarityService(), mapperRegistry,
            { indexService.newQueryShardContext(
                    0, null,
                    { throw UnsupportedOperationException() }) })
    }

    override fun getPlugins(): Collection<Class<out Plugin>> {
        return pluginList(InternalSettingsPlugin::class.java)
    }

    fun testDefaults() {
        val mapping = XContentFactory.jsonBuilder()
                .startObject().startObject("type")
                    .startObject("properties").startObject("ext_field")
                        .field("type", "external_file")
                    .endObject().endObject()
                .endObject().endObject().string()
        val mapper = parser.parse("type", CompressedXContent(mapping))
        val parsedDoc = mapper.parse("test", "type", "1",
            XContentFactory.jsonBuilder().startObject().field("ext_field", "value").endObject().bytes())
        val fields = parsedDoc.rootDoc().getFields("ext_field")
        assertNotNull(fields)
        assertEquals(Arrays.toString(fields), 0, fields.size)
    }

    fun testUpdateInterval() {
        val mapping = XContentFactory.jsonBuilder()
                .startObject().startObject("type")
                    .startObject("properties").startObject("ext_field")
                        .field("type", "external_file")
                        .field("update_interval", 60)
                    .endObject().endObject()
                .endObject().endObject().string()
        val mapper = parser.parse("type", CompressedXContent(mapping))
        val parsedDoc = mapper.parse("test", "type", "1",
                XContentFactory.jsonBuilder().startObject().field("ext_field", "value").endObject().bytes())
        val fields = parsedDoc.rootDoc().getFields("ext_field")
        assertNotNull(fields)
        assertEquals(Arrays.toString(fields), 0, fields.size)
        assertEquals(60L, extFileService.getUpdateInterval(indexService.index(), "ext_field"))
    }

    fun testDocValuesNotAllowed() {
        val mapping = XContentFactory.jsonBuilder()
                .startObject().startObject("type")
                    .startObject("properties").startObject("ext_field")
                        .field("type", "external_file")
                        .field("doc_values", false)
                    .endObject().endObject()
                .endObject().endObject().string()
        try {
            parser.parse("type", CompressedXContent(mapping))
            fail("Expected a mapper parsing exception")
        } catch (e: MapperParsingException) {
            assertThat(e.message,
                containsString("Setting [doc_values] cannot be modified for field [ext_field]"))
        }
    }

    fun testStoredNotAllowed() {
        val mapping = XContentFactory.jsonBuilder()
                .startObject().startObject("type")
                    .startObject("properties").startObject("ext_field")
                        .field("type", "external_file")
                        .field("stored", true)
                    .endObject().endObject()
                .endObject().endObject().string()
        try {
            parser.parse("type", CompressedXContent(mapping))
            fail("Expected a mapper parsing exception")
        } catch (e: MapperParsingException) {
            assertThat(e.message,
                containsString("Setting [stored] cannot be modified for field [ext_field]"))
        }
    }

    fun testExternalFileService() {
        copyTestResources()
        extFileService.tryLoad("test", "ext_price")
        var values = extFileService.getValues("test", "ext_price")
        assertEquals(3, values.size)
        assertEquals(1.1, values.get("1"))
        assertEquals(1.2, values.get("2"))
        assertEquals(1.3, values.get("3"))
        assertEquals(null, values.get("4"))

        val extFilePath = extFileService.getExternalFilePath("test", "ext_price")
        Files.newBufferedWriter(extFilePath, StandardOpenOption.APPEND).use {
            val out = PrintWriter(it)
            out.println("4=1.4")
        }
        extFileService.tryLoad("test", "ext_price")

        values = extFileService.getValues("test", "ext_price")
        assertEquals(4, values.size)
        assertEquals(1.1, values.get("1"))
        assertEquals(1.2, values.get("2"))
        assertEquals(1.3, values.get("3"))
        assertEquals(1.4, values.get("4"))
    }

    private fun copyTestResources() {
        val extFilePath = extFileService.getExternalFilePath("test", "ext_price")
        Files.createDirectories(extFilePath.parent)
        val resourcePath = getDataPath("/indices")
        Files.newInputStream(resourcePath.resolve("ext_price.txt")).use {
            Files.copy(it, extFilePath)
        }
    }
}
