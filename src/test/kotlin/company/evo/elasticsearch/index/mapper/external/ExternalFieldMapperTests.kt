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

import java.util.Arrays

import org.elasticsearch.common.bytes.BytesReference
import org.elasticsearch.common.compress.CompressedXContent
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.IndexService
import org.elasticsearch.index.mapper.DocumentMapperParser
import org.elasticsearch.index.mapper.MapperParsingException
import org.elasticsearch.index.mapper.SourceToParse
import org.elasticsearch.plugins.Plugin
import org.elasticsearch.test.ESSingleNodeTestCase
import org.elasticsearch.test.InternalSettingsPlugin

import org.hamcrest.Matchers.*

import org.junit.Before

import company.evo.elasticsearch.indices.ExternalFileService
import company.evo.elasticsearch.plugin.mapper.ExternalFileMapperPlugin


class ExternalFieldMapperTests : ESSingleNodeTestCase() {

    lateinit var indexService: IndexService
    lateinit var parser: DocumentMapperParser

    @Before fun setup() {
        indexService = createIndex("test")
        parser = indexService.mapperService().documentMapperParser()
    }

    override fun getPlugins(): Collection<Class<out Plugin>> {
        return pluginList(
                InternalSettingsPlugin::class.java,
                ExternalFileMapperPlugin::class.java)
    }

    fun testDefaults() {
        val mapping = XContentFactory.jsonBuilder()
                .startObject().startObject("type")
                    .startObject("properties").startObject("ext_field")
                        .field("type", "external_file")
                        .field("update_interval", 600)
                    .endObject().endObject()
                .endObject().endObject()
        val mapper = parser.parse("type", CompressedXContent(BytesReference.bytes(mapping)))
        val parsedDoc = mapper.parse(
                SourceToParse.source(
                        "test", "type", "1",
                        BytesReference.bytes(
                                XContentFactory.jsonBuilder()
                                        .startObject()
                                            .field("ext_field", "value")
                                        .endObject()
                        ),
                        XContentType.JSON
                )
        )
        val fields = parsedDoc.rootDoc().getFields("ext_field")
        assertNotNull(fields)
        assertEquals(Arrays.toString(fields), 0, fields.size)
        assertEquals(
                600L,
                ExternalFileService.instance
                        .getFileSettings(indexService.index(), "ext_field")
                        ?.updateInterval)
    }

    fun testIdKeyField() {
        val mapping = XContentFactory.jsonBuilder()
                .startObject().startObject("type")
                    .startObject("properties")
                        .startObject("id")
                            .field("type", "long")
                            .field("index", false)
                            .field("doc_values", true)
                        .endObject()
                        .startObject("ext_field")
                            .field("type", "external_file")
                            .field("update_interval", 600)
                            .field("key_field", "id")
                        .endObject()
                    .endObject()
                .endObject().endObject()
        val mapper = parser.parse("type", CompressedXContent(BytesReference.bytes(mapping)))
        val parsedDoc = mapper.parse(
                SourceToParse.source(
                        "test", "type", "1",
                        BytesReference.bytes(
                                XContentFactory.jsonBuilder()
                                        .startObject()
                                            .field("id", 1)
                                            .field("ext_field", "value")
                                        .endObject()
                        ),
                        XContentType.JSON
                )
        )
        val fields = parsedDoc.rootDoc().getFields("ext_field")
        assertNotNull(fields)
        assertEquals(Arrays.toString(fields), 0, fields.size)
    }

    fun testTimeParsing() {
        val mapping = XContentFactory.jsonBuilder()
                .startObject().startObject("type")
                    .startObject("properties")
                        .startObject("ext_field_1")
                            .field("type", "external_file")
                            .field("update_interval", "600")
                            .field("timeout", 60)
                        .endObject()
                        .startObject("ext_field_2")
                            .field("type", "external_file")
                            .field("update_interval", "2h")
                            .field("timeout", "5m")
                        .endObject()
                    .endObject()
                .endObject().endObject()
        val mapper = parser.parse("type", CompressedXContent(BytesReference.bytes(mapping)))
        val extFieldMapper1 = mapper.mappers().getMapper("ext_field_1")
        assertThat(extFieldMapper1, `is`(instanceOf(ExternalFileFieldMapper::class.java)))
        val fileSettings1 = (extFieldMapper1 as ExternalFileFieldMapper)
                .fieldType()
                .fileSettings()
        assertEquals(fileSettings1?.updateInterval, 600L)
        assertEquals(fileSettings1?.timeout, 60)
        val extFieldMapper2 = mapper.mappers().getMapper("ext_field_2")
        assertThat(extFieldMapper2, `is`(instanceOf(ExternalFileFieldMapper::class.java)))
        val fileSettings2 = (extFieldMapper2 as ExternalFileFieldMapper)
                .fieldType()
                .fileSettings()
        assertEquals(fileSettings2?.updateInterval, 7200L)
        assertEquals(fileSettings2?.timeout, 300)
    }

    fun testUpdateScatterParsing() {
        val mapping = XContentFactory.jsonBuilder()
                .startObject().startObject("type")
                    .startObject("properties")
                        .startObject("ext_field_1")
                            .field("type", "external_file")
                            .field("update_interval", 60)
                            .field("update_scatter", "10%")
                        .endObject()
                        .startObject("ext_field_2")
                            .field("type", "external_file")
                            .field("update_interval", 3600)
                            .field("update_scatter", "5m")
                        .endObject()
                        .startObject("ext_field_3")
                            .field("type", "external_file")
                            .field("update_interval", 3600)
                            .field("update_scatter", 600)
                        .endObject()
                    .endObject()
                .endObject().endObject()
        val mapper = parser.parse("type", CompressedXContent(BytesReference.bytes(mapping)))
        val extFieldMapper1 = mapper.mappers().getMapper("ext_field_1")
        assertThat(extFieldMapper1, `is`(instanceOf(ExternalFileFieldMapper::class.java)))
        val fileSettings1 = (extFieldMapper1 as ExternalFileFieldMapper)
                .fieldType()
                .fileSettings()
        assertEquals(60L, fileSettings1?.updateInterval)
        assertEquals(6L, fileSettings1?.updateScatter)
        val extFieldMapper2 = mapper.mappers().getMapper("ext_field_2")
        assertThat(extFieldMapper2, `is`(instanceOf(ExternalFileFieldMapper::class.java)))
        val fileSettings2 = (extFieldMapper2 as ExternalFileFieldMapper)
                .fieldType()
                .fileSettings()
        assertEquals(3600L, fileSettings2?.updateInterval)
        assertEquals(300L, fileSettings2?.updateScatter)
        val extFieldMapper3 = mapper.mappers().getMapper("ext_field_3")
        assertThat(extFieldMapper3, `is`(instanceOf(ExternalFileFieldMapper::class.java)))
        val fileSettings3 = (extFieldMapper3 as ExternalFileFieldMapper)
                .fieldType()
                .fileSettings()
        assertEquals(3600L, fileSettings3?.updateInterval)
        assertEquals(600L, fileSettings3?.updateScatter)
    }

// TODO find a way to check existing of the key_field
//    fun testNonexistentIdKeyField() {
//        val mapping = XContentFactory.jsonBuilder()
//                .startObject().startObject("type")
//                    .startObject("properties")
//                        .startObject("ext_field")
//                            .field("type", "external_file")
//                            .field("key_field", "id")
//                        .endObject()
//                    .endObject()
//                .endObject().endObject().string()
//        try {
//            parser.parse("type", CompressedXContent(mapping))
//            fail("Expected a mapper parsing exception")
//        } catch (e: MapperParsingException) {
//            assertThat(e.message, containsString("[id] field not found"))
//        }
//    }

    fun testDocValuesNotAllowed() {
        val mapping = XContentFactory.jsonBuilder()
                .startObject().startObject("type")
                    .startObject("properties").startObject("ext_field")
                        .field("type", "external_file")
                        .field("update_interval", 600)
                        .field("doc_values", false)
                    .endObject().endObject()
                .endObject().endObject()
        try {
            parser.parse("type", CompressedXContent(BytesReference.bytes(mapping)))
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
                        .field("update_interval", 600)
                        .field("stored", true)
                    .endObject().endObject()
                .endObject().endObject()
        try {
            parser.parse("type", CompressedXContent(BytesReference.bytes(mapping)))
            fail("Expected a mapper parsing exception")
        } catch (e: MapperParsingException) {
            assertThat(e.message,
                containsString("Setting [stored] cannot be modified for field [ext_field]"))
        }
    }
}
