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
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.IndexService
import org.elasticsearch.index.mapper.DocumentMapperParser
import org.elasticsearch.index.mapper.MapperParsingException
import org.elasticsearch.index.mapper.SourceToParse
import org.elasticsearch.plugins.Plugin
import org.elasticsearch.test.ESSingleNodeTestCase
import org.elasticsearch.test.InternalSettingsPlugin

import org.hamcrest.Matchers.containsString

import org.junit.Before

import company.evo.elasticsearch.indices.ExternalFileService
import company.evo.elasticsearch.plugin.mapper.ExternalFileMapperPlugin
import company.evo.persistent.hashmap.straight.StraightHashMapEnv
import company.evo.persistent.hashmap.straight.StraightHashMapType_Int_Float

import java.nio.file.Files

inline fun XContentBuilder.obj(
        name: String? = null,
        block: XContentBuilder.() -> Unit
): XContentBuilder {
    if (name != null) startObject(name) else startObject()
    block()
    endObject()
    return this
}

class ExternalFieldMapperTests : ESSingleNodeTestCase() {

    lateinit var indexService: IndexService
    lateinit var parser: DocumentMapperParser

    @Before fun setup() {
        indexService = createIndex("test")
        parser = indexService.mapperService().documentMapperParser()
    }

    private fun initMap(name: String, entries: Map<Int, Float>) {
        StraightHashMapEnv.Builder(StraightHashMapType_Int_Float)
                .useUnmapHack(true)
                .open(
                        ExternalFileService.instance.getExternalFileDir(name)
                                .also { Files.createDirectories(it) }
                )
                .use { mapEnv ->
                    mapEnv.openMap().use { map ->
                        entries.forEach { k, v ->
                            map.put(k, v)
                        }
                    }
                }
    }

    override fun getPlugins(): Collection<Class<out Plugin>> {
        return pluginList(
                InternalSettingsPlugin::class.java,
                ExternalFileMapperPlugin::class.java)
    }

    fun testDefaults() {
        initMap("test_ext_file", mapOf(1 to 1.1F))

        val mapping = jsonBuilder().obj {
            obj("type") {
                obj("properties") {
                    obj("id") {
                        field("type", "integer")
                    }
                    obj("ext_field") {
                        field("type", "external_file")
                        field("key_field", "id")
                        field("map_name", "test_ext_file")
                    }
                }
            }
        }
        val mapper = parser.parse("type", CompressedXContent(BytesReference.bytes(mapping)))

        val parsedDoc = mapper.parse(
                SourceToParse.source(
                        "test", "type", "1",
                        BytesReference.bytes(
                                jsonBuilder().obj {
                                    field("id", 1)
                                    field("ext_field", "value")
                                }
                        ),
                        XContentType.JSON
                )
        )
        val testExtFileFields = parsedDoc.rootDoc().getFields("ext_field")
        assertNotNull(testExtFileFields)
        assertEquals(Arrays.toString(testExtFileFields), 0, testExtFileFields.size)
        val idFields = parsedDoc.rootDoc().getFields("id")
        assertNotNull(idFields)
        assertEquals(Arrays.toString(idFields), 2, idFields.size)
    }

    // TODO find a way to check existing of the key_field when parsing mapping
    // fun testNonexistentIdKeyField() {
    //     val mapping = jsonBuilder().obj {
    //         obj("type") {
    //             obj("properties") {
    //                 obj("ext_field") {
    //                     field("type", "external_file")
    //                     field("key_field", "id")
    //                     field("map_name", "test_ext_file")
    //                 }
    //             }
    //         }
    //     }
    //     try {
    //         parser.parse("type", CompressedXContent(BytesReference.bytes(mapping)))
    //         fail("Expected a mapper parsing exception")
    //     } catch (e: MapperParsingException) {
    //         assertThat(e.message, containsString("[id] field not found"))
    //     }
    // }

    fun testDocValuesNotAllowed() {
        val mapping = jsonBuilder().obj {
            obj("type") {
                obj("properties") {
                    obj("ext_field") {
                        field("type", "external_file")
                        field("key_field", "id")
                        field("map_name", "test_ext_file")
                        field("doc_values", false)
                    }
                }
            }
        }
        try {
            parser.parse("type", CompressedXContent(BytesReference.bytes(mapping)))
            fail("Expected a mapper parsing exception")
        } catch (e: MapperParsingException) {
            assertThat(
                    e.message,
                    containsString("[doc_values] parameter cannot be modified for field [ext_field]")
            )
        }
    }

    fun testStoredNotAllowed() {
        val mapping = jsonBuilder().obj {
            obj("type") {
                obj("properties") {
                    obj("ext_field") {
                        field("type", "external_file")
                        field("key_field", "id")
                        field("map_name", "test_ext_file")
                        field("stored", true)
                    }
                }
            }
        }
        try {
            parser.parse("type", CompressedXContent(BytesReference.bytes(mapping)))
            fail("Expected a mapper parsing exception")
        } catch (e: MapperParsingException) {
            assertThat(
                    e.message,
                    containsString("[stored] parameter cannot be modified for field [ext_field]")
            )
        }
    }
}
