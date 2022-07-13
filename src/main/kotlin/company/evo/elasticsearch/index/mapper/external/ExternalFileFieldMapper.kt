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

import company.evo.elasticsearch.indices.*

import java.util.function.Supplier

import org.apache.lucene.document.FieldType
import org.apache.lucene.index.DocValuesType
import org.apache.lucene.index.IndexOptions
import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.index.SortedNumericDocValues
import org.apache.lucene.search.Query
import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.index.Index

import org.elasticsearch.index.fielddata.FieldData
import org.elasticsearch.index.fielddata.IndexFieldData
import org.elasticsearch.index.fielddata.IndexNumericFieldData
import org.elasticsearch.index.fielddata.LeafNumericFieldData
import org.elasticsearch.index.fielddata.ScriptDocValues
import org.elasticsearch.index.fielddata.SortedBinaryDocValues
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues
import org.elasticsearch.index.mapper.FieldMapper
import org.elasticsearch.index.mapper.MappedFieldType
import org.elasticsearch.index.mapper.Mapper
import org.elasticsearch.index.mapper.MapperParsingException
import org.elasticsearch.index.mapper.ParseContext
import org.elasticsearch.index.mapper.TextSearchInfo
import org.elasticsearch.index.query.QueryShardContext
import org.elasticsearch.index.query.QueryShardException
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType
import org.elasticsearch.search.aggregations.support.ValuesSourceType
import org.elasticsearch.search.lookup.SearchLookup

class ExternalFileFieldMapper private constructor(
    simpleName: String,
    fieldType: FieldType,
    mappedFieldType: MappedFieldType,
    multiFields: MultiFields,
    copyTo: CopyTo?
) : FieldMapper(
    simpleName,
    fieldType,
    mappedFieldType,
    multiFields,
    copyTo
) {
    companion object {
        @JvmStatic
        val CONTENT_TYPE = "external_file"

        @JvmStatic
        val FIELD_TYPE = FieldType().apply {
            setIndexOptions(IndexOptions.NONE)
            setDocValuesType(DocValuesType.NONE)
            freeze()
        }
    }

    class TypeParser : Mapper.TypeParser {
        override fun parse(
                name: String,
                node: MutableMap<String, Any?>,
                parserContext: Mapper.TypeParser.ParserContext
        ): Mapper.Builder<*> {
            val builder = Builder(name)
            val entries = node.entries.iterator()
            for ((key, value) in entries) {
                when (key) {
                    "type" -> {}
                    "key_field" -> {
                        builder.keyFieldName = value?.toString()
                        entries.remove()
                    }
                    "map_name" -> {
                        builder.mapName = value?.toString()
                        entries.remove()
                    }
                    "sharding" -> {
                        val sharding = value?.toString()?.toBoolean()
                        if (sharding != null) {
                            builder.sharding = sharding
                        }
                        entries.remove()
                    }
                    "scaling_factor" -> {
                        // Deprecated option: ignore it
                        entries.remove()
                    }
                }
            }
            if (builder.keyFieldName == null) {
                throw MapperParsingException(
                        "[key_field] parameter must be set for field [$name]"
                )
            }
            if (builder.mapName == null) {
                throw MapperParsingException(
                        "[map_name] parameter must be set for field [$name]"
                )
            }
            return builder
        }
    }

    class Builder(
        name: String
    ) : FieldMapper.Builder<Builder>(name, FIELD_TYPE) {
        var mapName: String? = null
        var keyFieldName: String? = null
        var sharding: Boolean = false

        override fun build(builderContext: BuilderContext): ExternalFileFieldMapper {
            val indexSettings = builderContext.indexSettings()
            val indexName = indexSettings.get(IndexMetadata.SETTING_INDEX_PROVIDED_NAME)
            val indexUuid = indexSettings.get(IndexMetadata.SETTING_INDEX_UUID)
            val numShards = indexSettings.get(IndexMetadata.SETTING_NUMBER_OF_SHARDS).toInt()

            val mapName = mapName ?: throw IllegalStateException("mapName property is required")
            val keyFieldName = keyFieldName ?: throw IllegalStateException("keyFieldName property is required")

            // There is no index when putting template
            if (indexName != null && indexUuid != null) {
                ExternalFileService.instance.addFile(
                    indexName,
                    name,
                    mapName,
                    sharding,
                    numShards
                )
            }

            return ExternalFileFieldMapper(
                name,
                fieldType,
                ExternalFileFieldType(
                    name, mapName, keyFieldName, sharding
                ),
                multiFieldsBuilder.build(this, builderContext),
                copyTo
            )
        }
    }

    override fun contentType() : String {
        return CONTENT_TYPE
    }

    override fun parseCreateField(context: ParseContext) {
        // Just ignore field values
    }

    override fun mergeOptions(other: FieldMapper, conflicts: MutableList<String>) {
        // All options are allowed to be changed
    }

    class ExternalFileFieldType(
        name: String,
        private val mapName: String,
        private val keyFieldName: String,
        private val sharding: Boolean
    ) : MappedFieldType(name, false, false, TextSearchInfo.NONE, emptyMap()) {

        override fun typeName(): String {
            return CONTENT_TYPE
        }

        override fun termQuery(value: Any, context: QueryShardContext?): Query {
            throw QueryShardException(
                    context,
                    "ExternalFile field type does not support search queries"
            )
        }

        override fun existsQuery(context: QueryShardContext): Query {
            throw QueryShardException(
                    context,
                    "ExternalField field type does not support exists queries"
            )
        }

        override fun fielddataBuilder(
            fullyQualifiedIndexName: String,
            searchLookupSupplier: Supplier<SearchLookup>
        ): IndexFieldData.Builder {
            return IndexFieldData.Builder { indexSettings, _, cache, breakerService, mapperService ->
                val keyFieldType = mapperService.fieldType(keyFieldName)
                val searchLookup = searchLookupSupplier.get()
                val shardId: Int = searchLookup.shardId()
                val keyFieldData = keyFieldType
                    .fielddataBuilder(fullyQualifiedIndexName, searchLookupSupplier)
                    .build(indexSettings, keyFieldType, cache, breakerService, mapperService) as? IndexNumericFieldData
                    ?: throw IllegalStateException("[$keyFieldName] field must be numeric")
                ExternalFileFieldData(
                    name(),
                    indexSettings.index,
                    keyFieldData,
                    ExternalFileService.instance.getValues(mapName, if (sharding) shardId else null)
                )
            }
        }
    }

    class ExternalFileFieldData(
        private val fieldName: String,
        private val index: Index,
        private val keyFieldData: IndexNumericFieldData,
        private val values: FileValues
    ) : IndexNumericFieldData() {

        companion object {
            private val DEFAULT_VALUE = Double.NaN
        }

        class AtomicNumericKeyFieldData(
                private val values: FileValues,
                private val keyFieldData: LeafNumericFieldData
        ) : LeafNumericFieldData {

            class Values(
                    private val values: FileValues,
                    private val keys: SortedNumericDocValues
            ) : SortedNumericDoubleValues() {

                private var value = DEFAULT_VALUE

                override fun advanceExact(target: Int): Boolean {
                    return if (keys.advanceExact(target)) {
                        val key = keys.nextValue()
                        val v = values.get(key, DEFAULT_VALUE)
                        if (!v.isNaN()) {
                            value = v
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                }

                override fun nextValue() = value

                override fun docValueCount() = 1
            }

            override fun getDoubleValues(): SortedNumericDoubleValues {
                return Values(values, keyFieldData.longValues)
            }

            override fun getLongValues(): SortedNumericDocValues {
                return FieldData.castToLong(doubleValues)
            }

            override fun getScriptValues(): ScriptDocValues.Doubles {
                return ScriptDocValues.Doubles(Values(values, keyFieldData.longValues))
            }

            override fun getBytesValues(): SortedBinaryDocValues {
                return FieldData.toString(doubleValues)
            }

            override fun ramBytesUsed(): Long {
                // TODO Calculate ram used
                return 0
            }

            override fun close() {}
        }

        override fun index(): Index {
            return index
        }

        override fun getValuesSourceType(): ValuesSourceType {
            return CoreValuesSourceType.NUMERIC
        }

        override fun sortRequiresCustomComparator(): Boolean {
            return true
        }

        override fun getNumericType(): IndexNumericFieldData.NumericType {
            return NumericType.DOUBLE
        }

        override fun getFieldName(): String {
            return fieldName
        }

        override fun load(ctx: LeafReaderContext): LeafNumericFieldData {
            return AtomicNumericKeyFieldData(values, keyFieldData.load(ctx))
        }

        override fun loadDirect(ctx: LeafReaderContext): LeafNumericFieldData {
            return load(ctx)
        }

        override fun clear() {}
    }
}
