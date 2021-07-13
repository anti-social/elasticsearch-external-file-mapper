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

import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.index.SortedNumericDocValues
import org.apache.lucene.search.Query

import org.elasticsearch.index.IndexSettings
import org.elasticsearch.index.fielddata.FieldData
import org.elasticsearch.index.fielddata.IndexFieldData
import org.elasticsearch.index.fielddata.IndexNumericFieldData
import org.elasticsearch.index.fielddata.LeafNumericFieldData
import org.elasticsearch.index.fielddata.ScriptDocValues
import org.elasticsearch.index.fielddata.SortedBinaryDocValues
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues
import org.elasticsearch.index.mapper.ContentPath
import org.elasticsearch.index.mapper.FieldMapper
import org.elasticsearch.index.mapper.MappedFieldType
import org.elasticsearch.index.mapper.ParseContext
import org.elasticsearch.index.mapper.TextSearchInfo
import org.elasticsearch.index.mapper.ValueFetcher
import org.elasticsearch.index.query.QueryShardException
import org.elasticsearch.index.query.SearchExecutionContext
import org.elasticsearch.search.aggregations.support.ValuesSourceType
import org.elasticsearch.search.lookup.SearchLookup

import java.util.function.Supplier
import org.elasticsearch.index.mapper.MapperParsingException
import org.elasticsearch.index.mapper.MappingLookup
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType

class ExternalFileFieldMapper private constructor(
    simpleName: String,
    fieldType: MappedFieldType,
    multiFields: MultiFields,
    copyTo: CopyTo?,
    builder: Builder
) : FieldMapper(
    simpleName,
    fieldType,
    multiFields,
    copyTo
) {
    private val hasDocValues: Boolean = builder.hasDocValues.get()
    private val mapName: String = builder.mapName.get()
    private val keyFieldName: String = builder.keyFieldName.get()
    private val sharding: Boolean = builder.sharding.get()

    companion object {
        @JvmStatic
        val CONTENT_TYPE = "external_file"

        @JvmStatic
        val PARSER = TypeParser { name, ctx ->
            Builder(name, ctx.indexSettings)
        }

        @JvmStatic
        fun toType(mapper: FieldMapper): ExternalFileFieldMapper {
            return mapper as ExternalFileFieldMapper
        }
    }

    class Builder(
        name: String,
        private val indexSettings: IndexSettings?
    ) : FieldMapper.Builder(name) {
        val mapName = Parameter.stringParam("map_name", true, { m -> toType(m).mapName }, "null")
        val keyFieldName = Parameter.stringParam("key_field", true, { m -> toType(m).keyFieldName }, null)
        val sharding = Parameter.boolParam("sharding", true, { m -> toType(m).sharding }, false)

        val hasDocValues = Parameter.docValuesParam({ m -> toType(m).hasDocValues }, true)

        // Ignored, but keep it to work with old indexes
        val scalingFactor = Parameter.floatParam("scaling_factor", true, { 0.0F }, 0.0F)

        override fun getParameters(): MutableList<Parameter<*>> {
            return mutableListOf(hasDocValues, mapName, keyFieldName, sharding, scalingFactor)
        }

        override fun build(contentPath: ContentPath): ExternalFileFieldMapper {
            val indexMetadata = indexSettings?.indexMetadata
            val indexName = indexMetadata?.index?.name
            val indexUuid = indexMetadata?.indexUUID
            val numShards = indexMetadata?.numberOfShards

            // There is no index when putting template
            if (indexName != null && indexUuid != null && numShards != null) {
                ExternalFileService.instance.addFile(
                    indexName,
                    name,
                    mapName.get(),
                    sharding.get(),
                    numShards
                )
            }

            return ExternalFileFieldMapper(
                name,
                ExternalFileFieldType(
                    buildFullName(contentPath),
                    mapName.get(), keyFieldName.get(), sharding.get()
                ),
                multiFieldsBuilder.build(this, contentPath),
                copyTo.build(),
                this
            )
        }
    }

    override fun doValidate(mappers: MappingLookup) {
        if (!hasDocValues) {
            throw MapperParsingException(
                "[doc_values] parameter cannot be modified for field [${name()}]"
            )
        }
        mappers.getFieldType(keyFieldName)
            ?: throw MapperParsingException("[$keyFieldName] field not found")
    }

    override fun contentType() : String {
        return CONTENT_TYPE
    }

    override fun parseCreateField(context: ParseContext) {
        // Just ignore field values
    }

    override fun getMergeBuilder(): FieldMapper.Builder {
        return Builder(simpleName(), null).init(this)
    }

    class ExternalFileFieldType(
        name: String,
        private val mapName: String,
        private val keyFieldName: String,
        private val sharding: Boolean
    ) : MappedFieldType(name, false, false, true, TextSearchInfo.NONE, emptyMap()) {

        override fun typeName(): String {
            return CONTENT_TYPE
        }

        override fun termQuery(value: Any, context: SearchExecutionContext): Query {
            throw QueryShardException(
                    context,
                    "ExternalFile field type does not support search queries"
            )
        }

        override fun existsQuery(context: SearchExecutionContext): Query {
            throw QueryShardException(
                    context,
                    "ExternalField field type does not support exists queries"
            )
        }

        override fun valueFetcher(context: SearchExecutionContext, format: String?): ValueFetcher {
            return ValueFetcher {
                emptyList()
            }
        }

        override fun fielddataBuilder(
            fullyQualifiedIndexName: String,
            searchLookupSupplier: Supplier<SearchLookup>
        ): IndexFieldData.Builder {
            return IndexFieldData.Builder { cache, breakerService ->
                val searchLookup = searchLookupSupplier.get()
                val shardId: Int = searchLookup.shardId()
                val keyFieldType = searchLookup.fieldType(keyFieldName)
                val keyFieldData = keyFieldType
                    .fielddataBuilder(fullyQualifiedIndexName, searchLookupSupplier)
                    .build(cache, breakerService) as? IndexNumericFieldData
                    ?: throw IllegalStateException("[$keyFieldName] field must be numeric")
                ExternalFileFieldData(
                    name(),
                    keyFieldData,
                    ExternalFileService.instance.getValues(mapName, if (sharding) shardId else null)
                )
            }
        }
    }

    class ExternalFileFieldData(
            private val fieldName: String,
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
    }
}
