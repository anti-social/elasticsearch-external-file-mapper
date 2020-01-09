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

import java.util.Objects

import org.apache.lucene.index.IndexableField
import org.apache.lucene.index.IndexOptions
import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.index.SortedNumericDocValues
import org.apache.lucene.search.Query
import org.apache.lucene.search.SortField

import org.elasticsearch.cluster.metadata.IndexMetaData
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.index.Index
import org.elasticsearch.index.fielddata.*
import org.elasticsearch.index.fielddata.fieldcomparator.DoubleValuesComparatorSource
import org.elasticsearch.index.mapper.FieldMapper
import org.elasticsearch.index.mapper.MappedFieldType
import org.elasticsearch.index.mapper.Mapper
import org.elasticsearch.index.mapper.MapperParsingException
import org.elasticsearch.index.mapper.ParseContext
import org.elasticsearch.index.query.QueryShardContext
import org.elasticsearch.index.query.QueryShardException
import org.elasticsearch.search.MultiValueMode

import company.evo.elasticsearch.indices.*

import kotlin.IllegalStateException


class ExternalFileFieldMapper(
        simpleName: String,
        fieldType: MappedFieldType,
        defaultFieldType: MappedFieldType,
        indexSettings: Settings,
        multiFields: MultiFields,
        // Do we need that argument?
        copyTo: CopyTo?
) : FieldMapper(
        simpleName, fieldType, defaultFieldType,
        indexSettings, multiFields, copyTo) {

    companion object {
        const val CONTENT_TYPE = "external_file"
        val FIELD_TYPE = ExternalFileFieldType()

        init {
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE)
            FIELD_TYPE.setHasDocValues(false)
            FIELD_TYPE.freeze()
        }
    }

    override fun contentType() : String {
        return CONTENT_TYPE
    }

    override fun fieldType(): ExternalFileFieldType {
        return super.fieldType() as ExternalFileFieldType
    }

    override fun parseCreateField(context: ParseContext, fields: List<IndexableField>) {}

    override fun doXContentBody(builder: XContentBuilder, includeDefaults: Boolean, params: ToXContent.Params) {
        super.doXContentBody(builder, includeDefaults, params)

        val mapName = fieldType().mapName
        builder.field(
                "map_name",
                mapName ?: throw IllegalStateException("mapName must be set")
        )
        if (includeDefaults || fieldType().keyFieldName != null) {
            builder.field("key_field", fieldType().keyFieldName)
        }
        if (includeDefaults || fieldType().keyFieldName != null) {
            builder.field("scaling_factor", fieldType().scalingFactor)
        }
    }

    class TypeParser : Mapper.TypeParser {
        override fun parse(
                name: String,
                node: MutableMap<String, Any?>,
                parserContext: Mapper.TypeParser.ParserContext): Mapper.Builder<*,*>
        {
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
                    "scaling_factor" -> {
                        builder.scalingFactor = value?.toString()?.toLong()
                        entries.remove()
                    }
                    else -> {
                        throw MapperParsingException(
                                "[$key] parameter cannot be modified for field [$name]"
                        )
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
    ) : FieldMapper.Builder<Builder, ExternalFileFieldMapper>(
            name, FIELD_TYPE, FIELD_TYPE
    ) {
        var mapName: String? = null
        var keyFieldName: String? = null
        var scalingFactor: Long? = null

        override fun fieldType(): ExternalFileFieldType {
            return fieldType as ExternalFileFieldType
        }

        override fun build(context: BuilderContext): ExternalFileFieldMapper {
            val indexName = context.indexSettings()
                    .get(IndexMetaData.SETTING_INDEX_PROVIDED_NAME)
            val indexUuid = context.indexSettings()
                    .get(IndexMetaData.SETTING_INDEX_UUID)
            // There is no index when putting template
            if (indexName != null && indexUuid != null) {
                ExternalFileService.instance.addFile(
                        indexName,
                        name,
                        mapName ?: throw IllegalStateException("mapName property must be set")
                )
            }
            setupFieldType(context)
            fieldType().mapName = mapName
            fieldType().keyFieldName = keyFieldName
            fieldType().scalingFactor = scalingFactor
            return ExternalFileFieldMapper(
                    name, fieldType, defaultFieldType, context.indexSettings(),
                    multiFieldsBuilder.build(this, context), copyTo)
        }
    }

    class ExternalFileFieldType : MappedFieldType {
        var mapName: String? = null
        var keyFieldName: String? = null
        var scalingFactor: Long? = null

        constructor()

        private constructor(ref: ExternalFileFieldType) : super(ref) {
            this.mapName = ref.mapName
            this.keyFieldName = ref.keyFieldName
            this.scalingFactor = ref.scalingFactor
        }

        override fun clone(): ExternalFileFieldType {
            return ExternalFileFieldType(this)
        }

        override fun equals(other: Any?): Boolean {
            if (!super.equals(other)) {
                return false
            }
            if (other is ExternalFileFieldType) {
                return other.mapName == mapName &&
                        other.keyFieldName == keyFieldName &&
                        other.scalingFactor == scalingFactor
            }
            return false
        }

        override fun hashCode(): Int {
            return Objects.hash(super.hashCode(), mapName, keyFieldName, scalingFactor)
        }

        override fun typeName(): String {
            return CONTENT_TYPE
        }

        override fun termQuery(value: Any, context: QueryShardContext): Query {
            throw QueryShardException(
                    context,
                    "ExternalFile field type does not support search queries"
            )
        }

//        override fun existsQuery(context: QueryShardContext?): Query {
//            throw QueryShardException(
//                    context,
//                    "ExternalField field type does not support exists queries"
//            )
//        }

        override fun fielddataBuilder(): IndexFieldData.Builder {
            return IndexFieldData.Builder {
                indexSettings, _, cache, breakerService, mapperService ->

                val keyFieldType = mapperService.fullName(
                        keyFieldName ?: throw IllegalStateException("[keyFieldName is mandatory")
                ) ?: throw IllegalStateException("[$keyFieldName] field is missing")
                val keyFieldData = keyFieldType
                        .fielddataBuilder()
                        .build(
                                indexSettings, keyFieldType, cache, breakerService, mapperService
                        ) as? IndexNumericFieldData
                        ?: throw IllegalStateException("[$keyFieldName] field must be numeric")
                val values = ExternalFileService.instance.getValues(
                        mapName ?: throw IllegalStateException("[mapName] is mandatory")
                )
                ExternalFileFieldData(
                        name(), indexSettings.index, keyFieldData, values
                )
            }
        }
    }

    class ExternalFileFieldData(
            private val fieldName: String,
            private val index: Index,
            private val keyFieldData: IndexNumericFieldData,
            private val values: FileValues
    ) : IndexNumericFieldData {

        companion object {
            private const val DEFAULT_VALUE = 0.0
        }

        class AtomicNumericKeyFieldData(
                private val values: FileValues,
                private val keyFieldData: AtomicNumericFieldData
        ) : AtomicNumericFieldData {

            class Values(
                    private val values: FileValues,
                    private val keys: SortedNumericDocValues
            ) : SortedNumericDoubleValues() {

                private var value = DEFAULT_VALUE
                private var count = 0

                override fun setDocument(target: Int) {
                    keys.setDocument(target)
                    if (keys.count() > 0) {
                        val key = keys.valueAt(0)
                        if (values.contains(key)) {
                            count = 1
                            value = values.get(key, DEFAULT_VALUE)
                            return
                        }
                    }

                    count = 0
                    value = DEFAULT_VALUE
                }

                override fun valueAt(index: Int): Double = value

                override fun count(): Int = count
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

        override fun getNumericType(): IndexNumericFieldData.NumericType {
            return IndexNumericFieldData.NumericType.DOUBLE
        }

        override fun index(): Index {
            return index
        }

        override fun getFieldName(): String {
            return fieldName
        }

        override fun load(ctx: LeafReaderContext): AtomicNumericFieldData {
            return AtomicNumericKeyFieldData(values, keyFieldData.load(ctx))
        }

        override fun loadDirect(ctx: LeafReaderContext): AtomicNumericFieldData {
            return load(ctx)
        }

        override fun sortField(
                missingValue: Any?, sortMode: MultiValueMode,
                nested: IndexFieldData.XFieldComparatorSource.Nested?, reverse: Boolean): SortField
        {
            val source = DoubleValuesComparatorSource(this, missingValue, sortMode, nested)
            return SortField(fieldName, source, reverse)
        }

        override fun clear() {}
    }
}
