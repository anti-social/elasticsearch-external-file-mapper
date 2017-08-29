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
import org.elasticsearch.index.mapper.FieldMapper
import org.elasticsearch.index.mapper.IdFieldMapper
import org.elasticsearch.index.mapper.MappedFieldType
import org.elasticsearch.index.mapper.Mapper
import org.elasticsearch.index.mapper.MapperParsingException
import org.elasticsearch.index.mapper.ParseContext
import org.elasticsearch.index.mapper.Uid
import org.elasticsearch.index.mapper.UidFieldMapper
import org.elasticsearch.index.query.QueryShardContext
import org.elasticsearch.index.query.QueryShardException
import org.elasticsearch.search.MultiValueMode


import company.evo.elasticsearch.indices.ExternalFileService
import company.evo.elasticsearch.indices.FileSettings
import company.evo.elasticsearch.indices.FileValues


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
        const val DEFAULT_UPDATE_INTERVAL = 600L
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
        if (includeDefaults || fieldType().keyFieldName() != null) {
            builder.field("key_field", fieldType().keyFieldName())
        }
    }

    class TypeParser : Mapper.TypeParser {
        override fun parse(
                name: String,
                node: MutableMap<String, Any>,
                parserContext: Mapper.TypeParser.ParserContext): Mapper.Builder<*,*>
        {
            val builder = Builder(name, ExternalFileService.instance)
            val entries = node.entries.iterator()
            for ((key, value) in entries) {
                when (key) {
                    "type" -> {}
                    "key_field" -> {
                        builder.keyField(value.toString())
                        entries.remove()
                    }
                    "update_interval" -> {
                        builder.updateInterval(value.toString().toLong())
                        entries.remove()
                    }
                    "url" -> {
                        builder.url(value.toString())
                        entries.remove()
                    }
                    "timeout" -> {
                        builder.timeout(value.toString().toInt())
                        entries.remove()
                    }
                    else -> {
                        throw MapperParsingException(
                                "Setting [${key}] cannot be modified for field [$name]")
                    }
                }
            }
            return builder
        }
    }

    class Builder : FieldMapper.Builder<Builder, ExternalFileFieldMapper> {

        private val extFileService: ExternalFileService
        private var updateInterval: Long? = null
        private var url: String? = null
        private var timeout: Int? = null

        constructor(
                name: String,
                extFileService: ExternalFileService
        ) : super(name, FIELD_TYPE, FIELD_TYPE)
        {
            this.builder = this
            this.extFileService = extFileService
        }

        override fun fieldType(): ExternalFileFieldType {
            return fieldType as ExternalFileFieldType
        }

        override fun build(context: BuilderContext): ExternalFileFieldMapper {
            val indexName = context.indexSettings()
                    .get(IndexMetaData.SETTING_INDEX_PROVIDED_NAME)
            val indexUuid = context.indexSettings()
                    .get(IndexMetaData.SETTING_INDEX_UUID)
            extFileService.addField(
                    Index(indexName, indexUuid), name,
                    FileSettings(
                            this.updateInterval ?: DEFAULT_UPDATE_INTERVAL,
                            url, timeout))
            setupFieldType(context)
            return ExternalFileFieldMapper(
                    name, fieldType, defaultFieldType, context.indexSettings(),
                    multiFieldsBuilder.build(this, context), copyTo)
        }

        override fun setupFieldType(context: BuilderContext) {
            super.setupFieldType(context)
            fieldType().setExtFileService(extFileService)
        }

        fun keyField(keyFieldName: String): Builder {
            fieldType().setKeyFieldName(keyFieldName)
            return this
        }

        fun updateInterval(interval: Long): Builder {
            this.updateInterval = interval
            return this
        }

        fun url(url: String): Builder {
            this.url = url
            return this
        }

        fun timeout(timeout: Int): Builder {
            this.timeout = timeout
            return this
        }
    }

    class ExternalFileFieldType : MappedFieldType {
        private var extFileService: ExternalFileService? = null
        private var keyFieldName: String? = null

        constructor()
        private constructor(ref: ExternalFileFieldType) : super(ref) {
            this.keyFieldName = ref.keyFieldName
        }

        fun setExtFileService(extFileService: ExternalFileService) {
            this.extFileService = extFileService
        }

        fun keyFieldName(): String? {
            return keyFieldName
        }

        fun setKeyFieldName(keyFieldName: String) {
            this.keyFieldName = keyFieldName
        }

        override fun typeName(): String {
            return CONTENT_TYPE
        }

        override fun clone(): ExternalFileFieldType {
            return ExternalFileFieldType(this)
        }

        override fun termQuery(value: Any, context: QueryShardContext): Query {
            throw QueryShardException(context, "ExternalFile fields are not searcheable")
        }

        override fun fielddataBuilder(): IndexFieldData.Builder {
            val extFileService = this.extFileService
            return IndexFieldData.Builder {
                indexSettings, _, cache, breakerService, mapperService ->

                val keyFieldType = when (keyFieldName) {
                    null -> {
                        if (indexSettings.isSingleType) {
                            mapperService.fullName(IdFieldMapper.NAME)
                        } else {
                            mapperService.fullName(UidFieldMapper.NAME)
                        }
                    }
                    else -> {
                        mapperService.fullName(keyFieldName)
                    }
                }
                val keyFieldData = keyFieldType.fielddataBuilder().build(
                        indexSettings, keyFieldType, cache, breakerService, mapperService)
                if (extFileService == null) {
                    throw IllegalArgumentException("Missing external file service")
                }
                val values = extFileService.getValues(indexSettings.index, name())
                ExternalFileFieldData(
                        name(), indexSettings.index, keyFieldData, values)
            }
        }
    }

    class ExternalFileFieldData(
            private val fieldName: String,
            private val index: Index,
            private val keyFieldData: IndexFieldData<*>,
            private val values: FileValues
    ) : IndexNumericFieldData {

        class AtomicUidKeyFieldData(
                private val values: FileValues,
                private val keyFieldData: AtomicFieldData
        ) : AtomicNumericFieldData {

            class Values(
                    private val values: FileValues,
                    private val uids: SortedBinaryDocValues
            ) : SortedNumericDoubleValues() {

                private var doc: Int = -1

                override fun setDocument(doc: Int) {
                    this.doc = doc
                    uids.setDocument(doc)
                }

                override fun valueAt(index: Int): Double {
                    return try {
                        values.get(getKey(), 0.0)
                    } catch (e: NumberFormatException) {
                        0.0
                    }
                }

                override fun count(): Int {
                    return try {
                        if (values.contains(getKey())) 1 else 0
                    } catch (e: NumberFormatException) {
                        0
                    }
                }

                private fun getKey(): Long {
                    return Uid.createUid(uids.valueAt(0).utf8ToString()).id().toLong()
                }
            }

            override fun getDoubleValues(): SortedNumericDoubleValues {
                return Values(values, keyFieldData.bytesValues)
            }

            override fun getLongValues(): SortedNumericDocValues {
                throw UnsupportedOperationException("getLongValues: not implemented")
            }

            override fun getScriptValues(): ScriptDocValues.Doubles {
                return ScriptDocValues.Doubles(Values(values, keyFieldData.bytesValues))
            }

            override fun getBytesValues(): SortedBinaryDocValues {
                throw UnsupportedOperationException("getBytesValues: not implemented")
            }

            override fun ramBytesUsed(): Long {
                return 0
            }

            override fun close() {}
        }

        class AtomicNumericKeyFieldData(
                private val values: FileValues,
                private val keyFieldData: AtomicNumericFieldData
        ) : AtomicNumericFieldData {

            class Values(
                    private val values: FileValues,
                    private val keys: SortedNumericDocValues
            ) : SortedNumericDoubleValues() {

                private var doc: Int = -1

                override fun setDocument(doc: Int) {
                    this.doc = doc
                    keys.setDocument(doc)
                }

                override fun valueAt(index: Int): Double {
                    return values.get(keys.valueAt(0), 0.0)
                }

                override fun count(): Int {
                    return if (values.contains(keys.valueAt(0))) 1 else 0
                }
            }

            override fun getDoubleValues(): SortedNumericDoubleValues {
                return Values(values, keyFieldData.longValues)
            }

            override fun getLongValues(): SortedNumericDocValues {
                throw UnsupportedOperationException("getLongValues: not implemented")
            }

            override fun getScriptValues(): ScriptDocValues.Doubles {
                return ScriptDocValues.Doubles(Values(values, keyFieldData.longValues))
            }

            override fun getBytesValues(): SortedBinaryDocValues {
                throw UnsupportedOperationException("getBytesValues: not implemented")
            }

            override fun ramBytesUsed(): Long {
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
            if (keyFieldData is IndexNumericFieldData) {
                return AtomicNumericKeyFieldData(values, keyFieldData.load(ctx))
            }
            return AtomicUidKeyFieldData(values, keyFieldData.load(ctx))
        }

        override fun loadDirect(ctx: LeafReaderContext): AtomicNumericFieldData {
            return load(ctx)
        }

        override fun sortField(
                missingValue: Any?, sortMode: MultiValueMode,
                nested: IndexFieldData.XFieldComparatorSource.Nested, reverse: Boolean): SortField
        {
            throw UnsupportedOperationException("sortField: not implemented")
        }

        override fun clear() {}
    }
}
