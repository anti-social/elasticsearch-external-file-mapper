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

import java.util.Locale
import java.util.Objects

import org.apache.lucene.index.IndexableField
import org.apache.lucene.index.IndexOptions
import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.index.SortedNumericDocValues
import org.apache.lucene.search.Query
import org.apache.lucene.search.SortField

import org.elasticsearch.cluster.metadata.IndexMetaData
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.index.Index
import org.elasticsearch.index.fielddata.*
import org.elasticsearch.index.fielddata.fieldcomparator.DoubleValuesComparatorSource
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

import company.evo.elasticsearch.indices.*


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
        val DEFAULT_VALUES_STORE_TYPE = ValuesStoreType.RAM
        val FIELD_TYPE = ExternalFileFieldType()

        init {
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE)
            FIELD_TYPE.setHasDocValues(false)
            FIELD_TYPE.freeze()
        }
    }

    data class TimeValueWithOriginal(
            val time: TimeValue,
            val original: Any
    ) {
        companion object {
            fun parse(value: Any, fieldName: String): TimeValueWithOriginal {
                val valueStr = value.toString()
                val timeValue = try {
                    TimeValue.timeValueSeconds(valueStr.toLong())
                } catch (_: NumberFormatException) {
                    TimeValue.parseTimeValue(valueStr, fieldName)
                }
                return TimeValueWithOriginal(timeValue, value)
            }
        }
    }

    data class TimeValueOrPercent(
            private val percent: Long?,
            private val time: TimeValue?,
            val original: Any
    ) {
        companion object {
            fun parse(value: Any, fieldName: String): TimeValueOrPercent {
                val valueStr = value.toString()
                return if (valueStr.endsWith("%")) {
                    TimeValueOrPercent(valueStr.substring(0, valueStr.length - 1).toLong(), null, value)
                } else {
                    val timeValue = try {
                        TimeValue.timeValueSeconds(valueStr.toLong())
                    } catch (_: NumberFormatException) {
                        TimeValue.parseTimeValue(valueStr, fieldName)
                    }
                    TimeValueOrPercent(null, timeValue, value)
                }
            }
        }

        fun getTimeValue(baseTimeValue: TimeValue): TimeValue {
            return if (percent != null) {
                TimeValue.timeValueSeconds(baseTimeValue.seconds * percent / 100)
            } else if (time != null) {
                time
            } else {
                throw IllegalStateException("Percent or time must be set")
            }
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

        val fileSettings = fieldType().fileSettings()
        val updateInterval = fieldType().originalUpdateInterval()
        val updateScatter = fieldType().originalUpdateScatter()
        val timeout = fieldType().originalTimeout()

        // TODO Find out how to rid of next check
        fileSettings ?: throw IllegalStateException("fileSettings must be set")
        updateInterval ?: throw IllegalStateException("updateInterval must be set")

        builder.field("update_interval", updateInterval)
        if (includeDefaults || updateScatter != null) {
            builder.field("update_scatter", updateScatter)
        }
        if (includeDefaults || fieldType().keyFieldName() != null) {
            builder.field("key_field", fieldType().keyFieldName())
        }
        if (includeDefaults || fileSettings.valuesStoreType != DEFAULT_VALUES_STORE_TYPE) {
            builder.field("values_store_type",
                    fileSettings.valuesStoreType.toString().toLowerCase(Locale.ENGLISH))
        }
        if (includeDefaults || fileSettings.scalingFactor != null) {
            builder.field("scaling_factor", fileSettings.scalingFactor)
        }
        if (includeDefaults || fileSettings.url != null) {
            builder.field("url", fileSettings.url)
        }
        if (includeDefaults || timeout != null) {
            builder.field("timeout", timeout)
        }
    }

    class TypeParser : Mapper.TypeParser {
        override fun parse(
                name: String,
                node: MutableMap<String, Any>,
                parserContext: Mapper.TypeParser.ParserContext): Mapper.Builder<*,*>
        {
            val builder = Builder(name)
            val entries = node.entries.iterator()
            for ((key, value) in entries) {
                when (key) {
                    "type" -> {}
                    "key_field" -> {
                        builder.keyField(value.toString())
                        entries.remove()
                    }
                    "values_store_type" -> {
                        builder.valuesStoreType(
                                ValuesStoreType.valueOf(value.toString().toUpperCase(Locale.ENGLISH)))
                        entries.remove()
                    }
                    "scaling_factor" -> {
                        builder.scalingFactor(value.toString().toLong())
                        entries.remove()
                    }
                    "update_interval" -> {
                        builder.updateInterval(
                                TimeValueWithOriginal.parse(value, "update_interval"))
                        entries.remove()
                    }
                    "update_scatter" -> {
                        builder.updateScatter(
                                TimeValueOrPercent.parse(value, "update_scatter"))
                        entries.remove()
                    }
                    "url" -> {
                        builder.url(value.toString())
                        entries.remove()
                    }
                    "timeout" -> {
                        builder.timeout(TimeValueWithOriginal.parse(value, "timeout"))
                        entries.remove()
                    }
                    else -> {
                        throw MapperParsingException(
                                "Setting [${key}] cannot be modified for field [$name]")
                    }
                }
            }
            if (builder.updateInterval() == null) {
                throw MapperParsingException(
                        "update_interval parameter must be set for field [$name]")
            }
            return builder
        }
    }

    class Builder : FieldMapper.Builder<Builder, ExternalFileFieldMapper> {

        private var valuesStoreType: ValuesStoreType = DEFAULT_VALUES_STORE_TYPE
        private var scalingFactor: Long? = null
        private var updateInterval: TimeValueWithOriginal? = null
        private var updateScatter: TimeValueOrPercent? = null
        private var url: String? = null
        private var timeout: TimeValueWithOriginal? = null

        constructor(name: String) : super(name, FIELD_TYPE, FIELD_TYPE) {
            this.builder = this
        }

        override fun fieldType(): ExternalFileFieldType {
            return fieldType as ExternalFileFieldType
        }

        override fun build(context: BuilderContext): ExternalFileFieldMapper {
            val updateInterval = this.updateInterval
            updateInterval ?: throw IllegalStateException("update_interval must be set")
            val indexName = context.indexSettings()
                    .get(IndexMetaData.SETTING_INDEX_PROVIDED_NAME)
            val indexUuid = context.indexSettings()
                    .get(IndexMetaData.SETTING_INDEX_UUID)
            val updateIntervalScatter = updateScatter?.getTimeValue(updateInterval.time)
            val fileSettings = FileSettings(
                    valuesStoreType,
                    updateInterval.time.seconds,
                    updateIntervalScatter?.seconds,
                    scalingFactor,
                    url,
                    timeout?.time?.seconds?.toInt())
            // There is no index when putting template
            if (indexName != null && indexUuid != null) {
                ExternalFileService.instance.addField(
                        Index(indexName, indexUuid), name, fileSettings)
            }
            setupFieldType(context)
            fieldType().setFileSettings(
                    fileSettings,
                    updateInterval.original,
                    updateScatter?.original,
                    timeout?.original)
            return ExternalFileFieldMapper(
                    name, fieldType, defaultFieldType, context.indexSettings(),
                    multiFieldsBuilder.build(this, context), copyTo)
        }

        fun keyField(keyFieldName: String): Builder {
            fieldType().setKeyFieldName(keyFieldName)
            return this
        }

        fun updateInterval(updateInterval: TimeValueWithOriginal): Builder {
            this.updateInterval = updateInterval
            return this
        }

        fun updateInterval(): TimeValueWithOriginal? {
            return updateInterval
        }

        fun updateScatter(scatter: TimeValueOrPercent): Builder {
            this.updateScatter = scatter
            return this
        }

        fun valuesStoreType(valuesStoreType: ValuesStoreType): Builder {
            this.valuesStoreType = valuesStoreType
            return this
        }

        fun scalingFactor(factor: Long): Builder {
            this.scalingFactor = factor
            return this
        }

        fun url(url: String): Builder {
            this.url = url
            return this
        }

        fun timeout(timeout: TimeValueWithOriginal): Builder {
            this.timeout = timeout
            return this
        }
    }

    class ExternalFileFieldType : MappedFieldType {
        private var keyFieldName: String? = null
        private var fileSettings: FileSettings? = null
        private var originalUpdateInterval: Any? = null
        private var originalUpdateScatter: Any? = null
        private var originalTimeout: Any? = null

        constructor()
        private constructor(ref: ExternalFileFieldType) : super(ref) {
            this.keyFieldName = ref.keyFieldName
            this.fileSettings = ref.fileSettings
            this.originalUpdateInterval = ref.originalUpdateInterval
            this.originalTimeout = ref.originalTimeout
        }

        override fun clone(): ExternalFileFieldType {
            return ExternalFileFieldType(this)
        }

        override fun equals(other: Any?): Boolean {
            if (!super.equals(other)) {
                return false
            }
            if (other is ExternalFileFieldType) {
                return other.keyFieldName == keyFieldName &&
                        other.fileSettings == fileSettings
            }
            return false
        }

        override fun hashCode(): Int {
            return Objects.hash(super.hashCode(), keyFieldName, fileSettings)
        }

        fun keyFieldName(): String? {
            return keyFieldName
        }

        fun setKeyFieldName(keyFieldName: String) {
            this.keyFieldName = keyFieldName
        }

        fun fileSettings(): FileSettings? {
            return fileSettings
        }

        fun setFileSettings(
                fileSettings: FileSettings,
                originalUpdateInterval: Any,
                originalUpdateScatter: Any?,
                originalTimeout: Any?) {
            this.fileSettings = fileSettings
            this.originalUpdateInterval = originalUpdateInterval
            this.originalUpdateScatter = originalUpdateScatter
            this.originalTimeout = originalTimeout
        }

        fun originalUpdateInterval(): Any? {
            return originalUpdateInterval
        }

        fun originalUpdateScatter(): Any? {
            return originalUpdateScatter
        }

        fun originalTimeout(): Any? {
            return originalTimeout
        }

        override fun typeName(): String {
            return CONTENT_TYPE
        }

        override fun termQuery(value: Any, context: QueryShardContext): Query {
            throw QueryShardException(context, "ExternalFile fields are not searcheable")
        }

        override fun fielddataBuilder(): IndexFieldData.Builder {
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
                val values = ExternalFileService.instance.getValues(indexSettings.index, name())
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

        companion object {
            private const val DEFAULT_VALUE = 0.0
        }

        class AtomicUidKeyFieldData(
                private val values: FileValues,
                private val keyFieldData: AtomicFieldData
        ) : AtomicNumericFieldData {

            class Values(
                    private val values: FileValues,
                    private val uids: SortedBinaryDocValues
            ) : SortedNumericDoubleValues() {

                private var doc = -1
                private var value = DEFAULT_VALUE

                override fun advanceExact(target: Int): Boolean {
                    doc = target
                    return if (uids.advanceExact(doc)) {
                        val uid = uids.nextValue().utf8ToString()
                        val key = try {
                            uid.toLong()
                        } catch (e: NumberFormatException) {
                            // Possibly it is indice created in 5.x
                            try {
                                Uid.createUid(uid).id().toLong()
                            } catch (e: NumberFormatException) {
                                return false
                            }
                        }
                        if (values.contains(key)) {
                            value = values.get(key, DEFAULT_VALUE)
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

                private var value = DEFAULT_VALUE

                override fun advanceExact(target: Int): Boolean {
                    return if (keys.advanceExact(target)) {
                        val key = keys.nextValue()
                        if (values.contains(key)) {
                            value = values.get(key, DEFAULT_VALUE)
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
                nested: IndexFieldData.XFieldComparatorSource.Nested?, reverse: Boolean): SortField
        {
            val source = DoubleValuesComparatorSource(this, missingValue, sortMode, nested)
            return SortField(fieldName, source, reverse)
        }

        override fun clear() {}
    }
}
