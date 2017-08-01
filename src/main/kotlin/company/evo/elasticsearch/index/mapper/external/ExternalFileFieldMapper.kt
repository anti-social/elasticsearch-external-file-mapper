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
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.index.Index
import org.elasticsearch.index.IndexSettings
import org.elasticsearch.index.fielddata.AtomicNumericFieldData
import org.elasticsearch.index.fielddata.IndexFieldData
import org.elasticsearch.index.fielddata.IndexFieldDataCache
import org.elasticsearch.index.fielddata.IndexNumericFieldData
import org.elasticsearch.index.fielddata.ScriptDocValues
import org.elasticsearch.index.fielddata.SortedBinaryDocValues
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues
import org.elasticsearch.index.mapper.FieldMapper
import org.elasticsearch.index.mapper.MappedFieldType
import org.elasticsearch.index.mapper.Mapper
import org.elasticsearch.index.mapper.MapperService
import org.elasticsearch.index.mapper.ParseContext
import org.elasticsearch.index.query.QueryShardContext
import org.elasticsearch.index.query.QueryShardException
import org.elasticsearch.indices.breaker.CircuitBreakerService
import org.elasticsearch.search.MultiValueMode


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
            FIELD_TYPE.freeze()
        }
    }

    override fun contentType() : String {
        return CONTENT_TYPE
    }

    override fun parseCreateField(
            context: ParseContext, fields: List<IndexableField>)
    {
    }

    class ExternalFileFieldType : MappedFieldType {
        constructor()
        constructor(ref: ExternalFileFieldType) : super(ref)

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
            return object : IndexFieldData.Builder {
                override fun build(
                        indexSettings: IndexSettings, fieldType: MappedFieldType,
                        cache: IndexFieldDataCache, breakerService: CircuitBreakerService,
                        mapperService: MapperService
                ): IndexNumericFieldData {
                    return ExternalFileFieldData(name(), indexSettings.getIndex())
                }
            }
        }
    }

    class ExternalFileFieldData : IndexNumericFieldData {

        private val fieldName: String
        private val index: Index

        constructor(fieldName: String, index: Index) {
            this.fieldName = fieldName
            this.index = index
        }

        class ExternalFileValues : SortedNumericDoubleValues() {
            override fun setDocument(doc: Int) {
            }

            override fun valueAt(doc: Int): Double {
                return 1.2
            }

            override fun count(): Int {
                return 1
            }
        }

        class Atomic : AtomicNumericFieldData {
            override fun getDoubleValues(): SortedNumericDoubleValues {
                return ExternalFileValues()
            }

            override fun getLongValues(): SortedNumericDocValues {
                throw UnsupportedOperationException("getLongValues: not implemented")
            }

            override fun getScriptValues(): ScriptDocValues.Doubles {
                return ScriptDocValues.Doubles(ExternalFileValues())
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

        override fun load(context: LeafReaderContext): Atomic {
            return Atomic()
        }

        override fun loadDirect(context: LeafReaderContext): Atomic {
            return load(context)
        }

        override fun sortField(
                missingValue: Any?, sortMode: MultiValueMode,
                nested: IndexFieldData.XFieldComparatorSource.Nested, reverse: Boolean): SortField
        {
            throw UnsupportedOperationException("sortField: not implemented")
        }

        override fun clear() {}
    }

    class TypeParser : Mapper.TypeParser {
        override fun parse(
                name: String,
                node: Map<String, Any>,
                parserContext: Mapper.TypeParser.ParserContext): Mapper.Builder<*,*>
        {
            return Builder(name)
        }
    }

    class Builder : FieldMapper.Builder<Builder, ExternalFileFieldMapper> {
        constructor(name: String) : super(name, FIELD_TYPE, FIELD_TYPE) {
            this.builder = this
        }

        override fun build(context: BuilderContext): ExternalFileFieldMapper {
            setupFieldType(context)
            return ExternalFileFieldMapper(
                    name, fieldType, defaultFieldType, context.indexSettings(),
                    multiFieldsBuilder.build(this, context), copyTo)
        }

        override fun setupFieldType(context: BuilderContext) {
            super.setupFieldType(context)
            fieldType.setIndexOptions(IndexOptions.NONE)
            defaultFieldType.setIndexOptions(IndexOptions.NONE)
            fieldType.setHasDocValues(false)
            defaultFieldType.setHasDocValues(false)
        }
    }
}
