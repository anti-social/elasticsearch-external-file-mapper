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


public class ExternalFileFieldMapper(
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

    override protected fun contentType() : String {
        return CONTENT_TYPE
    }

    override protected fun parseCreateField(
            context: ParseContext, fields: List<IndexableField>)
    {
    }

    class ExternalFileFieldType : MappedFieldType {
        constructor()
        constructor(ref: ExternalFileFieldType) : super(ref)

        override public fun typeName(): String {
            return CONTENT_TYPE
        }

        override public fun clone(): ExternalFileFieldType {
            return ExternalFileFieldType(this)
        }

        override public fun termQuery(value: Any, context: QueryShardContext): Query {
            throw QueryShardException(context, "ExternalFile fields are not searcheable")
        }

        override public fun fielddataBuilder(): IndexFieldData.Builder {
            return object : IndexFieldData.Builder {
                override public fun build(
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
            override public fun setDocument(doc: Int) {
            }

            override public fun valueAt(doc: Int): Double {
                return 1.2
            }

            override public fun count(): Int {
                return 1
            }
        }

        class Atomic : AtomicNumericFieldData {
            override public fun getDoubleValues(): SortedNumericDoubleValues {
                return ExternalFileValues()
            }

            override public fun getLongValues(): SortedNumericDocValues {
                throw UnsupportedOperationException("getLongValues: not implemented")
            }

            override public fun getScriptValues(): ScriptDocValues.Doubles {
                return ScriptDocValues.Doubles(ExternalFileValues())
            }

            override public fun getBytesValues(): SortedBinaryDocValues {
                throw UnsupportedOperationException("getBytesValues: not implemented")
            }

            override public fun ramBytesUsed(): Long {
                return 0
            }

            override public fun close() {}
        }

        override fun getNumericType(): IndexNumericFieldData.NumericType {
            return IndexNumericFieldData.NumericType.DOUBLE
        }

        override public fun index(): Index {
            return index
        }

        override public fun getFieldName(): String {
            return fieldName
        }

        override public fun load(context: LeafReaderContext): Atomic {
            return Atomic()
        }

        override public fun loadDirect(context: LeafReaderContext): Atomic {
            return load(context)
        }

        override public fun sortField(
                missingValue: Any?, sortMode: MultiValueMode,
                nested: IndexFieldData.XFieldComparatorSource.Nested, reverse: Boolean): SortField
        {
            throw UnsupportedOperationException("sortField: not implemented")
        }

        override public fun clear() {}
    }

    class TypeParser : Mapper.TypeParser {
        override public fun parse(
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

        override public fun build(context: BuilderContext): ExternalFileFieldMapper {
            setupFieldType(context)
            return ExternalFileFieldMapper(
                    name, fieldType, defaultFieldType, context.indexSettings(),
                    multiFieldsBuilder.build(this, context), copyTo)
        }

        override protected fun setupFieldType(context: BuilderContext) {
            super.setupFieldType(context)
            fieldType.setIndexOptions(IndexOptions.NONE)
            defaultFieldType.setIndexOptions(IndexOptions.NONE)
            fieldType.setHasDocValues(false)
            defaultFieldType.setHasDocValues(false)
        }
    }
}
