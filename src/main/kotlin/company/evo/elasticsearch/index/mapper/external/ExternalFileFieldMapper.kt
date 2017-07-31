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
import org.apache.lucene.search.Query
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.index.mapper.FieldMapper
import org.elasticsearch.index.mapper.MappedFieldType
import org.elasticsearch.index.mapper.Mapper
import org.elasticsearch.index.mapper.ParseContext
import org.elasticsearch.index.query.QueryShardContext
import org.elasticsearch.index.query.QueryShardException


public class ExternalFileFieldMapper(
        simpleName: String,
        fieldType: MappedFieldType,
        defaultFieldType: MappedFieldType,
        indexSettings: Settings,
        multiFields: MultiFields,
        copyTo: CopyTo
) : FieldMapper(
        simpleName, fieldType, defaultFieldType,
        indexSettings, multiFields, copyTo) {

    companion object {
        const val CONTENT_TYPE: String = "external_file"
        val FIELD_TYPE: ExternalFileFieldType = ExternalFileFieldType()
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

    class Builder(name: String) :
        FieldMapper.Builder<Builder, ExternalFileFieldMapper>(name, FIELD_TYPE, FIELD_TYPE)
    {

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
            fieldType.setHasDocValues(true)
            defaultFieldType.setHasDocValues(true)
        }
    }
}
