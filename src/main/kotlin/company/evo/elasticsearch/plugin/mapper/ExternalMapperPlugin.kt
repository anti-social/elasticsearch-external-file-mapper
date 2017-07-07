package company.evo.elasticsearch.plugin.mapper

import java.util.Collections

import org.elasticsearch.index.mapper.Mapper
import org.elasticsearch.plugins.MapperPlugin
import org.elasticsearch.plugins.Plugin


class ExternalMapperPlugin : Plugin(), MapperPlugin {

    override fun getMappers() : Map<String, Mapper.TypeParser> {
        return Collections.emptyMap()
    }
}
