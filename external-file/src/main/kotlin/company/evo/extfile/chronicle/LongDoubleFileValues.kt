package company.evo.extfile.chronicle

import java.nio.file.attribute.FileTime

import net.openhft.chronicle.map.ChronicleMap

import company.evo.extfile.FileValues
import company.evo.extfile.FileValues.Config
import company.evo.extfile.ScaledFileValues


const val ENTRIES_DEFAULT = 100_000_000L

fun create(config: Config, lastModified: FileTime): FileValues.Provider {
    return when(config.keyType to config.valueType) {
        Config.KeyType.LONG to Config.ValueType.DOUBLE -> LongDoubleFileValues::Provider
        Config.KeyType.INT to Config.ValueType.SHORT -> LongDoubleFileValues::Provider
        else -> LongDoubleFileValues::Provider
    }(config, lastModified)
}

class LongDoubleFileValues(
        private val values: ChronicleMap<Long, Double>
) : FileValues {

    private val valueRef = 0.0

    class Provider(
            config: Config,
            override val lastModified: FileTime
    ) : FileValues.Provider {

        private val entries = config.getLong("entries", ENTRIES_DEFAULT)
        private val map = ChronicleMap
                .of(Long::class.javaObjectType, Double::class.javaObjectType)
                .entries(entries)
                .create()
        override val sizeBytes: Long = 0L

        override val values
            get(): FileValues {
                return LongDoubleFileValues(map)
            }

        override fun put(key: Long, value: Double) {
            map.put(key, value)
        }

        override fun remove(key: Long) {
            map.remove(key)
        }

        override fun clone(): FileValues.Provider {
            TODO("not implemented")
        }
    }

    override fun get(key: Long, defaultValue: Double): Double {
        return values.getUsing(key, valueRef) ?: defaultValue
    }

    override fun contains(key: Long): Boolean {
        return values.containsKey(key)
    }
}

class IntShortFileValues(
        private val values: ChronicleMap<Int, Short>,
        baseValue: Long,
        invertedScalingFactor: Double
) : ScaledFileValues(baseValue, invertedScalingFactor) {

    private val valueRef: Short = 0

    class Provider(
            private val config: Config,
            override val lastModified: FileTime
    ) : ScaledFileValues.Provider(config) {

        private val entries = config.getLong("entries", ENTRIES_DEFAULT)

        private val map = ChronicleMap
                .of(Int::class.javaObjectType, Short::class.javaObjectType)
                .entries(entries)
                .create()
        override val sizeBytes: Long = 0L

        override val values
            get(): FileValues {
                return IntShortFileValues(map, baseValue, invertedScalingFactor)
            }

        override fun put(key: Long, value: Double) {
            map.put(key.toInt(), scaledShortValue(value))
        }

        override fun remove(key: Long) {
            map.remove(key.toInt())
        }

        override fun clone(): FileValues.Provider {
            TODO("not implemented")
        }
    }

    override fun get(key: Long, defaultValue: Double): Double {
        val v = values.getUsing(key.toInt(), valueRef)
        if (v != null) {
            restoreFromShort(v)
        }
        return defaultValue
    }

    override fun contains(key: Long): Boolean {
        return values.containsKey(key.toInt())
    }
}