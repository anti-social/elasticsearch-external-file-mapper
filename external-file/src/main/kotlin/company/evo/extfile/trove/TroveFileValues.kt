package company.evo.extfile.trove

import java.nio.file.attribute.FileTime

import gnu.trove.map.hash.*

import company.evo.extfile.FileValues
import company.evo.extfile.FileValues.Config
import company.evo.extfile.FileValues.Config.*
import company.evo.extfile.ScaledFileValues


const val MAP_LOAD_FACTOR = 0.75F
const val INITIAL_SIZE_DEFAULT = 10000


fun create(config: Config, lastModified: FileTime): FileValues.Provider {
    return when (config.keyType to config.valueType) {
        KeyType.LONG to ValueType.DOUBLE -> LongDoubleFileValues::Provider
        KeyType.LONG to ValueType.FLOAT -> LongFloatFileValues::Provider
        KeyType.LONG to ValueType.INT -> LongIntFileValues::Provider
        KeyType.LONG to ValueType.SHORT -> LongShortFileValues::Provider
        KeyType.INT to ValueType.DOUBLE -> IntDoubleFileValues::Provider
        KeyType.INT to ValueType.FLOAT -> IntFloatFileValues::Provider
        KeyType.INT to ValueType.INT -> IntIntFileValues::Provider
        KeyType.INT to ValueType.SHORT -> IntShortFileValues::Provider
        else -> LongDoubleFileValues::Provider
    }(config, lastModified)
}

class LongDoubleFileValues(
        private val values: TLongDoubleHashMap
) : FileValues {

    companion object {
        const val NO_KEY: Long = -1
        private val NO_VALUE: Double = Double.NaN
    }

    class Provider(
            private val config: Config,
            override val lastModified: FileTime
    ) : FileValues.Provider {

        private val initialSize = config.getInt("initial_size", INITIAL_SIZE_DEFAULT)
        private val map = TLongDoubleHashMap(
                (initialSize / MAP_LOAD_FACTOR).toInt(), MAP_LOAD_FACTOR, NO_KEY, NO_VALUE
        )
        override val sizeBytes = map.capacity() * (8L + 8L)
        override val values: FileValues
                get() = LongDoubleFileValues(map)

        override fun put(key: Long, value: Double) {
            map.put(key, value)
        }

        override fun remove(key: Long) {
            map.remove(key)
        }

        override fun clone(): FileValues.Provider {
            val provider = Provider(config, lastModified)
            val it = map.iterator()
            while (it.hasNext()) {
                it.advance()
                provider.map.put(it.key(), it.value())
            }
            return provider
        }
    }

    override fun get(key: Long, defaultValue: Double): Double {
        val v = values.get(key)
        if (v.isNaN()) {
            return defaultValue
        }
        return v
    }

    override fun contains(key: Long): Boolean {
        return values.containsKey(key)
    }
}

class LongFloatFileValues(
        private val values: TLongFloatHashMap
) : FileValues {

    companion object {
        const val NO_KEY = -1L
        private val NO_VALUE = Float.NaN
    }

    class Provider(
            private val config: Config,
            override val lastModified: FileTime
    ) : FileValues.Provider {

        private val initialSize = config.getInt("initial_size", INITIAL_SIZE_DEFAULT)
        private val map = TLongFloatHashMap(
                (initialSize / MAP_LOAD_FACTOR).toInt(), MAP_LOAD_FACTOR, NO_KEY, NO_VALUE
        )
        override val sizeBytes = map.capacity() * (8L + 8L)
        override val values: FileValues
            get() = LongFloatFileValues(map)

        override fun put(key: Long, value: Double) {
            map.put(key, value.toFloat())
        }

        override fun remove(key: Long) {
            map.remove(key)
        }

        override fun clone(): FileValues.Provider {
            val provider = Provider(config, lastModified)
            val it = map.iterator()
            while (it.hasNext()) {
                it.advance()
                provider.map.put(it.key(), it.value())
            }
            return provider
        }
    }

    override fun get(key: Long, defaultValue: Double): Double {
        val v = values.get(key)
        if (v.isNaN()) {
            return defaultValue
        }
        return v.toDouble()
    }

    override fun contains(key: Long): Boolean {
        return values.containsKey(key)
    }
}

class LongIntFileValues(
        private val values: TLongIntHashMap,
        private val baseValue: Long,
        private val invertedScalingFactor: Double
) : ScaledFileValues(baseValue, invertedScalingFactor) {

    companion object {
        const val NO_KEY: Long = -1
        const val NO_VALUE: Int = -1
    }

    class Provider(
            private val config: Config,
            override val lastModified: FileTime
    ) : ScaledFileValues.Provider(config) {

        private val initialSize = config.getInt("initial_size", INITIAL_SIZE_DEFAULT)
        private val map = TLongIntHashMap(
                (initialSize / MAP_LOAD_FACTOR).toInt(), MAP_LOAD_FACTOR, NO_KEY, NO_VALUE
        )
        override val sizeBytes = map.capacity() * (8L + 4L)
        override val values
            get(): FileValues {
                return LongIntFileValues(map, baseValue, invertedScalingFactor)
            }

        override fun put(key: Long, value: Double) {
            map.put(key, scaledIntValue(value))
        }

        override fun remove(key: Long) {
            map.remove(key)
        }

        override fun clone(): FileValues.Provider {
            val provider = Provider(config, lastModified)
            val it = map.iterator()
            while (it.hasNext()) {
                it.advance()
                provider.map.put(it.key(), it.value())
            }
            return provider
        }
    }

    override fun get(key: Long, defaultValue: Double): Double {
        val v = values.get(key)
        if (v == NO_VALUE) {
            return defaultValue
        }
        return restoreFromInt(v)
    }

    override fun contains(key: Long): Boolean {
        if (key > Int.MAX_VALUE) {
            return false
        }
        return values.containsKey(key)
    }
}

class LongShortFileValues(
        private val values: TLongShortHashMap,
        baseValue: Long,
        invertedScalingFactor: Double
) : ScaledFileValues(baseValue, invertedScalingFactor) {

    companion object {
        const val NO_KEY: Long = -1
        const val NO_VALUE: Short = -1
    }

    class Provider(
            private val config: Config,
            override val lastModified: FileTime
    ) : ScaledFileValues.Provider(config) {
        private val initialSize = config.getInt("initial_size", INITIAL_SIZE_DEFAULT)
        private val map = TLongShortHashMap(
                (initialSize / MAP_LOAD_FACTOR).toInt(), MAP_LOAD_FACTOR, NO_KEY, NO_VALUE
        )
        override val sizeBytes = map.capacity() * (8L + 2L)

        override val values
            get(): FileValues {
                return LongShortFileValues(map, baseValue, 1.0 / scalingFactor)
            }

        override fun put(key: Long, value: Double) {
            map.put(key, scaledShortValue(value))
        }

        override fun remove(key: Long) {
            map.remove(key)
        }

        override fun clone(): FileValues.Provider {
            val provider = Provider(config, lastModified)
            val it = map.iterator()
            while (it.hasNext()) {
                it.advance()
                provider.map.put(it.key(), it.value())
            }
            return provider
        }
    }

    override fun get(key: Long, defaultValue: Double): Double {
        val v = values.get(key)
        if (v == NO_VALUE) {
            return defaultValue
        }
        return restoreFromShort(v)
    }

    override fun contains(key: Long): Boolean {
        if (key > Int.MAX_VALUE) {
            return false
        }
        return values.containsKey(key)
    }
}

class IntDoubleFileValues(
        private val values: TIntDoubleHashMap
) : FileValues {

    companion object {
        val NO_KEY = -1
        val NO_VALUE = Double.NaN
    }

    class Provider(
            private val config: Config,
            override val lastModified: FileTime
    ) : FileValues.Provider {
        private val initialSize = config.getInt("initial_size", INITIAL_SIZE_DEFAULT)
        private val map = TIntDoubleHashMap(
                (initialSize / MAP_LOAD_FACTOR).toInt(), MAP_LOAD_FACTOR, NO_KEY, NO_VALUE
        )
        override val sizeBytes = map.capacity() * (4L + 8L)
        override val values
            get(): FileValues {
                return IntDoubleFileValues(map)
            }

        override fun put(key: Long, value: Double) {
            map.put(key.toInt(), value)
        }

        override fun remove(key: Long) {
            map.remove(key.toInt())
        }

        override fun clone(): FileValues.Provider {
            val provider = Provider(config, lastModified)
            val it = map.iterator()
            while (it.hasNext()) {
                it.advance()
                provider.map.put(it.key(), it.value())
            }
            return provider
        }
    }

    override fun get(key: Long, defaultValue: Double): Double {
        if (key > Int.MAX_VALUE) {
            return defaultValue
        }
        val v = values.get(key.toInt())
        if (v.isNaN()) {
            return defaultValue
        }
        return v
    }

    override fun contains(key: Long): Boolean {
        if (key > Int.MAX_VALUE) {
            return false
        }
        return values.containsKey(key.toInt())
    }
}
class IntFloatFileValues(
        private val values: TIntFloatHashMap
) : FileValues {

    companion object {
        val NO_KEY = -1
        val NO_VALUE = Float.NaN
    }

    class Provider(
            private val config: Config,
            override val lastModified: FileTime
    ) : FileValues.Provider {
        private val initialSize = config.getInt("initial_size", INITIAL_SIZE_DEFAULT)
        private val map = TIntFloatHashMap(
                (initialSize / MAP_LOAD_FACTOR).toInt(), MAP_LOAD_FACTOR, NO_KEY, NO_VALUE
        )
        override val sizeBytes = map.capacity() * (4L + 8L)
        override val values
            get(): FileValues {
                return IntFloatFileValues(map)
            }

        override fun put(key: Long, value: Double) {
            map.put(key.toInt(), value.toFloat())
        }

        override fun remove(key: Long) {
            map.remove(key.toInt())
        }

        override fun clone(): FileValues.Provider {
            val provider = Provider(config, lastModified)
            val it = map.iterator()
            while (it.hasNext()) {
                it.advance()
                provider.map.put(it.key(), it.value())
            }
            return provider
        }
    }

    override fun get(key: Long, defaultValue: Double): Double {
        if (key > Int.MAX_VALUE) {
            return defaultValue
        }
        val v = values.get(key.toInt())
        if (v.isNaN()) {
            return defaultValue
        }
        return v.toDouble()
    }

    override fun contains(key: Long): Boolean {
        if (key > Int.MAX_VALUE) {
            return false
        }
        return values.containsKey(key.toInt())
    }
}

class IntIntFileValues(
        private val values: TIntIntHashMap,
        private val baseValue: Long,
        private val inversedScalingFactor: Double
) : FileValues {

    companion object {
        const val NO_KEY: Int = -1
        const val NO_VALUE: Int = -1
    }

    class Provider(
            private val config: Config,
            override val lastModified: FileTime
    ) : FileValues.Provider {
        private val initialSize = config.getInt("initial_size", INITIAL_SIZE_DEFAULT)
        private val baseValue = config.getLong("base_value", 0L)
        private val scalingFactor = config.getLong("scaling_factor")
        private val map = TIntIntHashMap(
                (initialSize / MAP_LOAD_FACTOR).toInt(), MAP_LOAD_FACTOR, NO_KEY, NO_VALUE
        )
        override val sizeBytes = map.capacity() * (4L + 4L)
        override val values
            get(): FileValues {
                return IntIntFileValues(map, baseValue, 1.0 / scalingFactor)
            }

        override fun put(key: Long, value: Double) {
            map.put(key.toInt(), (value * scalingFactor - baseValue).toInt())
        }

        override fun remove(key: Long) {
            map.remove(key.toInt())
        }

        override fun clone(): FileValues.Provider {
            val provider = Provider(config, lastModified)
            val it = map.iterator()
            while (it.hasNext()) {
                it.advance()
                provider.map.put(it.key(), it.value())
            }
            return provider
        }
    }

    override fun get(key: Long, defaultValue: Double): Double {
        if (key > Int.MAX_VALUE) {
            return defaultValue
        }
        val v = values.get(key.toInt())
        if (v == NO_VALUE) {
            return defaultValue
        }
        return (baseValue + v) * inversedScalingFactor
    }

    override fun contains(key: Long): Boolean {
        if (key > Int.MAX_VALUE) {
            return false
        }
        return values.containsKey(key.toInt())
    }
}

class IntShortFileValues(
        private val values: TIntShortHashMap,
        private val baseValue: Long,
        private val inversedScalingFactor: Double
) : FileValues {

    companion object {
        const val NO_KEY: Int = -1
        const val NO_VALUE: Short = -1
    }

    class Provider(
            private val config: Config,
            override val lastModified: FileTime
    ) : FileValues.Provider {
        private val initialSize = config.getInt("initial_size", INITIAL_SIZE_DEFAULT)
        private val baseValue = config.getLong("base_value", 0L)
        private val scalingFactor = config.getLong("scaling_factor")
        private val map = TIntShortHashMap(
                (initialSize / MAP_LOAD_FACTOR).toInt(), MAP_LOAD_FACTOR, NO_KEY, NO_VALUE
        )
        override val sizeBytes = map.capacity() * (4L + 4L)
        override val values
            get(): FileValues {
                return IntShortFileValues(map, baseValue, 1.0 / scalingFactor)
            }

        override fun put(key: Long, value: Double) {
            map.put(key.toInt(), (value * scalingFactor - baseValue).toShort())
        }

        override fun remove(key: Long) {
            map.remove(key.toInt())
        }

        override fun clone(): FileValues.Provider {
            val provider = Provider(config, lastModified)
            val it = map.iterator()
            while (it.hasNext()) {
                it.advance()
                provider.map.put(it.key(), it.value())
            }
            return provider
        }
    }

    override fun get(key: Long, defaultValue: Double): Double {
        if (key > Int.MAX_VALUE) {
            return defaultValue
        }
        val v = values.get(key.toInt())
        if (v == NO_VALUE) {
            return defaultValue
        }
        return (baseValue + v) * inversedScalingFactor
    }

    override fun contains(key: Long): Boolean {
        if (key > Int.MAX_VALUE) {
            return false
        }
        return values.containsKey(key.toInt())
    }
}
