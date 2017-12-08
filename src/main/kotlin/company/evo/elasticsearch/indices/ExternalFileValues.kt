package company.evo.elasticsearch.indices

import java.nio.file.attribute.FileTime

import net.openhft.chronicle.map.ChronicleMap


interface FileValues {
    interface Provider {
        val sizeBytes: Long
        val lastModified: FileTime
        fun get(): FileValues
    }
    fun get(key: Long, defaultValue: Double): Double
    fun contains(key: Long): Boolean
}

class EmptyFileValues : FileValues {
    override fun get(key: Long, defaultValue: Double): Double {
        return defaultValue
    }

    override fun contains(key: Long): Boolean {
        return false
    }
}

class LongDoubleFileValues private constructor(
        private val values: ChronicleMap<java.lang.Long, java.lang.Double>
) : FileValues {

    class Provider : FileValues.Provider {
        private val map: ChronicleMap<java.lang.Long, java.lang.Double>
        override val sizeBytes: Long
        override val lastModified: FileTime

        constructor(keys: LongArray, values: DoubleArray, lastModified: FileTime) {
            val mapBuiler = ChronicleMap
                    .of(java.lang.Long::class.java, java.lang.Double::class.java)
                    .entries(keys.size * 2L)
                    .createPersistedTo(tmpPath.toFile())
//                    (keys.size / MAP_LOAD_FACTOR).toInt(), MAP_LOAD_FACTOR, -1, Double.NaN)
            for ((ix, k) in keys.withIndex()) {
                this.map.put(k, values[ix])
            }
            this.sizeBytes = map.capacity() * (8L + 8L)
            this.lastModified = lastModified
        }

        override fun get(): FileValues {
            return MemoryLongDoubleFileValues(map)
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

class MemoryLongIntFileValues(
        private val values: TLongIntHashMap,
        private val baseValue: Long,
        private val inversedScalingFactor: Double
) : FileValues {

    companion object {
        const val NO_KEY: Long = -1
        const val NO_VALUE: Int = -1
    }

    class Provider : FileValues.Provider {
        private val map: TLongIntHashMap
        private val baseValue: Long
        private val scalingFactor: Long
        override val sizeBytes: Long
        override val lastModified: FileTime

        constructor(keys: LongArray, values: DoubleArray,
                    baseValue: Long, scalingFactor: Long, lastModified: FileTime) {
            this.map = TLongIntHashMap(
                    (keys.size / MAP_LOAD_FACTOR).toInt(), MAP_LOAD_FACTOR, NO_KEY, NO_VALUE)
            for ((ix, k) in keys.withIndex()) {
                this.map.put(k, (values[ix] * scalingFactor - baseValue).toInt())
            }
            this.sizeBytes = map.capacity() * (8L + 4L)
            this.baseValue = baseValue
            this.scalingFactor = scalingFactor
            this.lastModified = lastModified
        }

        override fun get(): FileValues {
            return MemoryLongIntFileValues(map, baseValue, 1.0 / scalingFactor)
        }
    }

    override fun get(key: Long, defaultValue: Double): Double {
        if (key > Int.MAX_VALUE) {
            return defaultValue
        }
        val v = values.get(key)
        if (v == NO_VALUE) {
            return defaultValue
        }
        return (baseValue + v) * inversedScalingFactor
    }

    override fun contains(key: Long): Boolean {
        if (key > Int.MAX_VALUE) {
            return false
        }
        return values.containsKey(key)
    }
}

class MemoryLongShortFileValues(
        private val values: TLongShortHashMap,
        private val baseValue: Long,
        private val inversedScalingFactor: Double
) : FileValues {

    companion object {
        const val NO_KEY: Long = -1
        const val NO_VALUE: Short = -1
    }

    class Provider : FileValues.Provider {
        private val map: TLongShortHashMap
        private val baseValue: Long
        private val scalingFactor: Long
        override val sizeBytes: Long
        override val lastModified: FileTime

        constructor(keys: LongArray, values: DoubleArray,
                    baseValue: Long, scalingFactor: Long, lastModified: FileTime) {
            this.map = TLongShortHashMap(
                    (keys.size / MAP_LOAD_FACTOR).toInt(), MAP_LOAD_FACTOR, NO_KEY, NO_VALUE)
            for ((ix, k) in keys.withIndex()) {
                this.map.put(k, (values[ix] * scalingFactor - baseValue).toShort())
            }
            this.sizeBytes = map.capacity() * (8L + 2L)
            this.baseValue = baseValue
            this.scalingFactor = scalingFactor
            this.lastModified = lastModified
        }

        override fun get(): FileValues {
            return MemoryLongShortFileValues(map, baseValue, 1.0 / scalingFactor)
        }
    }

    override fun get(key: Long, defaultValue: Double): Double {
        if (key > Int.MAX_VALUE) {
            return defaultValue
        }
        val v = values.get(key)
        if (v == NO_VALUE) {
            return defaultValue
        }
        return (baseValue + v) * inversedScalingFactor
    }

    override fun contains(key: Long): Boolean {
        if (key > Int.MAX_VALUE) {
            return false
        }
        return values.containsKey(key)
    }
}

class MemoryIntDoubleFileValues(
        private val values: TIntDoubleHashMap
) : FileValues {

    class Provider : FileValues.Provider {
        private val map: TIntDoubleHashMap
        override val sizeBytes: Long
        override val lastModified: FileTime

        constructor(keys: LongArray, values: DoubleArray, lastModified: FileTime) {
            this.map = TIntDoubleHashMap(
                    (keys.size / MAP_LOAD_FACTOR).toInt(), MAP_LOAD_FACTOR, -1, Double.NaN)
            for ((ix, k) in keys.withIndex()) {
                map.put(k.toInt(), values[ix])
            }
            this.sizeBytes = map.capacity() * (4L + 8L)
            this.lastModified = lastModified
        }

        override fun get(): FileValues {
            return MemoryIntDoubleFileValues(map)
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

class MemoryIntIntFileValues(
        private val values: TIntIntHashMap,
        private val baseValue: Long,
        private val inversedScalingFactor: Double
) : FileValues {

    companion object {
        const val NO_KEY: Int = -1
        const val NO_VALUE: Int = -1
    }

    class Provider : FileValues.Provider {
        private val map: TIntIntHashMap
        private val baseValue: Long
        private val scalingFactor: Long
        override val sizeBytes: Long
        override val lastModified: FileTime

        constructor(keys: LongArray, values: DoubleArray,
                    baseValue: Long, scalingFactor: Long, lastModified: FileTime) {
            this.map = TIntIntHashMap(
                    (keys.size / MAP_LOAD_FACTOR).toInt(), MAP_LOAD_FACTOR, NO_KEY, NO_VALUE)
            for ((ix, k) in keys.withIndex()) {
                this.map.put(k.toInt(), (values[ix] * scalingFactor - baseValue).toInt())
            }
            this.sizeBytes = map.capacity() * (4L + 4L)
            this.baseValue = baseValue
            this.scalingFactor = scalingFactor
            this.lastModified = lastModified
        }

        override fun get(): FileValues {
            return MemoryIntIntFileValues(map, baseValue, 1.0 / scalingFactor)
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

class MemoryIntShortFileValues(
        private val values: TIntShortHashMap,
        private val baseValue: Long,
        private val inversedScalingFactor: Double
) : FileValues {

    companion object {
        const val NO_KEY: Int = -1
        const val NO_VALUE: Short = -1
    }

    class Provider : FileValues.Provider {
        private val map: TIntShortHashMap
        private val baseValue: Long
        private val scalingFactor: Long
        override val sizeBytes: Long
        override val lastModified: FileTime

        constructor(keys: LongArray, values: DoubleArray,
                    baseValue: Long, scalingFactor: Long, lastModified: FileTime) {
            this.map = TIntShortHashMap(
                    (keys.size / MAP_LOAD_FACTOR).toInt(), MAP_LOAD_FACTOR, NO_KEY, NO_VALUE)
            for ((ix, k) in keys.withIndex()) {
                this.map.put(k.toInt(), (values[ix] * scalingFactor - baseValue).toShort())
            }
            this.sizeBytes = map.capacity() * (4L + 2L)
            this.baseValue = baseValue
            this.scalingFactor = scalingFactor
            this.lastModified = lastModified
        }

        override fun get(): FileValues {
            return MemoryIntShortFileValues(map, baseValue, 1.0 / scalingFactor)
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

class ChronicleFileValues(
        private val values: ChronicleMap<java.lang.Long, java.lang.Double>
) : FileValues {

    private val valueRef = java.lang.Double(0.0)

    class Provider(
            private val values: ChronicleMap<java.lang.Long, java.lang.Double>,
            override val sizeBytes: Long,
            override val lastModified: FileTime
    ) : FileValues.Provider {

        override fun get(): FileValues {
            return ChronicleFileValues(values)
        }
    }

    override fun get(key: Long, defaultValue: Double): Double {
        val res =  values.getUsing(java.lang.Long(key), valueRef)
        if (res == null) {
            return defaultValue
        }
        return res.toDouble()
    }

    override fun contains(key: Long): Boolean {
        return values.containsKey(java.lang.Long(key))
    }
}
