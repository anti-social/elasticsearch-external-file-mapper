package company.evo.elasticsearch.indices

import company.evo.persistent.hashmap.Hasher_Int
import company.evo.persistent.hashmap.HasherProvider_Int
import company.evo.persistent.hashmap.straight.StraightHashMapEnv
import company.evo.persistent.hashmap.straight.StraightHashMapROEnv
import company.evo.persistent.hashmap.straight.StraightHashMap_Int_Float
import company.evo.persistent.hashmap.straight.StraightHashMapRO_Int_Float
import company.evo.persistent.hashmap.straight.StraightHashMapType_Int_Float

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicReference

interface FileValues {
    interface Provider : AutoCloseable {
        val sizeBytes: Long
        fun getValues(): FileValues
    }
    fun get(key: Long, defaultValue: Double): Double
    fun contains(key: Long): Boolean
}

object EmptyFileValues : FileValues {
    override fun get(key: Long, defaultValue: Double): Double {
        return defaultValue
    }

    override fun contains(key: Long): Boolean {
        return false
    }
}

typealias StraightHashMapROEnv_Int_Float = StraightHashMapROEnv<
        HasherProvider_Int, Hasher_Int, StraightHashMap_Int_Float, StraightHashMapRO_Int_Float>

class IntDoubleFileValues(
        private val map: StraightHashMapRO_Int_Float
) : FileValues {

    class Provider(private val dir: Path) : FileValues.Provider {
        private val mapEnv: AtomicReference<StraightHashMapROEnv_Int_Float?> = AtomicReference(null)

        override val sizeBytes: Long
            get() = TODO("not implemented")

        override fun getValues(): FileValues {
            var env = mapEnv.get()
            if (env == null) {
                try {
                    val newEnv = StraightHashMapEnv.Builder(StraightHashMapType_Int_Float)
                            .useUnmapHack(true)
                            .openReadOnly(dir)
                    if (!mapEnv.compareAndSet(null, newEnv)) {
                        // Another thread already have set an environment
                        newEnv.close()
                    }
                    env = mapEnv.get()!!
                } catch (e: company.evo.persistent.FileDoesNotExistException) {
                    return EmptyFileValues
                }
            }
            return IntDoubleFileValues(env.getCurrentMap())
        }

        override fun close() {
            mapEnv.get()?.close()
        }
    }

    override fun get(key: Long, defaultValue: Double): Double {
        if (key > Int.MAX_VALUE) {
            return defaultValue
        }
        val v = map.get(key.toInt(), Float.NaN)
        if (v.isNaN()) {
            return defaultValue
        }
        return v.toDouble()
    }

    override fun contains(key: Long): Boolean {
        return map.contains(key.toInt())
    }
}

// class MemoryLongDoubleFileValues(
//         private val values: TLongDoubleHashMap
// ) : FileValues {
//
//     class Provider : FileValues.Provider {
//         private val map: TLongDoubleHashMap
//         override val sizeBytes: Long
//         override val lastModified: FileTime
//
//         constructor(keys: LongArray, values: DoubleArray, lastModified: FileTime) {
//             this.map = TLongDoubleHashMap(
//                     (keys.size / MAP_LOAD_FACTOR).toInt(), MAP_LOAD_FACTOR, -1, Double.NaN)
//             for ((ix, k) in keys.withIndex()) {
//                 this.map.put(k, values[ix])
//             }
//             this.sizeBytes = map.capacity() * (8L + 8L)
//             this.lastModified = lastModified
//         }
//
//         override fun get(): FileValues {
//             return MemoryLongDoubleFileValues(map)
//         }
//     }
//
//     override fun get(key: Long, defaultValue: Double): Double {
//         val v = values.get(key)
//         if (v.isNaN()) {
//             return defaultValue
//         }
//         return v
//     }
//
//     override fun contains(key: Long): Boolean {
//         return values.containsKey(key)
//     }
// }
//
// class MemoryLongIntFileValues(
//         private val values: TLongIntHashMap,
//         private val baseValue: Long,
//         private val inversedScalingFactor: Double
// ) : FileValues {
//
//     companion object {
//         const val NO_KEY: Long = -1
//         const val NO_VALUE: Int = -1
//     }
//
//     class Provider : FileValues.Provider {
//         private val map: TLongIntHashMap
//         private val baseValue: Long
//         private val scalingFactor: Long
//         override val sizeBytes: Long
//         override val lastModified: FileTime
//
//         constructor(keys: LongArray, values: DoubleArray,
//                     baseValue: Long, scalingFactor: Long, lastModified: FileTime) {
//             this.map = TLongIntHashMap(
//                     (keys.size / MAP_LOAD_FACTOR).toInt(), MAP_LOAD_FACTOR, NO_KEY, NO_VALUE)
//             for ((ix, k) in keys.withIndex()) {
//                 this.map.put(k, (values[ix] * scalingFactor - baseValue).toInt())
//             }
//             this.sizeBytes = map.capacity() * (8L + 4L)
//             this.baseValue = baseValue
//             this.scalingFactor = scalingFactor
//             this.lastModified = lastModified
//         }
//
//         override fun get(): FileValues {
//             return MemoryLongIntFileValues(map, baseValue, 1.0 / scalingFactor)
//         }
//     }
//
//     override fun get(key: Long, defaultValue: Double): Double {
//         if (key > Int.MAX_VALUE) {
//             return defaultValue
//         }
//         val v = values.get(key)
//         if (v == NO_VALUE) {
//             return defaultValue
//         }
//         return (baseValue + v) * inversedScalingFactor
//     }
//
//     override fun contains(key: Long): Boolean {
//         if (key > Int.MAX_VALUE) {
//             return false
//         }
//         return values.containsKey(key)
//     }
// }
//
// class MemoryLongShortFileValues(
//         private val values: TLongShortHashMap,
//         private val baseValue: Long,
//         private val inversedScalingFactor: Double
// ) : FileValues {
//
//     companion object {
//         const val NO_KEY: Long = -1
//         const val NO_VALUE: Short = -1
//     }
//
//     class Provider : FileValues.Provider {
//         private val map: TLongShortHashMap
//         private val baseValue: Long
//         private val scalingFactor: Long
//         override val sizeBytes: Long
//         override val lastModified: FileTime
//
//         constructor(keys: LongArray, values: DoubleArray,
//                     baseValue: Long, scalingFactor: Long, lastModified: FileTime) {
//             this.map = TLongShortHashMap(
//                     (keys.size / MAP_LOAD_FACTOR).toInt(), MAP_LOAD_FACTOR, NO_KEY, NO_VALUE)
//             for ((ix, k) in keys.withIndex()) {
//                 this.map.put(k, (values[ix] * scalingFactor - baseValue).toShort())
//             }
//             this.sizeBytes = map.capacity() * (8L + 2L)
//             this.baseValue = baseValue
//             this.scalingFactor = scalingFactor
//             this.lastModified = lastModified
//         }
//
//         override fun get(): FileValues {
//             return MemoryLongShortFileValues(map, baseValue, 1.0 / scalingFactor)
//         }
//     }
//
//     override fun get(key: Long, defaultValue: Double): Double {
//         if (key > Int.MAX_VALUE) {
//             return defaultValue
//         }
//         val v = values.get(key)
//         if (v == NO_VALUE) {
//             return defaultValue
//         }
//         return (baseValue + v) * inversedScalingFactor
//     }
//
//     override fun contains(key: Long): Boolean {
//         if (key > Int.MAX_VALUE) {
//             return false
//         }
//         return values.containsKey(key)
//     }
// }
//
// class MemoryIntDoubleFileValues(
//         private val values: TIntDoubleHashMap
// ) : FileValues {
//
//     class Provider : FileValues.Provider {
//         private val map: TIntDoubleHashMap
//         override val sizeBytes: Long
//         override val lastModified: FileTime
//
//         constructor(keys: LongArray, values: DoubleArray, lastModified: FileTime) {
//             this.map = TIntDoubleHashMap(
//                     (keys.size / MAP_LOAD_FACTOR).toInt(), MAP_LOAD_FACTOR, -1, Double.NaN)
//             for ((ix, k) in keys.withIndex()) {
//                 map.put(k.toInt(), values[ix])
//             }
//             this.sizeBytes = map.capacity() * (4L + 8L)
//             this.lastModified = lastModified
//         }
//
//         override fun get(): FileValues {
//             return MemoryIntDoubleFileValues(map)
//         }
//     }
//
//     override fun get(key: Long, defaultValue: Double): Double {
//         if (key > Int.MAX_VALUE) {
//             return defaultValue
//         }
//         val v = values.get(key.toInt())
//         if (v.isNaN()) {
//             return defaultValue
//         }
//         return v
//     }
//
//     override fun contains(key: Long): Boolean {
//         if (key > Int.MAX_VALUE) {
//             return false
//         }
//         return values.containsKey(key.toInt())
//     }
// }
//
// class MemoryIntIntFileValues(
//         private val values: TIntIntHashMap,
//         private val baseValue: Long,
//         private val inversedScalingFactor: Double
// ) : FileValues {
//
//     companion object {
//         const val NO_KEY: Int = -1
//         const val NO_VALUE: Int = -1
//     }
//
//     class Provider : FileValues.Provider {
//         private val map: TIntIntHashMap
//         private val baseValue: Long
//         private val scalingFactor: Long
//         override val sizeBytes: Long
//         override val lastModified: FileTime
//
//         constructor(keys: LongArray, values: DoubleArray,
//                     baseValue: Long, scalingFactor: Long, lastModified: FileTime) {
//             this.map = TIntIntHashMap(
//                     (keys.size / MAP_LOAD_FACTOR).toInt(), MAP_LOAD_FACTOR, NO_KEY, NO_VALUE)
//             for ((ix, k) in keys.withIndex()) {
//                 this.map.put(k.toInt(), (values[ix] * scalingFactor - baseValue).toInt())
//             }
//             this.sizeBytes = map.capacity() * (4L + 4L)
//             this.baseValue = baseValue
//             this.scalingFactor = scalingFactor
//             this.lastModified = lastModified
//         }
//
//         override fun get(): FileValues {
//             return MemoryIntIntFileValues(map, baseValue, 1.0 / scalingFactor)
//         }
//     }
//
//     override fun get(key: Long, defaultValue: Double): Double {
//         if (key > Int.MAX_VALUE) {
//             return defaultValue
//         }
//         val v = values.get(key.toInt())
//         if (v == NO_VALUE) {
//             return defaultValue
//         }
//         return (baseValue + v) * inversedScalingFactor
//     }
//
//     override fun contains(key: Long): Boolean {
//         if (key > Int.MAX_VALUE) {
//             return false
//         }
//         return values.containsKey(key.toInt())
//     }
// }
//
// class MemoryIntShortFileValues(
//         private val values: TIntShortHashMap,
//         private val baseValue: Long,
//         private val inversedScalingFactor: Double
// ) : FileValues {
//
//     companion object {
//         const val NO_KEY: Int = -1
//         const val NO_VALUE: Short = -1
//     }
//
//     class Provider : FileValues.Provider {
//         private val map: TIntShortHashMap
//         private val baseValue: Long
//         private val scalingFactor: Long
//         override val sizeBytes: Long
//         override val lastModified: FileTime
//
//         constructor(keys: LongArray, values: DoubleArray,
//                     baseValue: Long, scalingFactor: Long, lastModified: FileTime) {
//             this.map = TIntShortHashMap(
//                     (keys.size / MAP_LOAD_FACTOR).toInt(), MAP_LOAD_FACTOR, NO_KEY, NO_VALUE)
//             for ((ix, k) in keys.withIndex()) {
//                 this.map.put(k.toInt(), (values[ix] * scalingFactor - baseValue).toShort())
//             }
//             this.sizeBytes = map.capacity() * (4L + 2L)
//             this.baseValue = baseValue
//             this.scalingFactor = scalingFactor
//             this.lastModified = lastModified
//         }
//
//         override fun get(): FileValues {
//             return MemoryIntShortFileValues(map, baseValue, 1.0 / scalingFactor)
//         }
//     }
//
//     override fun get(key: Long, defaultValue: Double): Double {
//         if (key > Int.MAX_VALUE) {
//             return defaultValue
//         }
//         val v = values.get(key.toInt())
//         if (v == NO_VALUE) {
//             return defaultValue
//         }
//         return (baseValue + v) * inversedScalingFactor
//     }
//
//     override fun contains(key: Long): Boolean {
//         if (key > Int.MAX_VALUE) {
//             return false
//         }
//         return values.containsKey(key.toInt())
//     }
// }
