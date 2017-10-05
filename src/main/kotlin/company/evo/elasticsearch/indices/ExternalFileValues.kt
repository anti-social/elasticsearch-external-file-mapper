package company.evo.elasticsearch.indices

import java.nio.ByteBuffer
import java.nio.file.attribute.FileTime

import org.mapdb.BTreeMap
import org.mapdb.DB
import org.mapdb.DBMaker
import org.mapdb.HTreeMap
import org.mapdb.Serializer

import net.uaprom.htable.HashTable
import net.uaprom.htable.TrieHashTable


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

class LongDoubleFileValues(
        private val values: BTreeMap<Long, Double>
) : FileValues {

    class Provider : FileValues.Provider {
        private val map: BTreeMap<Long, Double>
        override val sizeBytes: Long
        override val lastModified: FileTime

        constructor(mapMaker: DB.TreeMapMaker<Long, Double>, keys: LongArray, values: DoubleArray, lastModified: FileTime) {
            this.map = mapMaker
                    .counterEnable()
                    .create()

//                    (keys.size / MAP_LOAD_FACTOR).toInt(), MAP_LOAD_FACTOR, -1, Double.NaN)
            for ((ix, k) in keys.withIndex()) {
                this.map.put(k, values[ix])
            }
            this.sizeBytes = 0
            this.lastModified = lastModified
        }

        override fun get(): FileValues {
            return LongDoubleFileValues(map)
        }
    }

    override fun get(key: Long, defaultValue: Double): Double {
        val v = values.get(key)
        if (v == null) {
            return defaultValue
        }
        return v
    }

    override fun contains(key: Long): Boolean {
        return values.containsKey(key)
    }
}

class LongLongFileValues(
        private val values: BTreeMap<Long, Long>,
        private val baseValue: Long,
        private val inversedScalingFactor: Double
) : FileValues {

    class Provider : FileValues.Provider {
        private val map: BTreeMap<Long, Long>
        private val baseValue: Long
        private val scalingFactor: Long
        override val sizeBytes: Long
        override val lastModified: FileTime

        constructor(mapMaker: DB.TreeMapMaker<Long, Long>,
                    keys: LongArray, values: DoubleArray,
                    baseValue: Long, scalingFactor: Long,
                    lastModified: FileTime) {
            this.map = mapMaker
                    .counterEnable()
                    .keySerializer(Serializer.LONG)
                    .valueSerializer(Serializer.LONG)
                    .create()
            for ((ix, k) in keys.withIndex()) {
                val value = (values[ix] * scalingFactor - baseValue).toLong()
                this.map.put(k, value)
            }
            this.sizeBytes = 0
            this.baseValue = baseValue
            this.scalingFactor = scalingFactor
            this.lastModified = lastModified
        }

        override fun get(): FileValues {
            return LongLongFileValues(
                    map, baseValue, 1.0 / scalingFactor)
        }
    }

    override fun get(key: Long, defaultValue: Double): Double {
        val v = values.get(key)
        if (v == null) {
            return defaultValue
        }
        return (baseValue + v) * inversedScalingFactor
    }

    override fun contains(key: Long): Boolean {
        return values.containsKey(key)
    }
}

class MappedFileValues(
        private val values: HashTable.Reader
) : FileValues {

    class Provider(
            private val data: ByteBuffer,
            override val sizeBytes: Long,
            override val lastModified: FileTime
    ) : FileValues.Provider {

        override fun get(): FileValues {
            return MappedFileValues(TrieHashTable.Reader(data.slice()))
        }
    }

    override fun get(key: Long, defaultValue: Double): Double {
        return values.getDouble(key, defaultValue)
    }

    override fun contains(key: Long): Boolean {
        return values.exists(key)
    }
}
