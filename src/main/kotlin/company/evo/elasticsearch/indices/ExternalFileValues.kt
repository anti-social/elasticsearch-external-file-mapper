package company.evo.elasticsearch.indices

import company.evo.persistent.hashmap.straight.StraightHashMapEnv
import company.evo.persistent.hashmap.straight.StraightHashMapROEnv
import company.evo.persistent.hashmap.straight.StraightHashMapRO_Int_Float
import company.evo.persistent.hashmap.straight.StraightHashMapRO_Long_Float
import company.evo.persistent.hashmap.straight.StraightHashMapType_Int_Float
import company.evo.persistent.hashmap.straight.StraightHashMapType_Long_Float
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

enum class ExternalFieldKeyType {
    INT, LONG
}

interface ExternalFileValues {
    data class Provider(
        val dir: Path,
        val sharding: Boolean,
        val numShards: Int
    ) : AutoCloseable {
        private val mapEnvs: Array<AtomicReference<StraightHashMapROEnv<*, *, *, *>?>> = Array(numShards) {
            AtomicReference<StraightHashMapROEnv<*, *, *, *>?>(null)
        }

        fun getValues(keyType: ExternalFieldKeyType, shardId: Int?): ExternalFileValues {
            val mapEnv = mapEnvs[shardId ?: 0]
            var env = mapEnv.get()
            if (env == null) {
                try {
                    val mapDir = if (shardId != null) {
                        dir.resolve(shardId.toString())
                    } else {
                        dir
                    }
                    val mapEnvBuilder = when(keyType) {
                        ExternalFieldKeyType.INT -> StraightHashMapEnv.Builder(StraightHashMapType_Int_Float)
                        ExternalFieldKeyType.LONG -> StraightHashMapEnv.Builder(StraightHashMapType_Long_Float)
                    }
                    val newEnv = mapEnvBuilder
                        .useUnmapHack(true)
                        .openReadOnly(mapDir)
                    if (!mapEnv.compareAndSet(null, newEnv)) {
                        // Another thread already has set an environment
                        newEnv.close()
                    }
                    env = mapEnv.get()!!
                } catch (e: company.evo.persistent.FileDoesNotExistException) {
                    return EmptyFileValues
                }
            }
            return when(keyType) {
                ExternalFieldKeyType.INT -> IntFloatFileValues(env.getCurrentMap() as StraightHashMapRO_Int_Float)
                ExternalFieldKeyType.LONG -> LongFloatFileValues(env.getCurrentMap() as StraightHashMapRO_Long_Float)
            }
        }

        override fun close() {
            mapEnvs.forEach { env ->
                env.get()?.close()
            }
        }
    }

    fun get(key: Long, defaultValue: Double): Double
    fun contains(key: Long): Boolean
}

object EmptyFileValues : ExternalFileValues {
    override fun get(key: Long, defaultValue: Double): Double {
        return defaultValue
    }

    override fun contains(key: Long): Boolean {
        return false
    }
}

class LongFloatFileValues(
    private val map: StraightHashMapRO_Long_Float
) : ExternalFileValues {
    override fun get(key: Long, defaultValue: Double): Double {
        val v = map.get(key, Float.NaN)
        if (v.isNaN()) {
            return defaultValue
        }
        return v.toDouble()
    }

    override fun contains(key: Long): Boolean {
        return map.contains(key)
    }
}

class IntFloatFileValues(
    private val map: StraightHashMapRO_Int_Float
) : ExternalFileValues {
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
