package company.evo.extfile

import java.nio.file.attribute.FileTime
import java.util.*

import company.evo.extfile.chronicle.create as createChronicle
import company.evo.extfile.trove.create as createTrove


interface FileValues {
    companion object {
        fun create(backend: Backend, config: Config, lastModified: FileTime): Provider {
            return when (backend) {
                Backend.TROVE -> createTrove(config, lastModified)
                Backend.CHRONICLE -> createChronicle(config, lastModified)
            }
        }
    }

    enum class Backend {
        TROVE,
        CHRONICLE
    }

    class Config(
            val keyType: KeyType = KeyType.LONG,
            val valueType: ValueType = ValueType.DOUBLE
    ) {
        enum class KeyType {
            LONG,
            INT,
        }
        enum class ValueType {
            DOUBLE,
            FLOAT,
            INT,
            SHORT,
        }

        private val props = Properties()

        fun set(name: String, value: String) {
            props.setProperty(name, value)
        }

        fun getInt(name: String, defaultValue: Int = 0): Int {
            return props.getProperty(name)?.toInt() ?: defaultValue
        }

        fun getLong(name: String, defaultValue: Long = 0L): Long {
            return props.getProperty(name)?.toLong() ?: defaultValue
        }
    }

    interface Provider {
        val sizeBytes: Long
        val lastModified: FileTime
        val values: FileValues

        fun put(key: Long, value: Double)
        fun remove(key: Long)
        fun clone(): Provider

        fun applyDiff(diff: Map<Long, Double?>, lastModified: FileTime): Provider {
            val new = clone()
            diff.forEach { k, v ->
                if (v == null) {
                    new.remove(k)
                } else {
                    new.put(k, v)
                }
            }
            return new
        }
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

abstract class ScaledFileValues(
        private val baseValue: Long,
        private val invertedScalingFactor: Double
) : FileValues {

    abstract class Provider(
            config: FileValues.Config
    ) : FileValues.Provider {
        protected val baseValue = config.getLong("base_value", 0L)
        protected val scalingFactor = config.getLong("scaling_factor")
        protected val invertedScalingFactor = 1.0 / scalingFactor

        private fun scaledValue(value: Double): Double {
            return value * scalingFactor - baseValue
        }

        protected fun scaledIntValue(value: Double): Int =
                scaledValue(value).toInt()

        protected fun scaledShortValue(value: Double): Short =
            scaledValue(value).toShort()
    }

    protected fun restoreFromInt(scaledValue: Int): Double {
        return (baseValue + scaledValue) * invertedScalingFactor
    }

    protected fun restoreFromShort(scaledValue: Short): Double {
        return (baseValue + scaledValue) * invertedScalingFactor
    }
}
