package company.evo.extfile.robinhood

import java.nio.ByteBuffer
import java.nio.ByteOrder
import kotlin.math.abs


class RobinHoodHashtable(val keyType: KeyType, val valueType: ValueType, val capacity: Int) {
    enum class KeyType(val size: Int, val missing: Long) {
        LONG(8, Long.MIN_VALUE), INT(4, Int.MIN_VALUE.toLong());

        fun offset(ix: Int): Int = size * ix
    }
    enum class ValueType(val size: Int) {
        DOUBLE(8), FLOAT(4), INT(4), SHORT(2);

        fun offset(ix: Int): Int = size * ix
    }

    private val keyBufferCapacity = capacity * keyType.size
    private val valueBufferCapacity = capacity * valueType.size
    private val keys = ByteBuffer.allocateDirect(keyBufferCapacity)
            .order(ByteOrder.LITTLE_ENDIAN)
    private val values = ByteBuffer.allocateDirect(valueBufferCapacity)
            .order(ByteOrder.LITTLE_ENDIAN)

    @Volatile
    private var touch = 0

    init {
        when (keyType) {
            KeyType.LONG -> (0 until capacity).forEach { ix ->
                putKey(ix, Long.MIN_VALUE)
            }
            KeyType.INT -> (0 until capacity).forEach { ix ->
                putKey(ix, Int.MIN_VALUE.toLong())
            }
        }
    }

    private fun putKey(ix: Int, key: Long) = when(keyType) {
        KeyType.LONG -> keys.putLong(keyType.offset(ix), key)
        KeyType.INT -> keys.putInt(keyType.offset(ix), key.toInt())
    }

    private fun getKey(ix: Int): Long = when(keyType) {
        KeyType.LONG -> keys.getLong(keyType.offset(ix))
        KeyType.INT -> keys.getInt(keyType.offset(ix)).toLong()
    }

    private fun putValue(ix: Int, value: Double) = when(valueType) {
        ValueType.DOUBLE -> values.putDouble(valueType.offset(ix), value)
        ValueType.FLOAT -> values.putFloat(valueType.offset(ix), value.toFloat())
        ValueType.INT -> values.putInt(valueType.offset(ix), value.toInt())
        ValueType.SHORT -> values.putShort(valueType.offset(ix), value.toShort())
    }

    private fun getValue(ix: Int): Double = when(valueType) {
        ValueType.DOUBLE -> values.getDouble(valueType.offset(ix))
        ValueType.FLOAT -> values.getFloat(valueType.offset(ix)).toDouble()
        ValueType.INT -> values.getInt(valueType.offset(ix)).toDouble()
        ValueType.SHORT -> values.getShort(valueType.offset(ix)).toDouble()
    }

    private fun nextIx(ix: Int): Int {
        var nextIx = ix + 1
        if (nextIx >= capacity - 1) {
            nextIx = 0
        }
        return nextIx
    }

    fun put(key: Long, value: Double) {
        var ix = abs(key % capacity).toInt()
        do {
            val k = getKey(ix)
            ix = nextIx(ix)
        } while (k != keyType.missing)
        putValue(ix, value)
        putKey(ix, key)
    }

    fun get(key: Long, defaultValue: Double): Double {
//        if (touch != 0)
//            return defaultValue
        var ix = abs(key % capacity).toInt()
        while (true) {
            val k = getKey(ix)
            if (k == keyType.missing) {
                return defaultValue
            } else if (k == key) {
                return getValue(ix)
            }
            ix = nextIx(ix)
        }
    }
}

class LongDoubleFileValues(

)