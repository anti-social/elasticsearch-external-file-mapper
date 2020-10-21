package company.evo.io

import java.nio.Buffer
import java.nio.ByteBuffer
import java.nio.MappedByteBuffer
import java.security.AccessController
import java.security.PrivilegedExceptionAction

import sun.misc.Unsafe

interface IOBuffer {
    fun getByteBuffer(): ByteBuffer?
    fun isDirect(): Boolean
    fun size(): Int

    fun drop()

    fun readBytes(ix: Int, dst: ByteArray)
    fun readBytes(ix: Int, dst: ByteArray, offset: Int, length: Int)
    fun readByte(ix: Int): Byte
    fun readShort(ix: Int): Short
    fun readInt(ix: Int): Int
    fun readLong(ix: Int): Long
    fun readFloat(ix: Int): Float
    fun readDouble(ix: Int): Double

    fun readByteVolatile(ix: Int): Byte
    fun readShortVolatile(ix: Int): Short
    fun readIntVolatile(ix: Int): Int
    fun readLongVolatile(ix: Int): Long
    fun readFloatVolatile(ix: Int): Float
    fun readDoubleVolatile(ix: Int): Double
}

interface MutableIOBuffer : IOBuffer {
    fun writeBytes(ix: Int, src: ByteArray)
    fun writeBytes(ix: Int, src: ByteArray, offset: Int, length: Int)
    fun writeByte(ix: Int, v: Byte)
    fun writeShort(ix: Int, v: Short)
    fun writeInt(ix: Int, v: Int)
    fun writeLong(ix: Int, v: Long)
    fun writeFloat(ix: Int, v: Float)
    fun writeDouble(ix: Int, v: Double)

    fun writeByteVolatile(ix: Int, v: Byte)
    fun writeShortVolatile(ix: Int, v: Short)
    fun writeIntVolatile(ix: Int, v: Int)
    fun writeLongVolatile(ix: Int, v: Long)
    fun writeFloatVolatile(ix: Int, v: Float)
    fun writeDoubleVolatile(ix: Int, v: Double)

    fun writeIntOrdered(ix: Int, v: Int)
    fun writeLongOrdered(ix: Int, v: Long)

    fun fsync()
}

open class UnsafeBuffer(protected val buffer: ByteBuffer) : IOBuffer {
    companion object {
        @JvmStatic
        protected val UNSAFE: Unsafe
        @JvmStatic
        protected val ARRAY_BASE_OFFSET: Long

        private val BYTE_BUFFER_ADDRESS_FIELD_OFFSET: Long
        private val BYTE_BUFFER_OFFSET_FIELD_OFFSET: Long
        private val BYTE_BUFFER_HB_FIELD_OFFSET: Long

        init {
            UNSAFE = AccessController.doPrivileged(PrivilegedExceptionAction {
                val theUnsafeField = Unsafe::class.java.getDeclaredField("theUnsafe")
                theUnsafeField.setAccessible(true)
                theUnsafeField.get(null) as Unsafe
            })
            ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(ByteArray::class.java).toLong()

            BYTE_BUFFER_ADDRESS_FIELD_OFFSET = AccessController.doPrivileged(PrivilegedExceptionAction {
                UNSAFE.objectFieldOffset(
                        Buffer::class.java.getDeclaredField("address")
                )
            })
            BYTE_BUFFER_OFFSET_FIELD_OFFSET = AccessController.doPrivileged(PrivilegedExceptionAction {
                UNSAFE.objectFieldOffset(
                        ByteBuffer::class.java.getDeclaredField("offset")
                )
            })
            BYTE_BUFFER_HB_FIELD_OFFSET = AccessController.doPrivileged(PrivilegedExceptionAction {
                UNSAFE.objectFieldOffset(
                        ByteBuffer::class.java.getDeclaredField("hb")
                )
            })
        }

        private fun getDirectArrayAddress(buffer: ByteBuffer): Long {
            return UNSAFE.getLong(buffer, BYTE_BUFFER_ADDRESS_FIELD_OFFSET)
        }

        private fun getArrayAddress(buffer: ByteBuffer): Long {
            return UNSAFE.getInt(buffer, BYTE_BUFFER_OFFSET_FIELD_OFFSET).toLong()
        }

        private fun getByteArray(buffer: ByteBuffer): ByteArray {
            return UNSAFE.getObject(buffer, BYTE_BUFFER_HB_FIELD_OFFSET) as ByteArray
        }
    }

    protected val byteArray: ByteArray?
    protected val arrayAddress: Long

    private var isDropped = false

    init {
        if (buffer.isDirect) {
            byteArray = null
            arrayAddress = getDirectArrayAddress(buffer)
        } else {
            byteArray = getByteArray(buffer)
            arrayAddress = ARRAY_BASE_OFFSET + getArrayAddress(buffer)
        }
    }

    protected fun checkNotDropped() {
        if (isDropped) {
            throw IllegalStateException("Buffer was dropped")
        }
    }

    protected fun checkLength(len: Int) {
        if (len < 0) {
            throw ArrayIndexOutOfBoundsException("Length $len should not be < 0")
        }
    }
    protected fun checkBounds(buffer: ByteBuffer, ix: Int, len: Int) {
        checkBounds(buffer.capacity(), ix, len)
    }

    protected fun checkBounds(buffer: ByteArray, ix: Int, len: Int) {
        checkBounds(buffer.size, ix, len)
    }

    private fun checkBounds(capacity: Int, ix: Int, len: Int) {
        if (ix < 0 || ix + len > capacity) {
            throw ArrayIndexOutOfBoundsException("buffer capacity: $capacity, index: $ix, length: $len")
        }
    }

    override fun getByteBuffer(): ByteBuffer? {
        return buffer
    }

    override fun isDirect(): Boolean {
        return buffer.isDirect
    }

    override fun size(): Int {
        return buffer.capacity()
    }

    override fun drop() {
        if (BufferCleaner.BUFFER_CLEANER == null) {
            return
        }
        if (!buffer.isDirect) {
            return
        }
        BufferCleaner.BUFFER_CLEANER.clean(buffer)
        isDropped = true
    }

    override fun readBytes(ix: Int, dst: ByteArray) {
        checkBounds(buffer, ix, dst.size)
        UNSAFE.copyMemory(
                byteArray, arrayAddress + ix,
                dst, ARRAY_BASE_OFFSET,
                dst.size.toLong()
        )
    }

    override fun readBytes(ix: Int, dst: ByteArray, offset: Int, length: Int) {
        checkNotDropped()
        checkLength(length)
        checkBounds(buffer, ix, length)
        checkBounds(dst, offset, length)
        UNSAFE.copyMemory(
                byteArray, arrayAddress + ix,
                dst, ARRAY_BASE_OFFSET + offset,
                length.toLong()
        )
    }

    override fun readByte(ix: Int): Byte {
        checkNotDropped()
        checkBounds(buffer, ix, 1)
        return UNSAFE.getByte(byteArray, arrayAddress + ix)
    }

    override fun readShort(ix: Int): Short {
        checkNotDropped()
        checkBounds(buffer, ix, 2)
        return UNSAFE.getShort(byteArray, arrayAddress + ix)
    }

    override fun readInt(ix: Int): Int {
        checkNotDropped()
        checkBounds(buffer, ix, 4)
        return UNSAFE.getInt(byteArray, arrayAddress + ix)
    }

    override fun readLong(ix: Int): Long {
        checkNotDropped()
        checkBounds(buffer, ix, 8)
        return UNSAFE.getLong(byteArray, arrayAddress + ix)
    }

    override fun readFloat(ix: Int): Float {
        checkNotDropped()
        checkBounds(buffer, ix, 4)
        return UNSAFE.getFloat(byteArray, arrayAddress + ix)
    }

    override fun readDouble(ix: Int): Double {
        checkNotDropped()
        checkBounds(buffer, ix, 8)
        return UNSAFE.getDouble(byteArray, arrayAddress + ix)
    }

    override fun readByteVolatile(ix: Int): Byte {
        checkNotDropped()
        checkBounds(buffer, ix, 1)
        return UNSAFE.getByteVolatile(byteArray, arrayAddress + ix)

    }

    override fun readShortVolatile(ix: Int): Short {
        checkNotDropped()
        checkBounds(buffer, ix, 2)
        return UNSAFE.getShortVolatile(byteArray, arrayAddress + ix)
    }

    override fun readIntVolatile(ix: Int): Int {
        checkNotDropped()
        checkBounds(buffer, ix, 4)
        return UNSAFE.getIntVolatile(byteArray, arrayAddress + ix)
    }

    override fun readLongVolatile(ix: Int): Long {
        checkNotDropped()
        checkBounds(buffer, ix, 8)
        return UNSAFE.getLongVolatile(byteArray, arrayAddress + ix)
    }

    override fun readFloatVolatile(ix: Int): Float {
        checkNotDropped()
        checkBounds(buffer, ix, 4)
        return UNSAFE.getFloatVolatile(byteArray, arrayAddress + ix)
    }

    override fun readDoubleVolatile(ix: Int): Double {
        checkNotDropped()
        checkBounds(buffer, ix, 8)
        return UNSAFE.getDoubleVolatile(byteArray, arrayAddress + ix)
    }
}

class MutableUnsafeBuffer(buffer: ByteBuffer) : UnsafeBuffer(buffer), MutableIOBuffer {
    override fun writeBytes(ix: Int, src: ByteArray) {
        checkNotDropped()
        checkBounds(buffer, ix, src.size)
        UNSAFE.copyMemory(
                src, ARRAY_BASE_OFFSET,
                byteArray, arrayAddress + ix,
                src.size.toLong()
        )
    }

    override fun writeBytes(ix: Int, src: ByteArray, offset: Int, length: Int) {
        checkNotDropped()
        checkLength(length)
        checkBounds(buffer, ix, length)
        checkBounds(src, offset, length)
        UNSAFE.copyMemory(
                src, ARRAY_BASE_OFFSET + offset,
                byteArray, arrayAddress + ix,
                length.toLong()
        )
    }

    override fun writeByte(ix: Int, v: Byte) {
        checkNotDropped()
        checkBounds(buffer, ix, 1)
        UNSAFE.putByte(byteArray, arrayAddress + ix, v)
    }

    override fun writeShort(ix: Int, v: Short) {
        checkNotDropped()
        checkBounds(buffer, ix, 2)
        UNSAFE.putShort(byteArray, arrayAddress + ix, v)
    }

    override fun writeInt(ix: Int, v: Int) {
        checkNotDropped()
        checkBounds(buffer, ix, 4)
        UNSAFE.putInt(byteArray, arrayAddress + ix, v)
    }

    override fun writeLong(ix: Int, v: Long) {
        checkNotDropped()
        checkBounds(buffer, ix, 8)
        UNSAFE.putLong(byteArray, arrayAddress + ix, v)
    }

    override fun writeFloat(ix: Int, v: Float) {
        checkNotDropped()
        checkBounds(buffer, ix, 4)
        UNSAFE.putFloat(byteArray, arrayAddress + ix, v)
    }

    override fun writeDouble(ix: Int, v: Double) {
        checkNotDropped()
        checkBounds(buffer, ix, 8)
        UNSAFE.putDouble(byteArray, arrayAddress + ix, v)

    }

    override fun writeByteVolatile(ix: Int, v: Byte) {
        checkNotDropped()
        checkBounds(buffer, ix, 1)
        UNSAFE.putByteVolatile(byteArray, arrayAddress + ix, v)
    }

    override fun writeShortVolatile(ix: Int, v: Short) {
        checkNotDropped()
        checkBounds(buffer, ix, 2)
        UNSAFE.putShortVolatile(byteArray, arrayAddress + ix, v)
    }

    override fun writeIntVolatile(ix: Int, v: Int) {
        checkNotDropped()
        checkBounds(buffer, ix, 4)
        UNSAFE.putIntVolatile(byteArray, arrayAddress + ix, v)
    }

    override fun writeLongVolatile(ix: Int, v: Long) {
        checkNotDropped()
        checkBounds(buffer, ix, 8)
        UNSAFE.putLongVolatile(byteArray, arrayAddress + ix, v)
    }

    override fun writeFloatVolatile(ix: Int, v: Float) {
        checkNotDropped()
        checkBounds(buffer, ix, 4)
        UNSAFE.putFloatVolatile(byteArray, arrayAddress + ix, v)
    }

    override fun writeDoubleVolatile(ix: Int, v: Double) {
        checkNotDropped()
        checkBounds(buffer, ix, 8)
        UNSAFE.putDoubleVolatile(byteArray, arrayAddress + ix, v)
    }

    override fun writeIntOrdered(ix: Int, v: Int) {
        checkNotDropped()
        checkBounds(buffer, ix, 4)
        UNSAFE.putOrderedInt(byteArray, arrayAddress + ix, v)
    }

    override fun writeLongOrdered(ix: Int, v: Long) {
        checkNotDropped()
        checkBounds(buffer, ix, 8)
        UNSAFE.putOrderedLong(byteArray, arrayAddress + ix, v)
    }

    override fun fsync() {
        if (buffer is MappedByteBuffer) {
            buffer.force()
        }
    }
}
