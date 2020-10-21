package company.evo.io

import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import java.lang.IllegalArgumentException

import java.nio.ByteBuffer

class UnsafeBufferTests : StringSpec() {
    init {
        "direct: bytes" {
            val buffer = MutableUnsafeBuffer(ByteBuffer.allocateDirect(4))
            testBytes(buffer::readBytes, buffer::writeBytes)
        }

        "heap: bytes" {
            val buffer = MutableUnsafeBuffer(ByteBuffer.allocate(4))
            testBytes(buffer::readBytes, buffer::writeBytes)
        }

        "direct: bytes with offset" {
            val buffer = MutableUnsafeBuffer(ByteBuffer.allocateDirect(4))
            testBytesOffset(buffer::readBytes, buffer::writeBytes)
        }

        "heap: bytes with offset" {
            val buffer = MutableUnsafeBuffer(ByteBuffer.allocate(4))
            testBytesOffset(buffer::readBytes, buffer::writeBytes)
        }

        "direct: byte" {
            val buffer = MutableUnsafeBuffer(ByteBuffer.allocateDirect(2))
            testByte(buffer::readByte, buffer::writeByte)
        }

        "heap: byte" {
            val buffer = MutableUnsafeBuffer(ByteBuffer.allocate(2))
            testByte(buffer::readByte, buffer::writeByte)
        }

        "direct: volatile byte" {
            val buffer = MutableUnsafeBuffer(ByteBuffer.allocateDirect(2))
            testByte(buffer::readByteVolatile, buffer::writeByteVolatile)
        }

        "heap: volatile byte" {
            val buffer = MutableUnsafeBuffer(ByteBuffer.allocate(2))
            testByte(buffer::readByteVolatile, buffer::writeByteVolatile)
        }

        "direct: short" {
            val buffer = MutableUnsafeBuffer(ByteBuffer.allocateDirect(4))
            testShort(buffer::readShort, buffer::writeShort)
        }

        "heap: short" {
            val buffer = MutableUnsafeBuffer(ByteBuffer.allocate(4))
            testShort(buffer::readShort, buffer::writeShort)
        }

        "direct: volatile short" {
            val buffer = MutableUnsafeBuffer(ByteBuffer.allocateDirect(4))
            testShort(buffer::readShortVolatile, buffer::writeShortVolatile)
        }

        "heap: volatile short" {
            val buffer = MutableUnsafeBuffer(ByteBuffer.allocate(4))
            testShort(buffer::readShortVolatile, buffer::writeShortVolatile)
        }

        "direct: int" {
            val buffer = MutableUnsafeBuffer(ByteBuffer.allocateDirect(8))
            testInt(buffer::readInt, buffer::writeInt)
        }

        "heap: int" {
            val buffer = MutableUnsafeBuffer(ByteBuffer.allocate(8))
            testInt(buffer::readInt, buffer::writeInt)
        }

        "direct: volatile int" {
            val buffer = MutableUnsafeBuffer(ByteBuffer.allocateDirect(8))
            testInt(buffer::readIntVolatile, buffer::writeIntVolatile)
        }

        "heap: volatile int" {
            val buffer = MutableUnsafeBuffer(ByteBuffer.allocate(8))
            testInt(buffer::readIntVolatile, buffer::writeIntVolatile)
        }

        "direct: ordered int" {
            val buffer = MutableUnsafeBuffer(ByteBuffer.allocateDirect(8))
            testInt(buffer::readInt, buffer::writeIntOrdered)
        }

        "heap: ordered int" {
            val buffer = MutableUnsafeBuffer(ByteBuffer.allocate(8))
            testInt(buffer::readInt, buffer::writeIntOrdered)
        }

        "direct: long" {
            val buffer = MutableUnsafeBuffer(ByteBuffer.allocateDirect(16))
            testLong(buffer::readLong, buffer::writeLong)
        }

        "heap: long" {
            val buffer = MutableUnsafeBuffer(ByteBuffer.allocate(16))
            testLong(buffer::readLong, buffer::writeLong)
        }

        "direct: volatile long" {
            val buffer = MutableUnsafeBuffer(ByteBuffer.allocateDirect(16))
            testLong(buffer::readLongVolatile, buffer::writeLongVolatile)
        }

        "heap: volatile long" {
            val buffer = MutableUnsafeBuffer(ByteBuffer.allocate(16))
            testLong(buffer::readLongVolatile, buffer::writeLongVolatile)
        }

        "direct: ordered long" {
            val buffer = MutableUnsafeBuffer(ByteBuffer.allocateDirect(16))
            testLong(buffer::readLong, buffer::writeLongOrdered)
        }

        "heap: ordered long" {
            val buffer = MutableUnsafeBuffer(ByteBuffer.allocate(16))
            testLong(buffer::readLong, buffer::writeLongOrdered)
        }

        "direct: float" {
            val buffer = MutableUnsafeBuffer(ByteBuffer.allocateDirect(8))
            testFloat(buffer::readFloat, buffer::writeFloat)
        }

        "heap: float" {
            val buffer = MutableUnsafeBuffer(ByteBuffer.allocate(8))
            testFloat(buffer::readFloat, buffer::writeFloat)
        }

        "direct: volatile float" {
            val buffer = MutableUnsafeBuffer(ByteBuffer.allocateDirect(8))
            testFloat(buffer::readFloat, buffer::writeFloat)
        }

        "heap: volatile float" {
            val buffer = MutableUnsafeBuffer(ByteBuffer.allocate(8))
            testFloat(buffer::readFloatVolatile, buffer::writeFloatVolatile)
        }

        "direct: double" {
            val buffer = MutableUnsafeBuffer(ByteBuffer.allocateDirect(16))
            testDouble(buffer::readDouble, buffer::writeDouble)
        }

        "heap: double" {
            val buffer = MutableUnsafeBuffer(ByteBuffer.allocate(16))
            testDouble(buffer::readDouble, buffer::writeDouble)
        }

        "direct: volatile double" {
            val buffer = MutableUnsafeBuffer(ByteBuffer.allocateDirect(16))
            testDouble(buffer::readDoubleVolatile, buffer::writeDoubleVolatile)
        }

        "heap: volatile double" {
            val buffer = MutableUnsafeBuffer(ByteBuffer.allocate(16))
            testDouble(buffer::readDoubleVolatile, buffer::writeDoubleVolatile)
        }
    }

    private fun testBytes(read: (Int, ByteArray) -> Unit, write: (Int, ByteArray) -> Unit) {
        var array = ByteArray(3)
        read(0, array)
        array shouldBe ByteArray(3) { 0.toByte() }
        read(1, array)
        array shouldBe ByteArray(3) { 0.toByte() }

        array[1] = 1
        array[2] = 2
        write(0, array)
        array = ByteArray(3)
        read(0, array)
        array shouldBe ByteArray(3) { ix -> ix.toByte() }
        array = ByteArray(3)
        read(1, array)
        array shouldBe ByteArray(3) { ix -> if (ix < 2) (ix + 1).toByte() else { 0.toByte() }}

        shouldThrow<ArrayIndexOutOfBoundsException> {
            read(2, array)
        }
        shouldThrow<ArrayIndexOutOfBoundsException> {
            read(-1, array)
        }

        shouldThrow<ArrayIndexOutOfBoundsException> {
            write(2, array)
        }
        shouldThrow<ArrayIndexOutOfBoundsException> {
            write(-1, array)
        }
    }

    private fun testBytesOffset(
            read: (Int, ByteArray, Int, Int) -> Unit,
            write: (Int, ByteArray, Int, Int) -> Unit
    ) {
        var array = ByteArray(5)
        read(0, array, 1, 3)
        array shouldBe ByteArray(5) { 0.toByte() }
        read(1, array, 1, 3)
        array shouldBe ByteArray(5) { 0.toByte() }

        array[1] = 1
        array[2] = 2
        write(0, array, 1, 3)
        array = ByteArray(5)
        read(0, array, 1, 3)
        array shouldBe ByteArray(5) { ix ->
            when (ix) {
                0 -> 0
                1 -> 1
                2 -> 2
                3 -> 0
                4 -> 0
                else -> throw IllegalArgumentException()
            }.toByte()
        }
        array = ByteArray(5)
        read(1, array, 1, 3)
        array shouldBe ByteArray(5) { ix ->
            when (ix) {
                0 -> 0
                1 -> 2
                2 -> 0
                3 -> 0
                4 -> 0
                else -> throw  IllegalArgumentException()
            }.toByte()
        }

        shouldThrow<ArrayIndexOutOfBoundsException> {
            read(2, array, 1, 3)
        }
        shouldThrow<ArrayIndexOutOfBoundsException> {
            read(-1, array, 1, 3)
        }

        shouldThrow<ArrayIndexOutOfBoundsException> {
            write(2, array, 1, 3)
        }
        shouldThrow<ArrayIndexOutOfBoundsException> {
            write(-1, array, 1, 3)
        }
    }

    private fun testByte(read: (Int) -> Byte, write: (Int, Byte) -> Unit) {
        read(0) shouldBe 0.toByte()
        read(1) shouldBe 0.toByte()

        write(0, Byte.MAX_VALUE)
        read(0) shouldBe Byte.MAX_VALUE
        read(1) shouldBe 0.toByte()

        shouldThrow<ArrayIndexOutOfBoundsException> {
            read(2)
        }
        shouldThrow<ArrayIndexOutOfBoundsException> {
            read(-1)
        }

        shouldThrow<ArrayIndexOutOfBoundsException> {
            write(2, 1.toByte())
        }
        shouldThrow<ArrayIndexOutOfBoundsException> {
            write(-1, 1.toByte())
        }
    }

    private fun testShort(read: (Int) -> Short, write: (Int, Short) -> Unit) {
        read(0) shouldBe 0.toShort()
        read(2) shouldBe 0.toShort()

        write(0, Short.MAX_VALUE)
        read(0) shouldBe Short.MAX_VALUE
        read(2) shouldBe 0.toShort()

        shouldThrow<ArrayIndexOutOfBoundsException> {
            read(4)
        }
        shouldThrow<ArrayIndexOutOfBoundsException> {
            read(-1)
        }

        shouldThrow<ArrayIndexOutOfBoundsException> {
            write(4, 1.toShort())
        }
        shouldThrow<ArrayIndexOutOfBoundsException> {
            write(-1, 1.toShort())
        }
    }

    private fun testInt(read: (Int) -> Int, write: (Int, Int) -> Unit) {
        read(0) shouldBe 0
        read(4) shouldBe 0

        write(0, Int.MAX_VALUE)
        read(0) shouldBe Int.MAX_VALUE
        read(4) shouldBe 0

        shouldThrow<ArrayIndexOutOfBoundsException> {
            read(8)
        }
        shouldThrow<ArrayIndexOutOfBoundsException> {
            read(-1)
        }

        shouldThrow<ArrayIndexOutOfBoundsException> {
            write(8, 1)
        }
        shouldThrow<ArrayIndexOutOfBoundsException> {
            write(-1, 1)
        }
    }

    private fun testLong(read: (Int) -> Long, write: (Int, Long) -> Unit) {
        read(0) shouldBe 0L
        read(8) shouldBe 0L

        write(0, Long.MAX_VALUE)
        read(0) shouldBe Long.MAX_VALUE
        read(8) shouldBe 0L

        shouldThrow<ArrayIndexOutOfBoundsException> {
            read(16)
        }
        shouldThrow<ArrayIndexOutOfBoundsException> {
            read(-1)
        }

        shouldThrow<ArrayIndexOutOfBoundsException> {
            write(16, 1)
        }
        shouldThrow<ArrayIndexOutOfBoundsException> {
            write(-1, 1)
        }
    }

    private fun testFloat(read: (Int) -> Float, write: (Int, Float) -> Unit) {
        read(0) shouldBe 0.0F
        read(4) shouldBe 0.0F

        write(0, Float.MAX_VALUE)
        read(0) shouldBe Float.MAX_VALUE
        read(4) shouldBe 0.0F

        shouldThrow<ArrayIndexOutOfBoundsException> {
            read(8)
        }
        shouldThrow<ArrayIndexOutOfBoundsException> {
            read(-1)
        }

        shouldThrow<ArrayIndexOutOfBoundsException> {
            write(8, 1.0F)
        }
        shouldThrow<ArrayIndexOutOfBoundsException> {
            write(-1, 1.0F)
        }
    }

    private fun testDouble(read: (Int) -> Double, write: (Int, Double) -> Unit) {
        read(0) shouldBe 0.0
        read(8) shouldBe 0.0

        write(0, Double.MAX_VALUE)
        read(0) shouldBe Double.MAX_VALUE
        read(8) shouldBe 0.0

        shouldThrow<ArrayIndexOutOfBoundsException> {
            read(16)
        }
        shouldThrow<ArrayIndexOutOfBoundsException> {
            read(-1)
        }

        shouldThrow<ArrayIndexOutOfBoundsException> {
            write(16, 1.0)
        }
        shouldThrow<ArrayIndexOutOfBoundsException> {
            write(-1, 1.0)
        }
    }
}
