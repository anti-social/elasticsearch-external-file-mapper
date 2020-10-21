package company.evo.persistent.hashmap

import company.evo.io.IOBuffer
import company.evo.io.MutableIOBuffer

interface Serializer {
    val serial: Long
    val size: Int

    companion object {
        fun <T> getForClass(clazz: Class<T>): Serializer = when (clazz) {
            Int::class.javaPrimitiveType -> Serializer_Int()
            Long::class.javaPrimitiveType -> Serializer_Long()
            Float::class.javaPrimitiveType -> Serializer_Float()
            Double::class.javaPrimitiveType -> Serializer_Double()
            else -> throw IllegalArgumentException("Unsupported class: $clazz")
        }
    }
}

class Serializer_Short : Serializer {
    override val serial = 1L
    override val size = 2
    fun read(buf: IOBuffer, offset: Int) = buf.readShort(offset)
    fun write(buf: MutableIOBuffer, offset: Int, v: Short) {
        buf.writeShort(offset, v)
    }
}

class Serializer_Int : Serializer {
    override val serial = 2L
    override val size = 4
    fun read(buf: IOBuffer, offset: Int) = buf.readInt(offset)
    fun write(buf: MutableIOBuffer, offset: Int, v: Int) {
        buf.writeInt(offset, v)
    }
}

class Serializer_Long : Serializer {
    override val serial = 3L
    override val size = 8
    fun read(buf: IOBuffer, offset: Int) = buf.readLong(offset)
    fun write(buf: MutableIOBuffer, offset: Int, v: Long) {
        buf.writeLong(offset, v)
    }
}

class Serializer_Float : Serializer {
    override val serial = 4L
    override val size = 4
    fun read(buf: IOBuffer, offset: Int) = buf.readFloat(offset)
    fun write(buf: MutableIOBuffer, offset: Int, v: Float) {
        buf.writeFloat(offset, v)
    }
}

class Serializer_Double : Serializer {
    override val serial = 5L
    override val size = 8
    fun read(buf: IOBuffer, offset: Int) = buf.readDouble(offset)
    fun write(buf: MutableIOBuffer, offset: Int, v: Double) {
        buf.writeDouble(offset, v)
    }
}

