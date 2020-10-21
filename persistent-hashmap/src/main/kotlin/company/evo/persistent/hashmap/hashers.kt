package company.evo.persistent.hashmap

import java.lang.IllegalArgumentException

interface Hasher {
    val serial: Long
}

interface Hasher_Int : Hasher {
    fun hash(v: Int): Int
}

interface Hasher_Long : Hasher {
    fun hash(v: Long): Int
}

interface HasherProvider<out H: Hasher> {
    companion object {
        @Suppress("UNCHECKED_CAST")
        fun <K, H: Hasher> getHashProvider(clazz: Class<K>): HasherProvider<H> = when(clazz) {
            Int::class.javaPrimitiveType -> HasherProvider_Int
            Long::class.javaPrimitiveType -> HasherProvider_Long
            else -> throw IllegalArgumentException(
                    "Unsupported primitive type: $clazz"
            )
        } as HasherProvider<H>
    }

    val defaultHasherSerial: Long
    fun getHasher(serial: Long): Hasher
}

object HasherProvider_Int : HasherProvider<Hasher_Int> {
    override val defaultHasherSerial = Hash32.serial
    override fun getHasher(serial: Long): Hasher = when (serial) {
        Hash32.serial -> Hash32
        Prospector32.serial -> Prospector32
        Murmurhash32Mix.serial -> Murmurhash32Mix
        Lowbias32.serial -> Lowbias32
        else -> throw IllegalArgumentException(
                "Unknown serial for int: $serial"
        )
    }
}

object HasherProvider_Long : HasherProvider<Hasher_Long> {
    override val defaultHasherSerial = Hash64.serial
    override fun getHasher(serial: Long) = when (serial) {
        Hash64.serial -> Hash64
        else -> throw IllegalArgumentException(
                "Unknown serial for long: $serial"
        )
    }
}

object Hash32 : Hasher_Int {
    override val serial = 0L
    override fun hash(v: Int): Int {
        // hash32 function from https://nullprogram.com/blog/2018/07/31/
        var x = v
        x = (x xor (x ushr 16)) * 0x45d9f3b
        x = (x xor (x ushr 16)) * 0x45d9f3b
        x = x xor (x ushr 16)
        return x
    }
}

object Prospector32 : Hasher_Int {
    override val serial = 1L
    override fun hash(v: Int): Int {
        // prospector32 function from https://nullprogram.com/blog/2018/07/31/
        var x = v
        x = (x xor (x ushr 15)) * 0x2c1b3c6d
        x = (x xor (x ushr 12)) * 0x297a2d39
        x = x xor (x ushr 15)
        return x
    }
}

object Murmurhash32Mix : Hasher_Int {
    override val serial = 2L
    override fun hash(v: Int): Int {
        // murmurhash32_mix32 function from https://nullprogram.com/blog/2018/07/31/
        var x = v
        x = (x xor (x ushr 16)) * -0x7a143595
        x = (x xor (x ushr 13)) * -0x3d4d51cb
        x = x xor (x ushr 16)
        return x
    }
}

object Lowbias32 : Hasher_Int {
    override val serial = 3L
    override fun hash(v: Int): Int {
        var x = v
        x = (x xor (x ushr 16)) * 0x7feb352d
        x = (x xor (x ushr 15)) * -0x7b935975
        x = x xor (x ushr 16)
        return x
    }
}

object Hash64 : Hasher_Long {
    override val serial = 0L
    override fun hash(v: Long): Int {
        // hash64 function from https://nullprogram.com/blog/2018/07/31/
        var x = v
        x = (x xor (x ushr 32)) * -0x2917014799a6026d
        x = (x xor (x ushr 32)) * -0x2917014799a6026d
        x = x xor (x ushr 32)
        return x.toInt()
    }
}
