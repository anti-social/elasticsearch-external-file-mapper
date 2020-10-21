package company.evo.persistent.hashmap.straight

import company.evo.io.IOBuffer
import company.evo.io.MutableIOBuffer
import company.evo.persistent.MappedFile
import company.evo.persistent.hashmap.BucketLayout
import company.evo.persistent.hashmap.Hasher
import company.evo.persistent.hashmap.HasherProvider
import company.evo.persistent.hashmap.PAGE_SIZE
import company.evo.persistent.hashmap.PRIMES
import company.evo.persistent.hashmap.Serializer
import company.evo.rc.RefCounted

open class PersistentHashMapException(msg: String, cause: Exception? = null) : Exception(msg, cause)
class InvalidHashtableException(msg: String) : PersistentHashMapException(msg)

enum class PutResult {
    OK, OVERFLOW
}

interface StraightHashMapRO : AutoCloseable {
    val version: Long
    val name: String
    val maxEntries: Int
    val capacity: Int
    fun size(): Int

    fun loadBookmark(ix: Int): Long
    fun loadAllBookmarks(): LongArray
}

interface StraightHashMap : StraightHashMapRO {
    fun storeBookmark(ix: Int, value: Long)
    fun storeAllBookmarks(values: LongArray)
}

interface StraightHashMapType<P: HasherProvider<H>, H: Hasher, W: StraightHashMap, RO: StraightHashMapRO> {
    val bucketLayout: BucketLayout
    val keySerializer: Serializer
    val valueSerializer: Serializer
    val hasherProvider: HasherProvider<H>
    fun createWritable(
            version: Long,
            file: RefCounted<MappedFile<MutableIOBuffer>>
    ): W
    fun createReadOnly(
            version: Long,
            file: RefCounted<MappedFile<IOBuffer>>,
            collectStats: Boolean = false
    ): RO
    fun copyMap(fromMap: W, toMap: W): Boolean
}

data class MapInfo(
        val maxEntries: Int,
        val loadFactor: Double,
        val capacity: Int,
        val bucketsPerPage: Int,
        val numDataPages: Int,
        val bufferSize: Int
) {
    companion object {
        const val DATA_PAGE_HEADER_SIZE = 16

        const val META_SIZE = 2
        const val META_TAG_BITS = 2
        const val META_TAG_SHIFT = 14
        const val META_TAG_MASK = ((1 shl META_TAG_BITS) - 1) shl META_TAG_SHIFT
        const val META_FREE = 0x0000
        const val META_OCCUPIED = 0x8000
        const val META_TOMBSTONE = 0x4000
        const val VER_TAG_BITS = 14
        const val VER_TAG_MASK = (1 shl VER_TAG_BITS) - 1

        fun calcFor(maxEntries: Int, loadFactor: Double, bucketSize: Int): MapInfo {
            val capacity = calcCapacity(maxEntries, loadFactor)
            val bucketsPerPage = calcBucketsPerPage(bucketSize)
            val numDataPages = calcDataPages(capacity, bucketsPerPage)
            return MapInfo(
                    maxEntries = maxEntries,
                    loadFactor = loadFactor,
                    capacity = capacity,
                    bucketsPerPage = bucketsPerPage,
                    numDataPages = numDataPages,
                    bufferSize = (1 + numDataPages) * PAGE_SIZE
            )
        }

        fun calcCapacity(maxEntries: Int, loadFactor: Double): Int {
            val minCapacity = Math.ceil(maxEntries / loadFactor).toInt()
            return PRIMES.first { it >= minCapacity }
        }

        fun calcDataPages(capacity: Int, bucketsPerPage: Int): Int {
            return (capacity + bucketsPerPage - 1) / bucketsPerPage
        }

        fun calcBucketsPerPage(bucketSize: Int): Int {
            return (PAGE_SIZE  - DATA_PAGE_HEADER_SIZE) / bucketSize
        }
    }

    fun initBuffer(
            buffer: MutableIOBuffer,
            keySerializer: Serializer,
            valueSerializer: Serializer,
            hasher: Hasher
    ) {
        val header = Header(
                capacity, maxEntries,
                keySerializer, valueSerializer,
                hasher
        )
        header.dump(buffer)
    }
}

class Header<out H: Hasher>(
        val capacity: Int,
        val maxEntries: Int,
        val keySerializer: Serializer,
        val valueSerializer: Serializer,
        val hasher: H
) {
    companion object {
        val MAGIC = "SPHT\r\n\r\n".toByteArray()
        const val FLAGS_OFFSET = 8
        const val CAPACITY_OFFSET = 16
        const val MAX_ENTRIES_OFFSET = 24
        const val SIZE_OFFSET = 32
        const val TOMBSTONES_OFFSET = 40

        // Flags
        private const val TYPE_BITS = 3
        private const val TYPE_MASK = (1L shl TYPE_BITS) - 1
        private const val KEY_TYPE_SHIFT = 0
        private const val VALUE_TYPE_SHIFT = 3
        private const val HASHER_SERIAL_BITS = 8
        private const val HASHER_SERIAL_SHIFT = 8
        private const val HASHER_SERIAL_MASK = (1L shl HASHER_SERIAL_BITS) - 1

        const val NUM_BOOKMARKS = 32

        inline fun <K, V, reified H: Hasher> load(buffer: IOBuffer, keyClazz: Class<K>, valueClazz: Class<V>): Header<H> {
            val magic = ByteArray(MAGIC.size)
            buffer.readBytes(0, magic)
            if (!magic.contentEquals(MAGIC)) {
                throw InvalidHashtableException(
                        "Expected ${MAGIC.contentToString()} magic number " +
                                "but was: ${magic.contentToString()}"
                )
            }
            val flags = buffer.readLong(FLAGS_OFFSET)
            val keySerializer = Serializer.getForClass(keyClazz)
                    .also {
                        val serial = getKeySerial(flags)
                        assert(serial == it.serial) {
                            "Mismatch key type serial: expected ${it.serial} but was $serial"
                        }
                    }
            val valueSerializer = Serializer.getForClass(valueClazz)
                    .also {
                        val serial = getValueSerial(flags)
                        assert(serial == it.serial) {
                            "Mismatch value type serial: expected ${it.serial} but was $serial"
                        }
                    }
            val hasher = HasherProvider.getHashProvider<K, H>(keyClazz)
                    .getHasher(getHasherSerial(flags)) as? H
                    ?: throw InvalidHashtableException("Mismatched hasher for a key type")
            val capacity = toIntOrFail(buffer.readLong(CAPACITY_OFFSET), "capacity")
            val maxEntries = toIntOrFail(buffer.readLong(MAX_ENTRIES_OFFSET), "initialEntries")
            return Header(
                    capacity = capacity,
                    maxEntries = maxEntries,
                    keySerializer = keySerializer,
                    valueSerializer = valueSerializer,
                    hasher = hasher
            )
        }

        fun toIntOrFail(v: Long, property: String): Int {
            if (v > Int.MAX_VALUE) {
                throw InvalidHashtableException(
                        "Currently maximum supported $property is: ${Int.MAX_VALUE}, but was: $v"
                )
            }
            return v.toInt()
        }

        private fun calcFlags(
                keySerializer: Serializer,
                valueSerializer: Serializer,
                hasher: Hasher
        ): Long {
            return (keySerializer.serial and TYPE_MASK shl KEY_TYPE_SHIFT) or
                    (valueSerializer.serial and TYPE_MASK shl VALUE_TYPE_SHIFT) or
                    (hasher.serial and HASHER_SERIAL_MASK shl HASHER_SERIAL_SHIFT)
        }

        fun getKeySerial(flags: Long): Long {
            return flags ushr KEY_TYPE_SHIFT and TYPE_MASK
        }

        fun getValueSerial(flags: Long): Long {
            return flags ushr VALUE_TYPE_SHIFT and TYPE_MASK
        }

        fun getHasherSerial(flags: Long): Long {
            return flags ushr HASHER_SERIAL_SHIFT and HASHER_SERIAL_MASK
        }
    }

    fun dump(buffer: MutableIOBuffer) {
        buffer.writeBytes(0, MAGIC)
        buffer.writeLong(
                FLAGS_OFFSET,
                calcFlags(keySerializer, valueSerializer, hasher)
        )
        buffer.writeLong(CAPACITY_OFFSET, capacity.toLong())
        buffer.writeLong(MAX_ENTRIES_OFFSET, maxEntries.toLong())
        buffer.writeLong(SIZE_OFFSET, 0)
        buffer.writeLong(TOMBSTONES_OFFSET, 0)
    }

    fun loadBookmark(buffer: IOBuffer, bookmarkIx: Int): Long {
        checkNumBookmarks(bookmarkIx)
        return buffer.readLongVolatile(bookmarkOffset(bookmarkIx))
    }

    fun storeBookmark(buffer: MutableIOBuffer, bookmarkIx: Int, value: Long) {
        checkNumBookmarks(bookmarkIx)
        buffer.writeLongVolatile(bookmarkOffset(bookmarkIx), value)
    }

    fun loadAllBookmarks(buffer: IOBuffer): LongArray {
        val bookmarks = LongArray(NUM_BOOKMARKS)
        (0 until NUM_BOOKMARKS).forEach { ix ->
            bookmarks[ix] = buffer.readLongVolatile(bookmarkOffset(ix))
        }
        return bookmarks
    }

    fun storeAllBookmarks(buffer: MutableIOBuffer, bookmarks: LongArray) {
        checkNumBookmarks(bookmarks.size - 1)
        (0 until NUM_BOOKMARKS).forEach { ix ->
            buffer.writeLongVolatile(bookmarkOffset(ix), bookmarks[ix])
        }
    }

    private fun checkNumBookmarks(bookmarkIx: Int) {
        require(bookmarkIx in 0 until NUM_BOOKMARKS) {
            "Only $NUM_BOOKMARKS bookmarks are supported"
        }
    }

    private fun bookmarkOffset(bookmarkIx: Int) = PAGE_SIZE - (bookmarkIx + 1) * 8

    override fun toString(): String {
        return "${this::class.qualifiedName}<" +
                "capacity = $capacity, " +
                "maxEntries = $maxEntries" +
                ">"
    }
}

abstract class StatsCollector {
    var totalGet = 0L
        protected set
    var foundGet = 0L
        protected set
    var missedGet = 0L
        protected set
    var maxGetDistance = 0
        protected set
    val avgGetDistance
        get() = totalGetDistance.toFloat() / totalGet

    protected var totalGetDistance = 0L

    abstract fun addGet(found: Boolean, dist: Int)

    override fun toString(): String {
        return """
            |total gets: $totalGet
            |found gets: $foundGet
            |missed gets: $missedGet
            |max get distance: $maxGetDistance
            |avg get distance: $avgGetDistance
        """.trimMargin()
    }
}

class DummyStatsCollector : StatsCollector() {
    override fun addGet(found: Boolean, dist: Int) {}
}

class DefaultStatsCollector : StatsCollector() {
    override fun addGet(found: Boolean, dist: Int) {
        totalGet++
        if (found) {
            foundGet++
        } else {
            missedGet++
        }
        totalGetDistance += dist
        if (dist > maxGetDistance) {
            maxGetDistance = dist
        }
    }
}
