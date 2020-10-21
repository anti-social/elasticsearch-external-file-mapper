package company.evo.persistent.hashmap.straight

import company.evo.io.IOBuffer
import company.evo.io.MutableIOBuffer
import company.evo.persistent.MappedFile
import company.evo.persistent.hashmap.BucketLayout
import company.evo.persistent.hashmap.PAGE_SIZE
import company.evo.persistent.hashmap.straight.keyTypes.Int.*
import company.evo.persistent.hashmap.straight.valueTypes.Float.*
import company.evo.processor.KeyValueTemplate
import company.evo.rc.RefCounted

object StraightHashMapType_Int_Float : StraightHashMapType<HasherProvider_K, Hasher_K, StraightHashMap_Int_Float, StraightHashMapRO_Int_Float> {
    override val hasherProvider = HasherProvider_K
    override val keySerializer = Serializer_K()
    override val valueSerializer = Serializer_V()
    override val bucketLayout = BucketLayout(MapInfo.META_SIZE, keySerializer.size, valueSerializer.size)

    override fun createWritable(
            version: Long,
            file: RefCounted<MappedFile<MutableIOBuffer>>
    ): StraightHashMap_Int_Float {
        return StraightHashMapImpl_Int_Float(version, file)
    }

    override fun createReadOnly(
            version: Long,
            file: RefCounted<MappedFile<IOBuffer>>,
            collectStats: Boolean
    ): StraightHashMapRO_Int_Float {
        return StraightHashMapROImpl_Int_Float(
                version,
                file,
                if (collectStats) DefaultStatsCollector() else DummyStatsCollector()
        )
    }

    override fun copyMap(
            fromMap: StraightHashMap_Int_Float,
            toMap: StraightHashMap_Int_Float
    ): Boolean {
        val iterator = fromMap.iterator()
        while (iterator.next()) {
            if (toMap.put(iterator.key(), iterator.value()) == PutResult.OVERFLOW) {
                return false
            }
        }
        return true
    }
}

interface StraightHashMapRO_Int_Float : StraightHashMapRO {
    fun contains(key: K): Boolean
    fun get(key: K, defaultValue: V): V
    fun tombstones(): Int

    fun stats(): StatsCollector
    fun dump(dumpContent: Boolean): String
}

interface StraightHashMapIterator_Int_Float {
    fun next(): Boolean
    fun key(): K
    fun value(): V
}

@KeyValueTemplate(
        keyTypes = ["Int", "Long"],
        valueTypes = ["Short", "Int", "Long", "Double", "Float"]
)
interface StraightHashMap_Int_Float : StraightHashMapRO_Int_Float, StraightHashMap {
    fun put(key: K, value: V): PutResult
    fun remove(key: K): Boolean
    fun flush()
    fun iterator(): StraightHashMapIterator_Int_Float
}

open class StraightHashMapROImpl_Int_Float
@JvmOverloads constructor(
        override val version: Long,
        private val file: RefCounted<MappedFile<IOBuffer>>,
        private val statsCollector: StatsCollector = DummyStatsCollector()
) : StraightHashMapRO_Int_Float {

    private val buffer = file.get().buffer
    override val name = file.get().path

    init {
        assert(buffer.size() % PAGE_SIZE == 0) {
            "Buffer length should be a multiple of $PAGE_SIZE"
        }
    }

    val bucketsPerPage = MapInfo.calcBucketsPerPage(StraightHashMapType_Int_Float.bucketLayout.size)

    val header = Header.load<K, V, Hasher_K>(buffer, K::class.java, V::class.java)
    protected val hasher: Hasher_K = header.hasher

    final override val maxEntries = header.maxEntries
    final override val capacity = header.capacity

    override fun close() {
        file.release()
    }

    final override fun size() = readSize()
    final override fun tombstones() = readTombstones()

    final override fun loadBookmark(ix: Int): Long = header.loadBookmark(buffer, ix)
    final override fun loadAllBookmarks() = header.loadAllBookmarks(buffer)

    override fun stats() = statsCollector

    override fun toString() = dump(false)

    override fun dump(dumpContent: Boolean): String {
        val indexPad = capacity.toString().length
        val description = """Header: $header
            |Bucket layout: ${StraightHashMapType_Int_Float.bucketLayout}
            |Size: ${size()}
            |Tombstones: ${tombstones()}
        """.trimMargin()
        if (dumpContent) {
            val content = (0 until capacity).joinToString("\n") { bucketIx ->
                val pageOffset = getPageOffset(bucketIx)
                val bucketOffset = getBucketOffset(pageOffset, bucketIx)
                "${bucketIx.toString().padEnd(indexPad)}: " +
                        "0x${readBucketMeta(bucketOffset).toString(16)}, " +
                        "${readRawKey(bucketOffset).joinToString(", ", "[", "]")}, " +
                        readRawValue(bucketOffset).joinToString(", ", "[", "]")
            }
            return "$description\n$content"
        }
        return description
    }

    protected fun isBucketFree(meta: Int) = (meta and MapInfo.META_TAG_MASK) == 0

    protected fun isBucketOccupied(meta: Int) = (meta and MapInfo.META_OCCUPIED) != 0

    protected fun isBucketTombstoned(meta: Int) = (meta and MapInfo.META_TOMBSTONE) != 0

    protected fun bucketVersion(meta: Int) = meta and MapInfo.VER_TAG_MASK

    protected fun getBucketIx(hash: Int, dist: Int): Int {
        val h = (hash + dist) and Int.MAX_VALUE
        return  h % header.capacity
    }

    protected fun nextBucketIx(bucketIx: Int): Int {
        if (bucketIx >= header.capacity - 1) {
            return 0
        }
        return bucketIx + 1
    }

    protected fun prevBucketIx(bucketIx: Int): Int {
        if (bucketIx <= 0) {
            return header.capacity - 1
        }
        return bucketIx - 1
    }

    protected fun getPageOffset(bucketIx: Int): Int {
        return PAGE_SIZE * (1 + bucketIx / bucketsPerPage)
    }

    protected fun getBucketOffset(pageOffset: Int, bucketIx: Int): Int {
        return pageOffset + MapInfo.DATA_PAGE_HEADER_SIZE +
                (bucketIx % bucketsPerPage) * StraightHashMapType_Int_Float.bucketLayout.size
    }

    protected fun readSize(): Int {
        return buffer.readInt(Header.SIZE_OFFSET)
    }

    protected fun readTombstones(): Int {
        return buffer.readInt(Header.TOMBSTONES_OFFSET)
    }

    protected fun readBucketMeta(bucketOffset: Int): Int {
        return buffer.readShortVolatile(
                bucketOffset + StraightHashMapType_Int_Float.bucketLayout.metaOffset
        ).toInt() and 0xFFFF
    }

    protected fun readKey(bucketOffset: Int): K {
        return StraightHashMapType_Int_Float.keySerializer.read(
                buffer, bucketOffset + StraightHashMapType_Int_Float.bucketLayout.keyOffset
        )
    }

    protected fun readValue(bucketOffset: Int): V {
        return StraightHashMapType_Int_Float.valueSerializer.read(
                buffer, bucketOffset + StraightHashMapType_Int_Float.bucketLayout.valueOffset
        )
    }

    private fun readRawKey(bucketOffset: Int): ByteArray {
        val rawKey = ByteArray(StraightHashMapType_Int_Float.keySerializer.size)
        buffer.readBytes(bucketOffset + StraightHashMapType_Int_Float.bucketLayout.keyOffset, rawKey)
        return rawKey
    }

    private fun readRawValue(bucketOffset: Int): ByteArray {
        val rawValue = ByteArray(StraightHashMapType_Int_Float.valueSerializer.size)
        buffer.readBytes(bucketOffset + StraightHashMapType_Int_Float.bucketLayout.valueOffset, rawValue)
        return rawValue
    }

    override fun contains(key: K): Boolean {
        find(
                key,
                found = { _, bucketOffset, meta, dist ->
                    var m = meta
                    while (true) {
                        val meta2 = readBucketMeta(bucketOffset)
                        if (m == meta2) {
                            break
                        }
                        m = meta2
                        if (!isBucketOccupied(m) || key != readKey(bucketOffset)) {
                            statsCollector.addGet(false, dist)
                            return false
                        }
                    }
                    statsCollector.addGet(true, dist)
                    return true
                },
                notFound = { _, _, _, _, dist ->
                    statsCollector.addGet(false, dist)
                    return false
                }
        )
        return false
    }

    override fun get(key: K, defaultValue: V): V {
        find(
               key,
               found = { _, bucketOffset, meta, dist ->
                   var meta1 = meta
                   var value: V
                   while (true) {
                       value = readValue(bucketOffset)
                       val meta2 = readBucketMeta(bucketOffset)
                       if (meta1 == meta2) {
                           break
                       }
                       meta1 = meta2
                       if (!isBucketOccupied(meta1) || key != readKey(bucketOffset)) {
                           statsCollector.addGet(false, dist)
                           return defaultValue
                       }
                   }
                   statsCollector.addGet(true, dist)
                   return value
               },
               notFound = { _, _, _, _, dist ->
                   statsCollector.addGet(false, dist)
                   return defaultValue
               }
        )
        return defaultValue
    }

    protected inline fun find(
            key: K,
            found: (bucketIx: Int, bucketOffset: Int, meta: Int, dist: Int) -> Unit,
            notFound: (bucketOffset: Int, meta: Int, tombstoneOffset: Int, tombstoneMeta: Int, dist: Int) -> Unit
    ) {
        val hash = hasher.hash(key)
        var dist = -1
        var tombstoneBucketOffset = -1
        var tombstoneMeta = -1
        while(true) {
            dist++
            val bucketIx = getBucketIx(hash, dist)
            val pageOffset = getPageOffset(bucketIx)
            val bucketOffset = getBucketOffset(pageOffset, bucketIx)
            val meta = readBucketMeta(bucketOffset)
            if (isBucketTombstoned(meta)) {
                tombstoneBucketOffset = bucketOffset
                tombstoneMeta = meta
                continue
            }
            if (isBucketFree(meta) || dist > StraightHashMapBaseEnv.MAX_DISTANCE) {
                notFound(bucketOffset, meta, tombstoneBucketOffset, tombstoneMeta, dist)
                break
            }
            if (key == readKey(bucketOffset)) {
                found(bucketIx, bucketOffset, meta, dist)
                break
            }
        }
    }
}

class StraightHashMapImpl_Int_Float
@JvmOverloads constructor(
        version: Long,
        private val file: RefCounted<MappedFile<MutableIOBuffer>>,
        statsCollector: StatsCollector = DummyStatsCollector()
) : StraightHashMap_Int_Float, StraightHashMapROImpl_Int_Float(
        version,
        file,
        statsCollector
) {
    private val buffer = file.get().buffer

    protected fun writeSize(size: Int) {
        buffer.writeIntOrdered(Header.SIZE_OFFSET, size)
    }

    protected fun writeTombstones(tombstones: Int) {
        buffer.writeIntOrdered(Header.TOMBSTONES_OFFSET, tombstones)
    }

    protected fun writeBucketMeta(bucketOffset: Int, tag: Int, version: Int) {
        buffer.writeShortVolatile(
                bucketOffset + StraightHashMapType_Int_Float.bucketLayout.metaOffset,
                (tag or (version and MapInfo.VER_TAG_MASK)).toShort()
        )
    }

    protected fun writeKey(bucketOffset: Int, key: K) {
        StraightHashMapType_Int_Float.keySerializer.write(
                buffer, bucketOffset + StraightHashMapType_Int_Float.bucketLayout.keyOffset, key
        )
    }

    protected fun writeValue(bucketOffset: Int, value: V) {
        StraightHashMapType_Int_Float.valueSerializer.write(
                buffer, bucketOffset + StraightHashMapType_Int_Float.bucketLayout.valueOffset, value
        )
    }

    protected fun writeBucketData(bucketOffset: Int, key: K, value: V) {
        writeValue(bucketOffset, value)
        writeKey(bucketOffset, key)
    }

    final override fun storeBookmark(ix: Int, value: Long) = header.storeBookmark(buffer, ix, value)
    final override fun storeAllBookmarks(values: LongArray) = header.storeAllBookmarks(buffer, values)

    override fun flush() {
        buffer.fsync()
    }

    override fun put(key: K, value: V): PutResult {
        find(
                key,
                found = { _, bucketOffset, _, _ ->
                    writeValue(bucketOffset, value)
                    return PutResult.OK
                },
                notFound = { bucketOffset, meta, tombstoneOffset, tombstoneMeta, dist ->
                    if (dist > StraightHashMapBaseEnv.MAX_DISTANCE) {
                        return PutResult.OVERFLOW
                    }
                    if (size() >= header.maxEntries) {
                        return PutResult.OVERFLOW
                    }
                    if (tombstoneOffset < 0) {
                        writeBucketData(bucketOffset, key, value)
                        writeBucketMeta(bucketOffset, MapInfo.META_OCCUPIED, bucketVersion(meta) + 1)
                        writeSize(size() + 1)
                    } else {
                        writeBucketData(tombstoneOffset, key, value)
                        writeBucketMeta(tombstoneOffset, MapInfo.META_OCCUPIED, bucketVersion(tombstoneMeta) + 1)
                        writeTombstones(tombstones() - 1)
                        writeSize(size() + 1)
                    }
                }
        )
        return PutResult.OK
    }

    override fun remove(key: K): Boolean {
        find(
                key,
                found = { bucketIx, bucketOffset, meta, _ ->
                    val nextBucketIx = nextBucketIx(bucketIx)
                    val nextBucketPageOffset = getPageOffset(nextBucketIx)
                    val nextBucketOffset = getBucketOffset(nextBucketPageOffset, nextBucketIx)
                    val nextMeta = readBucketMeta(nextBucketOffset)

                    if (isBucketFree(nextMeta)) {
                        writeBucketMeta(bucketOffset, MapInfo.META_FREE, bucketVersion(meta) + 1)
                        writeTombstones(tombstones() - cleanupTombstones(bucketIx))
                        writeSize(size() - 1)
                    } else {
                        writeBucketMeta(bucketOffset, MapInfo.META_TOMBSTONE, bucketVersion(meta) + 1)
                        writeTombstones(tombstones() + 1)
                        writeSize(size() - 1)
                    }
                    return true
                },
                notFound = { _, _, _, _, _ ->
                    return false
                }
        )
        return false
    }

    private fun cleanupTombstones(bucketIx: Int): Int {
        var curBucketIx = bucketIx
        var cleaned = 0
        while (true) {
            val prevBucketIx = prevBucketIx(curBucketIx)
            val pageOffset = getPageOffset(prevBucketIx)
            val prevBucketOffset = getBucketOffset(pageOffset, prevBucketIx)
            val meta = readBucketMeta(prevBucketOffset)
            if (!isBucketTombstoned(meta)) {
                break
            }
            writeBucketMeta(prevBucketOffset, MapInfo.META_FREE, bucketVersion(meta) + 1)
            cleaned++
            curBucketIx = prevBucketIx
        }

        return cleaned
    }

    override fun iterator(): StraightHashMapIterator_Int_Float {
        return Iterator()
    }

    inner class Iterator : StraightHashMapIterator_Int_Float {
        private var curBucketIx = -1
        private var curBucketOffset = -1

        override fun next(): Boolean {
            while (true) {
                curBucketIx++
                if (curBucketIx >= capacity) {
                    return false
                }
                val pageOffset = getPageOffset(curBucketIx)
                val bucketOffset = getBucketOffset(pageOffset, curBucketIx)
                if (isBucketOccupied(readBucketMeta(bucketOffset))) {
                    curBucketOffset = bucketOffset
                    return true
                }
            }
        }
        override fun key(): K {
            if (curBucketIx >= capacity) {
                throw IndexOutOfBoundsException()
            }
            return readKey(curBucketOffset)
        }
        override fun value(): V {
            if (curBucketIx >= capacity) {
                throw IndexOutOfBoundsException()
            }
            return readValue(curBucketOffset)
        }
    }
}
