package company.evo.extfile.robinhood

import java.io.RandomAccessFile
import java.lang.Math.abs
import java.lang.Math.max
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.file.Path

//import kotlin.math.abs


class InvalidHashtable(msg: String) : Exception(msg)

class ReadonlyHashtable : Exception()

sealed class RobinHoodHashtable(builder: Builder) {
    enum class KeyType(size: Int) {
        INT(4), LONG(8)
    }
    enum class ValueType(size: Int) {
        FLOAT(4), DOUBLE(8), INT(4), SHORT(2)
    }

    /*
    Header:
    - 4b: magic (must be "RHHT")
    - 4b: capacity
    - 4b: maximum number of entries
    - 4b: number of data pages
    - 4b: number of hash table entries
     */
    private class Header(
            val maxEntries: Int,
            val capacity: Int,
            val numDataPages: Int,
            val size: Int
    ) {
        companion object {
            val HEADER_SIZE = 20
            val MAGIC = "RHHT".toByteArray()
            val SIZE_OFFSET = 16

            fun read(buffer: ByteBuffer): Header {
                val magic = ByteArray(4)
                buffer.get(magic)
                if (!magic.contentEquals(MAGIC)) {
                    throw InvalidHashtable(
                            "Expected ${MAGIC.contentToString()} magic number " +
                                    "but was: ${magic.contentToString()}"
                    )
                }
                val maxEntries = buffer.getInt()
                val capacity = buffer.getInt()
                val numDataPages = buffer.getInt()
                val size = buffer.getInt()
                return Header(capacity, maxEntries, numDataPages, size)
            }
        }

        fun write(buffer: ByteBuffer) {
            buffer.put(MAGIC)
            buffer.putInt(maxEntries)
            buffer.putInt(capacity)
            buffer.putInt(numDataPages)
            buffer.putInt(size)
        }

        fun toByteArray(): ByteArray {
            val buffer = ByteBuffer.allocate(HEADER_SIZE)
                    .order(ByteOrder.nativeOrder())
            write(buffer)
            return buffer.array()
        }
    }

    class Builder {
        internal var capacity: Int = 0
        internal var maxEntries: Int = 0
        internal var numDataPages: Int = 0
        internal var bucketsPerPage: Int = 0
        internal lateinit var bucketLayout: BucketLayout
        internal lateinit var buffer: ByteBuffer

        var writeMode: Boolean = false
            private set
        fun writeMode(writeMode: Boolean) = apply { this.writeMode = writeMode }

        internal fun calcCapacity(maxEntries: Int): Int {
            val minCapacity = (maxEntries / LOAD_FACTOR).toInt()
            return PRIMES.first { it >= minCapacity }
        }

        internal fun calcDataPages(capacity: Int, bucketsPerPage: Int): Int {
            return (capacity + bucketsPerPage - 1) / bucketsPerPage
        }

        internal fun calcBufferSize(numDataPages: Int): Int {
            return (1 + numDataPages) * PAGE_SIZE
        }

        internal fun calcBucketsPerPage(bucketLayout: BucketLayout): Int {
            return PAGE_SIZE / bucketLayout.size
        }

        fun prepareCreateAnonymous(maxEntries: Int, bucketLayout: BucketLayout) {
            this.maxEntries = maxEntries
            this.bucketLayout = bucketLayout
            capacity = calcCapacity(maxEntries)
            bucketsPerPage = calcBucketsPerPage(bucketLayout)
            numDataPages = calcDataPages(capacity, bucketsPerPage)
            buffer = ByteBuffer.allocateDirect(calcBufferSize(numDataPages))
                    .order(ByteOrder.nativeOrder())
            writeMode = true
        }

        fun prepareCreate(path: Path, maxEntries: Int, bucketLayout: BucketLayout) {
            RandomAccessFile(path.toString(), "rw").use { file ->
                capacity = calcCapacity(maxEntries)
                bucketsPerPage = calcBucketsPerPage(bucketLayout)
                numDataPages = calcDataPages(capacity, bucketsPerPage)
                file.setLength(calcBufferSize(numDataPages).toLong())
                val zerosPage = ByteArray(PAGE_SIZE)
                (0 until numDataPages + 1).forEach {
                    file.write(zerosPage)
                }
                file.seek(0)
                val header = Header(capacity, maxEntries, numDataPages, 0)
                file.write(header.toByteArray())
            }
        }

        fun prepareOpen(path: Path, bucketLayout: BucketLayout) {
            val mode = if (writeMode) "rw" else "r"
            val mapMode = if (writeMode) {
                FileChannel.MapMode.READ_WRITE
            } else {
                FileChannel.MapMode.READ_ONLY
            }
            val buffer = RandomAccessFile(path.toString(), mode).use { file ->
                val channel = file.channel
                val fileSize = channel.size()
                channel.map(mapMode, 0, fileSize)
                        .order(ByteOrder.nativeOrder())
            }
            val header = Header.read(buffer)
            capacity = header.capacity
            maxEntries = header.maxEntries
            numDataPages = header.numDataPages
            val expectedFileSize = calcBufferSize(numDataPages)
            if (buffer.capacity() != expectedFileSize) {
                throw InvalidHashtable(
                        "File size must be $expectedFileSize but was: ${buffer.capacity()}"
                )
            }
            this.buffer = buffer
        }

        inline fun <reified T: RobinHoodHashtable> instantiate(): T {
            return when (T::class) {
                IntToShort::class -> IntToShort(this) as T
                else -> throw IllegalArgumentException()
            }
        }

        inline fun <reified T: RobinHoodHashtable> create(maxEntries: Int): T {
            prepareCreateAnonymous(maxEntries, BucketLayout.create<T>())
            return instantiate()
        }

        inline fun <reified T: RobinHoodHashtable> create(path: Path, maxEntries: Int): T {
            prepareCreate(path, maxEntries, BucketLayout.create<T>())
            return open(path)
        }

        inline fun <reified T: RobinHoodHashtable> open(path: Path): T {
            prepareOpen(path, BucketLayout.create<T>())
            return instantiate()
        }
    }

    companion object {
        private val LOAD_FACTOR = 0.75
        private val PAGE_SIZE = 4096
        private val PRIMES = intArrayOf(
                // http://referencesource.microsoft.com/#mscorlib/system/collections/hashtable.cs,1663
                1, 3, 7, 11, 17, 23, 29, 37, 47, 59, 71, 89, 107, 131, 163, 197, 239, 293, 353, 431,
                521, 631, 761, 919, 1103, 1327, 1597, 1931, 2333, 2801, 3371, 4049, 4861, 5839,
                7013, 8419, 10103, 12143, 14591, 17519, 21023, 25229, 30293, 36353, 43627, 52361,
                62851, 75431, 90523, 108_631, 130_363, 156_437, 187_751, 225_307, 270_371, 324_449,
                389_357, 467_237, 560_689, 672_827, 807_403, 968_897, 1_162_687, 1_395_263,
                1_674_319, 2_009_191, 2_411_033, 2_893_249, 3_471_899, 4_166_287, 4_999_559,
                5_999_471, 7_199_369,
                // C++ stl (gcc) and
                // http://www.orcca.on.ca/~yxie/courses/cs2210b-2011/htmls/extra/PlanetMath_%20goodhashtable.pdf
                8_175_383, 12_582_917, 16_601_593, 25_165_843, 33_712_729, 50_331_653, 68_460_391,
                100_663_319, 139_022_417, 201_326_611, 282_312_799, 402_653_189, 573_292_817,
                805_306_457, 1_164_186_217, 1_610_612_741, 2_147_483_647
        )
    }

    data class BucketLayout(
            val meta: Int, val key: Int, val value: Int, val size: Int
    ) {
        companion object {
            inline fun <reified T: RobinHoodHashtable> create(): BucketLayout {
                return when(T::class) {
                    IntToShort::class -> {
                        BucketLayout(4, 0, 6, 8)
                    }
//                    KeyType.INT to ValueType.INT,
//                    KeyType.INT to ValueType.FLOAT -> {
//                        BucketLayout(0, 4, 8, 12)
//                    }
//                    KeyType.INT to ValueType.DOUBLE -> {
//                        BucketLayout(0, 4, 8, 16)
//                    }
//                    KeyType.INT to ValueType.SHORT -> {
//                        BucketLayout(4, 0, 6, 8)
//                    }
//                    KeyType.LONG to ValueType.INT,
//                    KeyType.LONG to ValueType.FLOAT,
//                    KeyType.LONG to ValueType.SHORT -> {
//                        BucketLayout(12, 0, 8, 16)
//                    }
//                    KeyType.LONG to ValueType.DOUBLE -> {
//                        BucketLayout(16, 0, 8, 24)
//                    }
                    else -> {
                        throw IllegalArgumentException()
                    }
                }
            }
        }
    }

    private val maxEntries: Int
    private val capacity: Int
    private val numDataPages: Int
    private val buffer: ByteBuffer
    private val allowWriteOperations: Boolean
    private val bucketLayout: BucketLayout
    private val bucketsPerPage: Int

    init {
        allowWriteOperations = builder.writeMode
        maxEntries = builder.maxEntries
        capacity = builder.capacity
        numDataPages = builder.numDataPages
        buffer = builder.buffer
        bucketLayout = builder.bucketLayout
        bucketsPerPage = builder.bucketsPerPage
    }

    val size: Int
        get() = buffer.getInt(Header.SIZE_OFFSET)

    private fun writeSize(size: Int) {
        buffer.putInt(Header.SIZE_OFFSET, size)
    }

    private fun getBucketIx(h: Int, i: Int): Int {
        return (h + i) % capacity
    }

    private fun nextBucketIx(bucketIx: Int): Int {
        val nextIx = bucketIx + 1
        if (nextIx >= capacity) {
            return 0
        }
        return nextIx
    }

    private fun prevBucketIx(bucketIx: Int): Int {
        val prevIx = bucketIx - 1
        if (prevIx < 0) {
            return capacity - 1
        }
        return prevIx
    }

//    private fun readEntry(offset: Int): Long {
////        println("  reading bucket at: $offset")
//        return buffer.getLong(offset)
//    }

    private fun readBucketMeta(offset: Int): Int {
        return buffer.getShort(offset + bucketLayout.meta).toInt() and 0xFFFF
    }

    private fun readBucketKey(offset: Int): Int {
        return buffer.getInt(offset + bucketLayout.key)
    }

    private fun readBucketValue(offset: Int): Short {
        return buffer.getShort(offset + bucketLayout.value)
    }

    private fun writeBucketMeta(offset: Int, meta: Int) {
        buffer.putShort(offset + bucketLayout.meta, meta.toShort())
    }

    private fun writeBucketKey(offset: Int, key: Int) {
        buffer.putInt(offset + bucketLayout.key, key)
    }

    private fun writeBucketValue(offset: Int, value: Short) {
        buffer.putShort(offset + bucketLayout.value, value)
    }

    private fun writeBucketData(offset: Int, key: Int, value: Short) {
        writeBucketKey(offset, key)
        writeBucketValue(offset, value)
    }

    private fun writeBucketDistance(offset: Int, distance: Int) {
        writeBucketMeta(offset, distance or 0x8000)
    }

    private fun putTombstone(offset: Int) {
        writeBucketMeta(offset, 0x4000)
    }

    private fun clearBucket(offset: Int) {
        writeBucketMeta(offset, 0)
    }

    private fun isBucketOccupied(meta: Int): Boolean {
        return (meta and 0x8000) != 0
    }

    private fun isBucketTombstoned(meta: Int): Boolean {
        return (meta and 0x4000) != 0
    }

    private fun getEntryKey(entry: Long): Int {
        return (entry ushr 32).toInt()
    }

    private fun getEntryValue(entry: Long): Short {
        return (entry and 0xFFFF).toShort()
    }

    private fun getDataPageIx(bucketIx: Int): Int {
        return bucketIx / bucketsPerPage
    }

    private fun getDataPageOffset(pageIx: Int): Int {
        return (1 + pageIx) * PAGE_SIZE
    }

    private fun getBucketOffset(pageOffset: Int, bucketIx: Int): Int {
        return pageOffset + (bucketIx % bucketsPerPage) * bucketLayout.size
    }

    class IntToShort(builder: Builder) : RobinHoodHashtable(builder) {

    }

    fun put(key: Int, value: Short): Boolean {
        if (!allowWriteOperations) {
            throw ReadonlyHashtable()
        }
//        println(">>> put($key, $value)")

        // TODO Check max entries
        val h = abs(key)

        find(h,
                maybeFound = { bucketOffset, dist ->
//                    println("  --- maybeFound ---")
                    if (key == readBucketKey(bucketOffset)) {
                        writeBucketValue(bucketOffset, value)
                        true
                    } else {
                        false
                    }
                },
                notFound = { bucketOffset, i ->
//                    println("  --- notFound ---")
                    putBucket(h, i) {
                        writeBucketData(bucketOffset, key, value)
                        writeBucketDistance(bucketOffset, i)
                    }
                }
        )
        return true
    }

    fun remove(key: Int) {
        if (!allowWriteOperations) {
            throw ReadonlyHashtable()
        }

//        println(">>> map.remove($key)")
        val h = abs(key)
        find(h,
                { bucketOffset, i ->
//                    println("  --- maybeFound ---")
                    if (key == readBucketKey(bucketOffset)) {
                        removeBucket(h, i)
                        true
                    } else {
                        false
                    }
                },
                { _, _ -> }
        )
    }

    fun get(key: Int, defaultValue: Short): Short {
        val h = abs(key)
        find(h,
                { bucketOffset, _ ->
                    if (key == readBucketKey(bucketOffset)) {
                        return readBucketValue(bucketOffset)
                    }
                    false
                },
                { _, _ ->
                    return defaultValue
                }
        )
        return defaultValue
    }

    fun contains(key: Int): Boolean {
        val h = abs(key)
        find(h,
                { bucketOffset, _ ->
                    if (key == readBucketKey(bucketOffset)) {
                        return true
                    }
                    false
                },
                { _, _ ->
                    return false
                }
        )
        return false
    }

    private inline fun find(
            h: Int,
            maybeFound: (Int, Int) -> Boolean,
            notFound: (Int, Int) -> Unit
    ) {
//        println(">>> find($h)")
        var i = 0
        var bucketIx = getBucketIx(h, i)
        while (true) {
            val dataPage = getDataPageIx(bucketIx)
            val dataPageOffset = getDataPageOffset(dataPage)
            val bucketOffset = getBucketOffset(dataPageOffset, bucketIx)
            val meta = readBucketMeta(bucketOffset)
            if (isBucketTombstoned(meta)) {
                i += 1
                continue
            }
            val dist = getDistance(meta)
//            println("  i: $i, dist: $dist, is occupied: ${isBucketOccupied(meta)}")
            if (!isBucketOccupied(meta) || dist < i) {
//            if (!isBucketOccupied(meta)) {
                notFound(bucketOffset, i)
                break
            }
            if (maybeFound(bucketOffset, i)) {
                break
            }
            i += 1
            bucketIx = getBucketIx(h, i)
        }
    }

    private fun removeBucket(h: Int, dist: Int) {
//        val bucketIx = getBucketIx(h, dist)

        var dstBucketIx = getBucketIx(h, dist)
//        println("  remove bucket ix: $dstBucketIx")
        var dstBucketOffset = calculateBucketOffset(dstBucketIx)
//        println("  removeBucket($h), prevBucketIx: $prevBucketIx, bucketIx: $bucketIx")
        while (true) {
            val srcBucketIx = nextBucketIx(dstBucketIx)
            val srcBucketOffset = calculateBucketOffset(srcBucketIx)
            val srcMeta = readBucketMeta(srcBucketOffset)
            val srcDistance = getDistance(srcMeta)
//            println("  src: $srcBucketIx($srcDistance), dst: $dstBucketIx")
            if (isBucketOccupied(srcMeta) && srcDistance != 0) {
//                println("  copying $srcBucketIx -> $dstBucketIx(${srcDistance - 1})")
                putTombstone(dstBucketOffset)
                copyBucket(srcBucketOffset, dstBucketOffset)
                writeBucketDistance(dstBucketOffset, srcDistance - 1)
            } else {
                clearBucket(dstBucketOffset)
                break
            }
            dstBucketIx = srcBucketIx
            dstBucketOffset = srcBucketOffset
        }

        writeSize(size - 1)
    }

    private inline fun putBucket(h: Int, dist: Int, writeBucket: (Int) -> Unit): Boolean {
//        println(">>> putBucket($h, $dist)")

        val bucketIx = getBucketIx(h, dist)
        val bucketOffset = calculateBucketOffset(bucketIx)

        // Find first free bucket
        var freeBucketIx = bucketIx
        var freeBucketOffset: Int
        while (true) {
            freeBucketOffset = calculateBucketOffset(freeBucketIx)
            val meta = readBucketMeta(freeBucketOffset)
            if (!isBucketOccupied(meta)) {
                break
            }
            freeBucketIx = nextBucketIx(freeBucketIx)
            // There are no free buckets, hash table is full
            if (freeBucketIx == bucketIx) {
                return false
            }
        }
//        println("  free bucket ix: $freeBucketIx")

        // Shift all buckets between current and free bucket
        if (bucketIx != freeBucketIx) {
            var dstBucketIx = freeBucketIx
            var dstBucketOffset = freeBucketOffset
            while (true) {
                val srcBucketIx = prevBucketIx(dstBucketIx)
                val srcBucketOffset = calculateBucketOffset(srcBucketIx)
                val srcMeta = readBucketMeta(srcBucketOffset)
                val srcDistance = getDistance(srcMeta)

//                println("  copying $srcBucketIx -> $dstBucketIx with new distance: ${srcDistance + 1}")
                copyBucket(srcBucketOffset, dstBucketOffset)
                writeBucketDistance(dstBucketOffset, srcDistance + 1)
                putTombstone(srcBucketOffset)

                if (srcBucketIx == bucketIx) {
                    break
                }

                dstBucketIx = srcBucketIx
                dstBucketOffset = srcBucketOffset
            }
        }

        // Write data into current bucket
        writeBucket(bucketOffset)
        writeBucketDistance(bucketOffset, dist)

        writeSize(size + 1)

        return true
    }

    private fun calculateBucketOffset(bucketIx: Int): Int {
        val dataPage = getDataPageIx(bucketIx)
        val dataPageOffset = getDataPageOffset(dataPage)
        return getBucketOffset(dataPageOffset, bucketIx)
    }

    private fun getDistance(meta: Int): Int {
        return meta and (0b0011_1111_1111_1111)
    }

    private fun copyBucket(srcBucketOffset: Int, dstBucketOffset: Int) {
        val key = readBucketKey(srcBucketOffset)
        val value = readBucketValue(srcBucketOffset)
        writeBucketData(dstBucketOffset, key, value)
    }

    internal fun print() {
        println("Capacity: $capacity")
        println("Buffer capacity: ${buffer.capacity()}")
        println("Data pages: $numDataPages")
        println("Buckets per page: $bucketsPerPage")
        (0 until capacity).forEach { bucketIx ->
            val bucketOffset = calculateBucketOffset(bucketIx)
            val meta = readBucketMeta(bucketOffset)
            val key = readBucketKey(bucketOffset)
            val value = readBucketValue(bucketOffset)
            println("$bucketIx: ${java.lang.Integer.toBinaryString(meta)}, $key, $value")
        }
    }
}
