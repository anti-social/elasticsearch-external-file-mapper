package company.evo.extfile.robinhood

import java.lang.Math.abs
import java.nio.ByteBuffer
import java.nio.ByteOrder

//import kotlin.math.abs


class RobinHoodHashtable(
//        val keyType: KeyType,
//        val valueType: ValueType,
        val maxEntries: Int
) {
    companion object {
        val LOAD_FACTOR = 0.75
        val PAGE_SIZE = 4096
        val BUCKET_SIZE = 8
        val BUCKETS_PER_PAGE = PAGE_SIZE / BUCKET_SIZE
        val PRIMES = intArrayOf(
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

    data class BucketLayout(val meta: Int, val key: Int, val value: Int)

    private val minCapacity = (maxEntries / LOAD_FACTOR).toInt()
    private val capacity = PRIMES.first { it >= minCapacity }
    private val dataPages = (capacity + BUCKETS_PER_PAGE - 1) / BUCKETS_PER_PAGE
    private val bucketLayout = BucketLayout(4, 0, 6)
    private val buffer = ByteBuffer.allocateDirect(
            (1 + dataPages) * PAGE_SIZE
    )
            .order(ByteOrder.nativeOrder())

    init {
    }

    private fun nextEntryIx(h: Int, i: Int): Int {
        return (h + i) % capacity
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
        return bucketIx / BUCKETS_PER_PAGE
    }

    private fun getDataPageOffset(pageIx: Int): Int {
        return (1 + pageIx) * PAGE_SIZE
    }

    private fun getBucketOffset(pageOffset: Int, bucketIx: Int): Int {
        return pageOffset + (bucketIx % BUCKETS_PER_PAGE) * BUCKET_SIZE
    }

    fun put(key: Int, value: Short) {
//        println(">>> put($key, $value)")

        // TODO Check max entries
        val h = abs(key)

        find(h,
                { bucketOffset, i ->
                    if (key == readBucketKey(bucketOffset)) {
                        writeBucketValue(bucketOffset, value)
                        true
                    } else {
                        false
                    }
                },
                { bucketOffset, i ->
                    writeBucketData(bucketOffset, key, value)
                    writeBucketDistance(bucketOffset, i)
                }
        )
    }

    fun remove(key: Int) {
//        println(">>> map.remove($key)")
        val h = abs(key)
        find(h,
                { bucketOffset, i ->
                    if (key == readBucketKey(bucketOffset)) {
                        removeBucket(h, i, this::copyBucket)
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
                { bucketOffset, i ->
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

    private inline fun find(
            h: Int,
            maybeFound: (Int, Int) -> Boolean,
            notFound: (Int, Int) -> Unit
    ) {
//        println(">>> find($h)")
        var i = 0
        var bucketIx = nextEntryIx(h, i)
        while (true) {
            val dataPage = getDataPageIx(bucketIx)
            val dataPageOffset = getDataPageOffset(dataPage)
            val bucketOffset = getBucketOffset(dataPageOffset, bucketIx)
            val meta = readBucketMeta(bucketOffset)
            if (isBucketTombstoned(meta)) {
                continue
            }
            val dist = getDistance(meta)
//            if (!isBucketOccupied(meta) || dist < i) {
            if (!isBucketOccupied(meta)) {
                notFound(bucketOffset, i)
                break
            }
            if (maybeFound(bucketOffset, i)) {
                break
            }
            i += 1
            bucketIx = nextEntryIx(h, i)
        }
    }

    private inline fun removeBucket(h: Int, i: Int, copyBucket: (Int, Int) -> Unit) {
        var prevI = i
        var prevBucketIx = nextEntryIx(h, prevI)
        var prevBucketOffset = calculateBucketOffset(prevBucketIx)
        var i = i + 1
        var bucketIx = nextEntryIx(h, i)
//        println("  removeBucket($h), prevBucketIx: $prevBucketIx, bucketIx: $bucketIx")
        while (true) {
            val bucketOffset = calculateBucketOffset(bucketIx)
            val meta = readBucketMeta(bucketOffset)
            val distance = getDistance(meta)
//            println("  bucketIx: $bucketIx, meta: $meta, dist: $distance, distDiff: ${i - prevI}, ${isBucketOccupied(meta)}")
            if (isBucketOccupied(meta)) {
                if (distance >= i - prevI) {
//                    println("  putting tombstone into: $prevBucketIx")
                    putTombstone(prevBucketOffset)
//                    println("  copying bucket: $bucketIx -> $prevBucketIx")
                    copyBucket(bucketOffset, prevBucketOffset)
//                    println("  setting distance for $prevBucketIx: ${distance - (i - prevI)}")
                    writeBucketDistance(prevBucketOffset, distance - (i - prevI))
                } else {
                    i += 1
                    bucketIx = nextEntryIx(h, i)
                    continue
                }
            } else {
                clearBucket(prevBucketOffset)
                break
            }
            prevBucketIx = bucketIx
            prevBucketOffset = bucketOffset
            prevI = i
            i += 1
            bucketIx = nextEntryIx(h, i)
        }
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
        println("Data pages: $dataPages")
        println("Buckets per page: $BUCKETS_PER_PAGE")
        (0 until capacity).forEach { bucketIx ->
            val bucketOffset = calculateBucketOffset(bucketIx)
            val meta = readBucketMeta(bucketOffset)
            val key = readBucketKey(bucketOffset)
            val value = readBucketValue(bucketOffset)
            println("$bucketIx: ${java.lang.Integer.toBinaryString(meta)}, $key, $value")
        }
    }
}
