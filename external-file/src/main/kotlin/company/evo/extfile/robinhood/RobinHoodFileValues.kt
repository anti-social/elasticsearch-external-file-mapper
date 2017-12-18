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
        return bucketIx / BUCKETS_PER_PAGE
    }

    private fun getDataPageOffset(pageIx: Int): Int {
        return (1 + pageIx) * PAGE_SIZE
    }

    private fun getBucketOffset(pageOffset: Int, bucketIx: Int): Int {
        return pageOffset + (bucketIx % BUCKETS_PER_PAGE) * BUCKET_SIZE
    }

    fun put(key: Int, value: Short): Boolean {
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
//        println(">>> map.remove($key)")
        val h = abs(key)
        find(h,
                { bucketOffset, i ->
//                    println("  --- maybeFound ---")
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
            bucketIx = nextEntryIx(h, i)
        }
    }

    private inline fun removeBucket(h: Int, dist: Int, copyBucket: (Int, Int) -> Unit) {
        val bucketIx = nextEntryIx(h, dist)
        val bucketOffset = calculateBucketOffset(bucketIx)
        println("  remove bucket ix: $bucketIx")

//        // Find first free bucket or bucket with zero distance
//        var stopBucketIx = bucketIx
//        var stopBucketOffset: Int
//        while (true) {
//            stopBucketOffset = calculateBucketOffset(stopBucketIx)
//            val meta = readBucketMeta(stopBucketOffset)
//            if (!isBucketOccupied(meta)) {
//                break
//            }
//            val stopBucketDist = getDistance(meta)
//            if (stopBucketDist == 0) {
//                break
//            }
//            stopBucketIx = nextBucketIx(stopBucketIx)
//            if (stopBucketIx == bucketIx) {
//                break
//            }
//        }
//        println("  stop bucket ix: $stopBucketIx")
//
//        // Shift all buckets between curent bucket and free bucket
//        var dstBucketIx = bucketIx
//        var dstBucketOffset = bucketOffset
//        var srcBucketIx: Int
//        var srcBucketOffset: Int
//        while (true) {
//            srcBucketIx = nextBucketIx(dstBucketIx)
//            srcBucketOffset = calculateBucketOffset(srcBucketIx)
//
//            if (srcBucketIx == stopBucketIx) {
//                break
//            }
//
//            val srcMeta = readBucketMeta(srcBucketOffset)
//            val srcDistance = getDistance(srcMeta)
////            println("  copying $srcBucketIx -> $dstBucketIx with dist: ${srcDistance - 1}")
//            putTombstone(dstBucketOffset)
//            copyBucket(srcBucketOffset, dstBucketOffset)
//            writeBucketDistance(dstBucketOffset, srcDistance - 1)
//
//            dstBucketIx = srcBucketIx
//            dstBucketOffset = srcBucketOffset
//        }
//
//        clearBucket(dstBucketOffset)

//        val prevBucketIx = nextEntryIx(h, prevI)
//        var prevBucketOffset = calculateBucketOffset(prevBucketIx)
        var i = dist + 1
//        var bucketIx = nextEntryIx(h, i)
        var dstBucketIx = bucketIx
        var dstBucketOffset = calculateBucketOffset(dstBucketIx)
        var dstDist = dist
//        println("  removeBucket($h), prevBucketIx: $prevBucketIx, bucketIx: $bucketIx")
        while (true) {
            var srcBucketIx = nextBucketIx(bucketIx)
            val srcBucketOffset = calculateBucketOffset(srcBucketIx)
            val meta = readBucketMeta(srcBucketOffset)
            val curDistance = getDistance(meta)
            println("  bucketIx: $bucketIx, meta: $meta, cur dist: $curDistance, distDiff: ${dist - dstDist}, ${isBucketOccupied(meta)}")
            if (isBucketOccupied(meta)) {
                val distanceToPrev = i - dstDist
                if (curDistance >= distanceToPrev) {
                    println("  putting tombstone into: $dstBucketIx")
                    putTombstone(dstBucketOffset)
                    println("  copying bucket: $srcBucketIx -> $dstBucketIx")
                    copyBucket(srcBucketOffset, dstBucketOffset)
                    println("  setting distance for $dstBucketIx: ${curDistance - distanceToPrev}")
                    writeBucketDistance(dstBucketOffset, curDistance - distanceToPrev)
                } else {
                    i += 1
                    srcBucketIx = nextBucketIx(srcBucketIx)
                    continue
                }
            } else {
                clearBucket(dstBucketOffset)
                break
            }
            dstBucketIx = srcBucketIx
            dstBucketOffset = srcBucketOffset
            dstDist = i
            i += 1
        }
    }

    private data class MoveAction(
            val srcBucketOffset: Int,
            val dstBucketOffset: Int,
            val newDistance: Int
    )

    private inline fun putBucket(h: Int, dist: Int, writeBucket: (Int) -> Unit): Boolean {
//        println(">>> putBucket($h, $dist)")

        val bucketIx = nextEntryIx(h, dist)
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

        return true

//        val actions = arrayListOf<MoveAction>()
//        var srcBucketIx = bucketIx
//        var srcBucketOffset = bucketOffset
//        while (true) {
//            val meta = readBucketMeta(srcBucketOffset)
//            val srcDistance = getDistance(meta)
//            var i = 1
//            var isLast = false
//            var dstBucketIx: Int
//            var dstBucketOffset: Int
//            while (true) {
//                dstBucketIx = srcBucketIx + i // FIXME
//                dstBucketOffset = calculateBucketOffset(dstBucketIx)
//                val dstMeta = readBucketMeta(dstBucketOffset)
//                if (!isBucketOccupied(dstMeta)) {
//                    isLast = true
//                    break
//                }
//                val dstDistance = getDistance(dstMeta)
//                if (dstDistance < srcDistance + i) {
//                    break
//                }
//                dstBucketIx += 1 // FIXME
//                i += 1
//            }
//            actions.add(MoveAction(srcBucketOffset, dstBucketOffset, srcDistance + i))
//            if (isLast) {
//                break
//            }
//        }
//
//        actions.asReversed().forEach {
//            copyBucket(it.srcBucketOffset, it.dstBucketOffset)
//            writeBucketDistance(it.dstBucketOffset, -1)
//            putTombstone(it.srcBucketOffset)
//        }
//        writeBucket(bucketOffset)
//        writeBucketDistance(bucketOffset, dist)
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
