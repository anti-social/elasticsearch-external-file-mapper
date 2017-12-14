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
        val META_PAGE_SIZE = 4096
        val CATALOG_PAGE_SIZE = 4096
        val CATALOG_ENTRY_SIZE = 4
        val CATALOG_PAGE_ENTRIES = 1000
        val CATALOG_PAGE_FREE_ENTRIES = 24
        val DATA_PAGE_SIZE = 4096
        val DATA_ENTRY_SIZE = 8
        val DATA_PAGE_ENTRIES = 509
        val CATALOG_ENTRIES = intArrayOf(
                1, 3, 7, 11, 17, 23, 29, 37, 47, 59, 71, 89, 107, 131, 163, 197, 239, 293, 353, 431,
                521, 631, 761, 919, 1103, 1327, 1597, 1931, 2333, 2801, 3371, 4049, 4861, 5839,
                7013, 8419, 10103, 12143, 14591, 17519, 21023, 25229, 30293, 36353, 43627, 52361,
                62851, 75431, 90523, 108631, 130363, 156437, 187751, 225307, 270371, 324449, 389357,
                467237, 560689, 672827, 807403, 968897, 1162687, 1395263, 1674319, 2009191, 2411033,
                2893249, 3471899, 4166287, 4999559, 5999471, 7199369
        )
    }

    private val capacity = (maxEntries / LOAD_FACTOR).toInt()
    private val dataPages = (capacity + DATA_PAGE_ENTRIES - 1) / DATA_PAGE_ENTRIES
    private val minCatalogPages = (dataPages + CATALOG_PAGE_ENTRIES - 1) / CATALOG_PAGE_ENTRIES
    private val catalogPages = CATALOG_ENTRIES.first { it >= minCatalogPages }
    private val freeDataPages = catalogPages * CATALOG_PAGE_FREE_ENTRIES
    private val allDataPages = dataPages + freeDataPages
    private val dataPagesOffset = META_PAGE_SIZE + catalogPages * CATALOG_PAGE_SIZE
    private val buffer = ByteBuffer.allocateDirect(
            dataPagesOffset + allDataPages * DATA_PAGE_SIZE
    )
            .order(ByteOrder.nativeOrder())

    private val catalogFreePages = emptyMap<Int, Set<Int>>()

    init {
        println("Buffer capacity: ${buffer.capacity()}")
        println("Catalog pages: $catalogPages")
        println("Data pages: $dataPages")
        println("All data pages: $allDataPages")
        println("Data pages offset: $dataPagesOffset")
        buffer.position(META_PAGE_SIZE)
        (0 until allDataPages).forEach { dataPageIx ->
            buffer.putInt(dataPageIx)
        }
    }

    private fun nextCatalogEntryIx(key: Int, i: Int): Int {
        return (key + i * 2309) % dataPages
    }

    private fun nextEntryIx(key: Int, i: Int): Int {
        return (key + i) % DATA_PAGE_ENTRIES
    }

    private fun readCatalogEntry(catalogEntryIx: Int): Int {
        val catalogPage = catalogEntryIx / CATALOG_PAGE_ENTRIES
        val catalogPageOffset = catalogPage * CATALOG_PAGE_SIZE
        val catalogEntry = catalogEntryIx % CATALOG_PAGE_ENTRIES
        val catalogEntryOffset = catalogEntry * CATALOG_ENTRY_SIZE
        return buffer.getInt(
                META_PAGE_SIZE + catalogPageOffset + catalogEntryOffset
        )
    }

    private fun isDataPageFull(catalogEntry: Int): Boolean {
        return (catalogEntry ushr 31) > 0
    }

    private fun getDataPageIx(catalogEntry: Int): Int {
        return catalogEntry and 0x00FF_FFFF
    }

    private fun getDataPageOffset(dataPageIx: Int): Int {
        return dataPagesOffset + dataPageIx * DATA_PAGE_SIZE
    }

    private fun readEntry(offset: Int): Long {
        return buffer.getLong(offset)
    }

    private fun writeEntry(offset: Int, entry: Long) {
        buffer.putLong(offset, entry)
    }

    private fun isEntryOccupied(entry: Long): Boolean {
        return (entry and 0xFFFF_0000) != 0L
    }

    private fun getEntryKey(entry: Long): Int {
        return (entry ushr 32).toInt()
    }

    private fun getEntryValue(entry: Long): Short {
        return (entry and 0xFFFF).toShort()
    }

    private fun findCatalogEntry(k: Int): Long {
        var i = 0
        var catalogEntryIx = k % dataPages
        while (true) {
            val catalogEntry = readCatalogEntry(catalogEntryIx)
//            println("  catalog entry: ${java.lang.Integer.toHexString(catalogEntryData)}")
            if (!isDataPageFull(catalogEntry)) {
                return (catalogEntryIx.toLong() shl 32) or catalogEntry.toLong()
            }
            i += 1
            if (i >= dataPages) {
                throw IllegalStateException("Database is overflowed")
            }
            catalogEntryIx = nextCatalogEntryIx(k, i)
        }
//        println("  catalogEntryIx: $catalogEntryIx")
//        println("  dataPage: $dataPage")
    }

    private fun copyDataPage(srcDataPageIx: Int, dstDataPageIx: Int) {
        buffer.position(getDataPageOffset(srcDataPageIx))
        val src = buffer.slice()
        src.limit(DATA_PAGE_SIZE)
        buffer.position(getDataPageOffset(dstDataPageIx))
        val dst = buffer.slice()
        dst.limit(DATA_PAGE_SIZE)
        dst.put(src)
    }

    private fun getFreeDataPage(catalogPage: Int): Int {
//        val freePages = catalogFreePages[catalogEntryIx]!!
//        val freeCatalogEntryIx = freePages.single()
        return -1
    }

    fun put(key: Int, value: Short) {
        val k = abs(key)

        val catalogEntry = findCatalogEntry(k)
        val catalogEntryIx = (catalogEntry ushr 32).toInt()

        val catalogPage = catalogEntryIx / CATALOG_PAGE_ENTRIES
        val freeDataPageIx = getFreeDataPage(catalogPage)
    }

    fun putNoCopy(key: Int, value: Short) {
//        println(">>> put($key, $value)")
        // TODO Check max entries
        val k = abs(key)

        val catalogEntry = findCatalogEntry(k)
        val dataPage = getDataPageIx((catalogEntry and 0xFFFF_FFFF).toInt())
        val dataPageOffset = getDataPageOffset(dataPage)

        var entryIx = k % DATA_PAGE_ENTRIES
        var j = 0
        while (true) {
            val entryOffset = dataPageOffset + entryIx * DATA_ENTRY_SIZE
//            println("  reading entry at offset: $entryOffset")
            val entry = readEntry(entryOffset)
            if (!isEntryOccupied(entry)) {
                val newEntry = (key.toLong() shl 32) or 0x8000_0000 or
                        (value.toLong() and 0xFFFF)
//                println("  writing entry: 0x${java.lang.Long.toHexString(newEntry)} at offset: $entryOffset")
                writeEntry(entryOffset, newEntry)
                break
            }
            val entryKey = (entry ushr 32).toInt()
            if (key == entryKey) {
                val newEntry = (entry and (0x0000_FFFF_FFFF_FFFF shl 16)) or
                        (value.toLong() and 0xFFFF)
                writeEntry(entryOffset, newEntry)
                break
            }
            j += 1
            entryIx = nextEntryIx(k, j)
        }
//        println("  entryIx: $entryIx")
    }

    fun removeNoCopy(key: Int) {
        find(key) { dataPageIx, entryIx, entry ->
            if (entry != 0L) {
                val dataPageOffset = dataPagesOffset + dataPageIx * DATA_PAGE_SIZE
                val entryOffset = dataPageOffset + entryIx * DATA_ENTRY_SIZE
                writeEntry(entryOffset, 0L)
                val k = abs(key)
                var i = 1
                while (true) {
                    nextEntryIx(k, i)
                    val entryOffset = dataPageOffset + entryIx * DATA_ENTRY_SIZE
                    val entry = readEntry(entryOffset)
                    i += 1
                }
            }
        }
    }

    fun get(key: Int, defaultValue: Short): Short {
        find(key) { _, _, entry ->
            if (entry != 0L) {
                return getEntryValue(entry)
            }
        }
        return defaultValue
    }

    private inline fun find(key: Int, body: (Int, Int, Long) -> Unit) {
        val k = abs(key)
//        println(">>> get($key, $defaultValue)")
        var dataPage: Int
        var i = 0
        val catalogEntryIx = k % dataPages
        var entryIx: Int
        while (true) {
            val catalogEntry = readCatalogEntry(catalogEntryIx)
            dataPage = getDataPageIx(catalogEntry)
            val dataPageOffset = dataPagesOffset + dataPage * DATA_PAGE_SIZE
//            println("catalogEntryIx: $catalogEntryIx")
//            println("dataPage: $dataPage")

            entryIx = k % DATA_PAGE_ENTRIES
            var j = 0
            while (true) {
//                println("testing entryIx: $entryIx")
                val entryOffset = dataPageOffset + entryIx * DATA_ENTRY_SIZE
                val entry = readEntry(entryOffset)
//                println("read entry: 0x${java.lang.Long.toHexString(entry)}")
                if (!isEntryOccupied(entry)) {
                    break
                }
                val entryKey = getEntryKey(entry)
                if (key == entryKey) {
                    return body(dataPage, entryIx, entry)
                }
                j += 1
                entryIx = nextEntryIx(k, j)
            }

            if (!isDataPageFull(catalogEntry)) {
                return body(dataPage, entryIx, 0L)
            }
        }
    }
}
