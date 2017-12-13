package company.evo.extfile.robinhood

import java.lang.Math.abs
import java.nio.ByteBuffer

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
        val CATALOG_FREE_ENTRIES = 24
        val DATA_PAGE_SIZE = 4096
        val DATA_ENTRY_SIZE = 8
        val DATA_PAGE_ENTRIES = 509 // 8 byte entry
        val CATALOG_ENTRIES = intArrayOf(
                1, 3, 7, 11, 17, 23, 29, 37, 47, 59, 71, 89, 107, 131, 163, 197, 239, 293, 353, 431, 521, 631, 761, 919,
                1103, 1327, 1597, 1931, 2333, 2801, 3371, 4049, 4861, 5839, 7013, 8419, 10103, 12143, 14591,
                17519, 21023, 25229, 30293, 36353, 43627, 52361, 62851, 75431, 90523, 108631, 130363, 156437,
                187751, 225307, 270371, 324449, 389357, 467237, 560689, 672827, 807403, 968897, 1162687, 1395263,
                1674319, 2009191, 2411033, 2893249, 3471899, 4166287, 4999559, 5999471, 7199369
        )
    }

    private val capacity = (maxEntries / LOAD_FACTOR).toInt()
    private val dataPages = (capacity + DATA_PAGE_ENTRIES - 1) / DATA_PAGE_ENTRIES
    private val minCatalogPages = (dataPages + CATALOG_PAGE_ENTRIES - 1) / CATALOG_PAGE_ENTRIES
    private val catalogPages = CATALOG_ENTRIES.first { it >= minCatalogPages }
    private val freeDataPages = catalogPages * CATALOG_FREE_ENTRIES
    private val allDataPages = dataPages + freeDataPages
    private val dataPagesOffset = META_PAGE_SIZE + catalogPages * CATALOG_PAGE_SIZE
    private val buffer = ByteBuffer.allocateDirect(
            dataPagesOffset + allDataPages * DATA_PAGE_SIZE
    )

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

    fun put(key: Int, value: Short) {
//        println(">>> put($key, $value)")
        // TODO Check max entries
        val k = abs(key)

        val dataPage: Int
        var i = 0
        var catalogEntryIx = k % dataPages
        while (true) {
            val catalogPage = catalogEntryIx / CATALOG_PAGE_ENTRIES
            val catalogPageOffset = catalogPage * CATALOG_PAGE_SIZE
            val catalogEntry = catalogEntryIx % CATALOG_PAGE_ENTRIES
            val catalogEntryOffset = catalogEntry * CATALOG_ENTRY_SIZE
            val catalogEntryData = buffer.getInt(
                    META_PAGE_SIZE + catalogPageOffset + catalogEntryOffset
            )
//            println("  catalog entry: ${java.lang.Integer.toHexString(catalogEntryData)}")
            val isDataPageFull = (catalogEntryData ushr 31) > 0
            if (!isDataPageFull) {
                dataPage = catalogEntryData and 0x7FFF_FFFF
                break
            }
            i += 1
            catalogEntryIx = nextCatalogEntryIx(k, i)
        }
//        println("  catalogEntryIx: $catalogEntryIx")
//        println("  dataPage: $dataPage")

        val dataPageOffset = dataPagesOffset + dataPage * DATA_PAGE_SIZE

        var entryIx = k % DATA_PAGE_ENTRIES
        var j = 0
        while (true) {
            val entryOffset = dataPageOffset + entryIx * DATA_ENTRY_SIZE
//            println("  reading entry at offset: $entryOffset")
            val entry = buffer.getLong(entryOffset)
            val isOccupied = (entry and 0xFFFF_0000) != 0L
            if (!isOccupied) {
                val newEntry = (key.toLong() shl 32) or 0x8000_0000 or (value.toLong() and 0xFFFF)
//                println("  writing entry: 0x${java.lang.Long.toHexString(newEntry)} at offset: $entryOffset")
                buffer.putLong(entryOffset, newEntry)
                break
            }
            val entryKey = (entry ushr 32).toInt()
            if (!isOccupied || key == entryKey) {
                buffer.putShort(entryOffset + 6, value)
                break
            }
            j += 1
            entryIx = nextEntryIx(k, j)
        }
//        println("  entryIx: $entryIx")
    }

    fun get(key: Int, defaultValue: Short): Short {
//        println(">>> get($key, $defaultValue)")
        val k = abs(key)


        val dataPage: Int
        var i = 0
        val catalogEntryIx = k % dataPages
        while (true) {
            val catalogPage = catalogEntryIx / CATALOG_PAGE_ENTRIES
            val catalogPageOffset = catalogPage * CATALOG_PAGE_SIZE
            val catalogEntry = catalogEntryIx % CATALOG_PAGE_ENTRIES
            val catalogEntryOffset = catalogEntry * CATALOG_ENTRY_SIZE
            val catalogEntryData = buffer.getInt(
                    META_PAGE_SIZE + catalogPageOffset + catalogEntryOffset
            )
            dataPage = catalogEntryData and 0x00FF_FFFF
            val dataPageOffset = dataPagesOffset + dataPage * DATA_PAGE_SIZE
//            println("catalogEntryIx: $catalogEntryIx")
//            println("dataPage: $dataPage")

            var entryIx = k % DATA_PAGE_ENTRIES
            var j = 0
            while (true) {
//                println("testing entryIx: $entryIx")
                val entryOffset = dataPageOffset + entryIx * DATA_ENTRY_SIZE
                val entry = buffer.getLong(entryOffset)
//                println("read entry: 0x${java.lang.Long.toHexString(entry)}")
                val isOccupied = (entry and 0xFFFF_0000) != 0L
                if (!isOccupied) {
                    return defaultValue
                }
                val entryKey = (entry ushr 32).toInt()
                if (key == entryKey) {
                    return (entry and 0xFFFF).toShort()
                }
                j += 1
                entryIx = nextEntryIx(k, j)
            }
        }
    }
}
