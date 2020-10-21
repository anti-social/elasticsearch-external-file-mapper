package company.evo.persistent.hashmap.straight

import java.util.Random

import org.openjdk.jmh.annotations.Param
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State

const val MAX_ENTRIES = 20_000_000
const val IDX_SEED = 1L
const val SEED = 2L

@State(Scope.Benchmark)
open class BaseState {
    companion object {
        private const val LIMIT = 1_000_000
        // const val LIMIT = 1 shl 20
        // val IX_MASK = LIMIT - 1
        val ixs: IntArray = Random(IDX_SEED)
                .ints(0, MAX_ENTRIES)
                .limit(LIMIT.toLong())
                .toArray()
        // val intKeys: IntArray = Random(SEED)
        //         .ints()
        //         .limit(MAX_ENTRIES.toLong())
        //         .toArray()
        val intKeys = (0 until MAX_ENTRIES)
                .asSequence()
                .toList()
                .toIntArray()
        // val longsKeys: LongArray = Random(SEED)
        //         .longs(0, Int.MAX_VALUE.toLong())
        //         .limit(MAX_ENTRIES.toLong())
        //         .toArray()
        val doubleValues: DoubleArray = Random(SEED)
                .doubles()
                .limit(MAX_ENTRIES.toLong())
                .toArray()
    }

    @Param("1000000", "10000000", "20000000")
    protected var entries: Int = 0
}
