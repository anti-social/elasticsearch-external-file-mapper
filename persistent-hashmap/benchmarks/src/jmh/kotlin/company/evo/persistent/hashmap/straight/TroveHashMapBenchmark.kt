package company.evo.persistent.hashmap.straight

import gnu.trove.map.hash.TIntFloatHashMap

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Level
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.TearDown
import org.openjdk.jmh.annotations.Threads
import org.openjdk.jmh.infra.Blackhole

open class TroveHashMapBenchmark {
    @State(Scope.Benchmark)
    open class TroveHashMapState : BaseState() {
        var map: TIntFloatHashMap = TIntFloatHashMap()

        @Setup(Level.Trial)
        fun initMap() {
            map = TIntFloatHashMap(entries, 0.5F, 0, 0.0F)

            val keys = intKeys.asSequence().take(entries)
            val values = doubleValues.asSequence().map { it.toFloat() }.take(entries)
            keys.zip(values).forEach { (k, v) ->
                map.put(k, v)
            }
        }

        @TearDown
        fun printInfo() {
            println()
            println("======== Map Info =========")
            println("Capacity: ${map.capacity()}")
            println("===========================")
        }
    }

    @Benchmark
    @Threads(1)
    open fun benchmark_1_reader(state: TroveHashMapState, blackhole: Blackhole) {
        for (ix in BaseState.ixs) {
            blackhole.consume(
                    state.map.get(BaseState.intKeys[ix])
            )
        }
    }
}
