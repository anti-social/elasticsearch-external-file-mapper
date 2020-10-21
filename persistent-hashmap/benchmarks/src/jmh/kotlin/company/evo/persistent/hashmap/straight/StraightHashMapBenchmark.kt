package company.evo.persistent.hashmap.straight

import company.evo.io.MutableUnsafeBuffer
import company.evo.persistent.MappedFile
import company.evo.persistent.hashmap.Hash32
import company.evo.rc.AtomicRefCounted

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Level
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.TearDown
import org.openjdk.jmh.annotations.Threads
import org.openjdk.jmh.infra.Blackhole

import java.nio.ByteBuffer

open class StraightHashMapBenchmark {
    @State(Scope.Benchmark)
    open class StraightHashMapState : BaseState() {
        var map: StraightHashMapImpl_Int_Float? = null

        @Setup(Level.Trial)
        fun initMap() {
            val mapInfo = MapInfo.calcFor(
                    entries,
                    0.5,
                    StraightHashMapType_Int_Float.bucketLayout.size
            )
            val buffer = ByteBuffer.allocateDirect(mapInfo.bufferSize)
            mapInfo.initBuffer(
                    MutableUnsafeBuffer(buffer),
                    StraightHashMapType_Int_Float.keySerializer,
                    StraightHashMapType_Int_Float.valueSerializer,
                    StraightHashMapType_Int_Float.hasherProvider.getHasher(Hash32.serial)
            )
            map = StraightHashMapImpl_Int_Float(
                    0L,
                    AtomicRefCounted(MappedFile("<map>", MutableUnsafeBuffer(buffer))) {},
                    DefaultStatsCollector()
            )

            val keys = intKeys.asSequence().take(entries)
            val values = doubleValues.asSequence().map { it.toFloat() }.take(entries)
            keys.zip(values).forEach { (k, v) ->
                map!!.put(k, v)
            }
        }

        @TearDown
        fun printStats() {
            println()
            println("======== Map Info =========")
            println(map?.toString())
            println("======== Get Stats ========")
            println(map?.stats())
            println("===========================")
        }
    }

    @Benchmark
    @Threads(1)
    open fun benchmark_1_reader(state: StraightHashMapState, blackhole: Blackhole) {
        val map = state.map
        for (ix in BaseState.ixs) {
            blackhole.consume(
                    map!!.get(BaseState.intKeys[ix], 0.0F)
            )
        }
    }
}
