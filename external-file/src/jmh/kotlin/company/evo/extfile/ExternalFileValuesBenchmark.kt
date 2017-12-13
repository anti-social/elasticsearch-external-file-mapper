package company.evo.extfile

import java.nio.file.attribute.FileTime
import java.nio.*
import java.util.*

import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole

import company.evo.extfile.robinhood.RobinHoodHashtable


const val ENTRIES = 10_000_000

open class ExternalFileValuesBenchmarks {
    companion object {
        val ixs = Random().ints(0, ENTRIES).limit(1000).toArray()
        val longsKeys = Random().longs(0, Long.MAX_VALUE).limit(ENTRIES.toLong()).toArray()
        val intKeys = Random().ints(Int.MIN_VALUE, Int.MAX_VALUE).limit(ENTRIES.toLong()).toArray()
        val doubleValues = Random().doubles().limit(ENTRIES.toLong()).toArray()
        val shortValues = Random().ints(Short.MIN_VALUE.toInt(), Short.MAX_VALUE.toInt()).limit(ENTRIES.toLong()).toArray()
    }

    open class BaseValuesState {
        lateinit var values: FileValues

        fun populateValues(backend: FileValues.Backend, config: FileValues.Config) {
            val provider = FileValues
                    .create(backend, config, FileTime.fromMillis(0))
            longsKeys.withIndex().forEach { (ix, k) ->
                provider.put(k, doubleValues[ix])
            }
            values = provider.values
        }
    }

    @State(Scope.Benchmark)
    open class TroveState : BaseValuesState() {
        @Param("LONG", "INT")
        lateinit var keyType: FileValues.Config.KeyType

        @Param("DOUBLE", "FLOAT", "INT", "SHORT")
        lateinit var valueType: FileValues.Config.ValueType

        @Setup(Level.Trial)
        fun setup() {
//            val keyType = FileValues.Config.KeyType.INT
//            val valueType = FileValues.Config.ValueType.SHORT
            val config = FileValues.Config(
                    keyType, valueType
            )
            populateValues(FileValues.Backend.TROVE, config)
        }
    }

    @State(Scope.Benchmark)
    open class ChronicleState : BaseValuesState() {
        @Param("LONG", "INT")
        lateinit var keyType: FileValues.Config.KeyType

        @Param("DOUBLE", "SHORT")
        lateinit var valueType: FileValues.Config.ValueType

        @Setup(Level.Trial)
        fun setup() {
            val config = FileValues.Config()
            config.set("entries", (ENTRIES * 2).toString())
            val provider = FileValues
                    .create(FileValues.Backend.CHRONICLE, config, FileTime.fromMillis(0))
            longsKeys.withIndex().forEach { (ix, k) ->
                provider.put(k, doubleValues[ix])
            }
            values = provider.values
        }
    }

    @State(Scope.Benchmark)
    open class LmdbState : BaseValuesState() {
        lateinit var keyType: FileValues.Config.KeyType

        lateinit var valueType: FileValues.Config.ValueType

        @Setup(Level.Trial)
        fun setup() {
            val config = FileValues.Config()
            config.set("map_size", (ENTRIES * 100).toString())
            val provider = LmdbFileValues.Provider(config, FileTime.fromMillis(0))
            longsKeys.withIndex().forEach { (ix, k) ->
                provider.put(k, doubleValues[ix])
            }
            provider.finalize()
            values = provider.values
        }
    }

    @State(Scope.Benchmark)
    open class ByteBufferState {
        lateinit var values: ByteBuffer

        @Setup(Level.Trial)
        fun setup() {
            values = ByteBuffer.allocateDirect(ENTRIES * 8)
                    .order(ByteOrder.LITTLE_ENDIAN)
            doubleValues.forEach { v ->
                values.putDouble(v)
            }
        }
    }

    @State(Scope.Benchmark)
    open class IntBufferState {
        lateinit var values: IntBuffer

        @Setup(Level.Trial)
        fun setup() {
            values = ByteBuffer.allocate(ENTRIES * 8)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .asIntBuffer()
            doubleValues.forEach { v ->
                values.put(v.toInt())
            }
        }
    }

//    @Benchmark
    fun benchmarkTroveFileValues(state: TroveState, blackhole: Blackhole) {
        for (ix in ixs) {
            blackhole.consume(
                    state.values.get(longsKeys[ix], 0.0)
            )
        }
    }

//    @Benchmark
//    @Threads(4)
    fun benchmarkChronicleFileValues(state: ChronicleState, blackhole: Blackhole) {
        for (ix in ixs) {
            blackhole.consume(
                    state.values.get(longsKeys[ix], 0.0)
            )
        }
    }

//    @Benchmark
    fun benchmarkLmdbFileValues(state: LmdbState, blackhole: Blackhole) {
        for (ix in ixs) {
            blackhole.consume(
                    state.values.get(longsKeys[ix], 0.0)
            )
        }
    }

    //    @Benchmark
    fun benchmarkByteBuffer(state: ByteBufferState, blackhole: Blackhole) {
        for (ix in ixs) {
            blackhole.consume(
                    state.values.getDouble(ix)
            )
        }
    }

//    @Benchmark
    fun benchmarkIntBuffer(state: IntBufferState, blackhole: Blackhole) {
        for (ix in ixs) {
            blackhole.consume(
                    state.values.get(ix)
            )
        }
    }

    @State(Scope.Benchmark)
    open class RobinHoodState {
        lateinit var table: RobinHoodHashtable

//        @Param("LONG", "INT")
//        lateinit var keyType: RobinHoodHashtable.KeyType
//
//        @Param("DOUBLE", "FLOAT", "INT", "SHORT")
//        lateinit var valueType: RobinHoodHashtable.ValueType

        @Setup(Level.Trial)
        fun setup() {
            table = RobinHoodHashtable(ENTRIES + ENTRIES / 2)
            intKeys.withIndex().forEach { (ix, k) ->
                table.put(k, shortValues[ix].toShort())
            }
        }
    }

    @Benchmark
    @Threads(4)
    fun benchmarkRobinHoodHashtable(state: RobinHoodState, blackhole: Blackhole) {
        for (ix in ixs) {
            blackhole.consume(
                    state.table.get(intKeys[ix], 0)
            )
        }
    }
}
