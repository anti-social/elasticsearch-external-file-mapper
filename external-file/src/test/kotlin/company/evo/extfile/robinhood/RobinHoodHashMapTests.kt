package company.evo.extfile.robinhood

import io.kotlintest.matchers.*
import io.kotlintest.properties.*
import io.kotlintest.specs.StringSpec
import java.util.*
import kotlin.collections.HashMap


class RobinHoodHashMapTests : StringSpec() {
    companion object {
        private val RANDOM = Random()

        fun short() = object : Gen<Short> {
            override fun generate(): Short = RANDOM.nextInt().toShort()
        }
    }

    init {
        "test single put then get" {
            forAll(Gen.int(), short(), { key: Int, value: Short ->
                val v = value.toShort()
                val map = RobinHoodHashtable(100)
                map.putNoCopy(key, v)
                map.get(key, 0) == v
            })
        }
                .config(enabled = false)

        "test" {
            val map = RobinHoodHashtable(1_500_000)
            map.putNoCopy(1462148595, -3990)
        }
                .config(enabled = false)

        "write bunch random entries, then read them" {
            val map = RobinHoodHashtable(1_500_000)
            val limit = 1_000_000L
            val keys = RANDOM
                    .ints()
                    .limit(limit)
                    .toArray()
            val values = RANDOM
                    .ints(Short.MIN_VALUE.toInt(), Short.MAX_VALUE.toInt())
                    .limit(limit)
                    .toArray()
            val entries = hashMapOf<Int, Short>()
            keys.withIndex().forEach { (i, k) ->
                val v = values[i].toShort()
                map.putNoCopy(k, v)
                entries.put(k, v)
            }

            entries.forEach { (k, v) ->
                map.get(k, Short.MIN_VALUE) shouldBe v
            }
        }
//                .config(enabled = false)
    }
}
