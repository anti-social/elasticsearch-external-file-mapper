package company.evo.extfile.robinhood

import java.util.*

import io.kotlintest.matchers.*
import io.kotlintest.properties.*
import io.kotlintest.specs.StringSpec
import java.nio.file.Path
import java.nio.file.Paths


class RobinHoodHashMapTests : StringSpec() {
    companion object {
        private val RANDOM = Random()

        fun short() = object : Gen<Short> {
            override fun generate(): Short = RANDOM.nextInt().toShort()
        }
    }

    private val mapBuilder = RobinHoodHashtable.Builder()

    init {
        "minimum capacity: single put then get" {
            forAll(Gen.int(), short(), { key: Int, value: Short ->
                val v = value.toShort()
                val map: RobinHoodHashtable.IntToShort = mapBuilder.create(1)
                map.put(key, v)
                map.size == 1 && map.get(key, 0) == v
            })
        }
//                .config(enabled = false)

        "minimum capacity: single put then remove and get" {
            forAll(Gen.int(), short(), { k: Int, v: Short ->
                val map: RobinHoodHashtable.IntToShort = mapBuilder.create(1)
                map.put(k, v)
                map.print()
                map.remove(k)
                map.print()
                map.get(k, 0) == 0.toShort()
            })
        }
                .config(enabled = false)

        "max entries reached" {
            val map: RobinHoodHashtable.IntToShort = mapBuilder.create(2)
            map.put(1, 1) shouldBe true
            map.put(2, 2) shouldBe true
            map.put(3, 3) shouldBe false
        }
                .config(enabled = false)

        "test collisions" {
            val map: RobinHoodHashtable.IntToShort = mapBuilder.create(4)
            map.put(1, 1)
            map.put(8, 8)
            map.put(15, 15)
            map.put(2, 2)
            map.print()
            map.get(1, 0) shouldBe 1.toShort()
            map.get(2, 0) shouldBe 2.toShort()
            map.get(8, 0) shouldBe 8.toShort()
            map.get(15, 0) shouldBe  15.toShort()
        }
                .config(enabled = false)

        "remove with shift: stop at zero distance bucket" {
            val map: RobinHoodHashtable.IntToShort = mapBuilder.create(5)
            map.put(1, 1)
            map.put(8, 8)
            map.put(15, 15)
            map.put(2, 2)
            map.put(5, 5)
            map.print()
            map.remove(8)
            map.print()
            map.get(1, 0) shouldBe 1.toShort()
            map.get(2, 0) shouldBe 2.toShort()
            map.get(5, 0) shouldBe 5.toShort()
            map.get(8, 0) shouldBe 0.toShort()
            map.get(15, 0) shouldBe  15.toShort()
        }
                .config(enabled = false)

        "remove with shift: stop at free bucket" {
            val map: RobinHoodHashtable.IntToShort = mapBuilder.create(5)
            map.put(1, 1)
            map.put(2, 2)
            map.put(9, 9)
            map.put(10, 10)
            map.print()
            println("!!! Removing 9")
            map.remove(9)
            map.print()
            println("!!! Removing 1")
            map.remove(1)
            map.print()
            map.get(1, 0) shouldBe 0.toShort()
            map.get(2, 0) shouldBe 2.toShort()
            map.get(9, 0) shouldBe 0.toShort()
            map.get(10, 0) shouldBe  10.toShort()
        }
                .config(enabled = false)

        "remove missing key" {
            val map: RobinHoodHashtable.IntToShort = mapBuilder.create(5)
            map.put(1, 1)
            map.put(3, 3)
            map.put(4, 4)
            map.remove(0)
            map.remove(2)
            map.remove(5)
            map.remove(6)
            map.get(0, Short.MIN_VALUE) shouldBe Short.MIN_VALUE
            map.get(1, 0) shouldBe 1.toShort()
            map.get(2, 0) shouldBe 0.toShort()
            map.get(3, 0) shouldBe 3.toShort()
            map.get(4, 0) shouldBe 4.toShort()
            map.get(5, 0) shouldBe 0.toShort()
            map.get(6, 0) shouldBe 0.toShort()
            map.get(7, 0) shouldBe 0.toShort()
            map.get(8, 0) shouldBe 0.toShort()
        }
                .config(enabled = false)

        "put and remove bunch of random entries, then get them" {
            val limit = 1_000_000
            val maxKey = limit * 5
            val map: RobinHoodHashtable.IntToShort = mapBuilder.create(limit)
            val entries = hashMapOf<Int, Short>()
            val keys = RANDOM
                    .ints(-maxKey, maxKey)
                    .limit(limit.toLong())
            keys.forEach { k ->
                if (RANDOM.nextInt(4) == 3) {
                    map.remove(k)
                    entries.remove(k)
                } else {
                    val v = RANDOM.nextInt(Short.MAX_VALUE.toInt()).toShort()
                    map.put(k, v)
                    entries.put(k, v)
                }
            }

            map.size shouldBe entries.size
            (-maxKey..maxKey).forEach { k ->
                val v = entries[k]
                if (v != null) {
                    map.get(k, Short.MIN_VALUE) shouldBe v
                } else {
                    map.get(k, Short.MIN_VALUE) shouldBe Short.MIN_VALUE
                }
            }
        }
//                .config(enabled = false)
    }
}
