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
                map.put(key, v)
                map.get(key, 0) == v
            })
        }
                .config(enabled = false)

        "test single put then remove and get" {
            forAll(Gen.int(), short(), { key: Int, value: Short ->
                val v = value.toShort()
                val map = RobinHoodHashtable(100)
                map.put(key, v)
                map.remove(key)
                map.get(key, 0) == 0.toShort()
            })
        }
                .config(enabled = false)

        "test put then remove" {
            val map = RobinHoodHashtable(10)
            map.put(-438415843, 3408)
            map.put(879598861, -9322)
            map.put(-1296080222, -20906)
            map.put(-783587017, 12761)
            map.put(-901230058, -21772)
            map.put(-1571068444, 26422)
            map.put(695236339, -26641)
            map.put(-1913076823, -24838)
            map.put(1564580737, -601)
            map.put(-23957747, -28833)
            map.print()
            map.remove(-783587017)
            map.remove(-23957747)
            map.print()
//            1 shouldBe 2
        }
                .config(enabled = false)

        "test put then remove 2" {
            val map = RobinHoodHashtable(20)
            map.put(9, -29734)
            map.put(39, 8023)
            map.put(34, -4066)
            map.put(33, -25250)
            map.put(4, -14852)
            map.put(38, 23856)
            map.put(18, -30703)
            map.put(9, -30344)
            map.put(4, -24228)
            map.put(35, -5893)
            map.put(2, 11401)
            map.put(3, -28922)
            map.put(29, 22798)
            map.put(22, -27092)
            map.put(0, -1382)
            map.put(19, 11203)
            map.put(9, -23891)
            map.put(10, -12909)
            map.put(0, 19568)
            map.put(8, -10589)
            map.print()
            map.remove(9)
            map.print()
            map.remove(9)
            map.remove(22)
            map.remove(3)
            map.print()
            map.get(9, 0) shouldBe 0.toShort()
            map.get(39, 0) shouldBe 8023.toShort()
            map.get(34, 0) shouldBe (-4066).toShort()
            map.get(33, 0) shouldBe (-25250).toShort()
            map.get(38, 0) shouldBe 23856.toShort()
            map.get(18, 0) shouldBe (-30703).toShort()
            map.get(4, 0) shouldBe (-24228).toShort()
            map.get(35, 0) shouldBe (-5893).toShort()
            map.get(2, 0) shouldBe 11401.toShort()
            map.get(3, 0) shouldBe 0.toShort()
            map.get(29, 0) shouldBe 22798.toShort()
            map.get(22, 0) shouldBe 0.toShort()
            map.get(19, 0) shouldBe 11203.toShort()
            map.get(10, 0) shouldBe (-12909).toShort()
            map.get(0, 0) shouldBe 19568.toShort()
            map.get(8, 0) shouldBe (-10589).toShort()
        }
                .config(enabled = false)

        "test put then remove 3" {
            val map = RobinHoodHashtable(4)
            map.put(1, 1)
            map.put(2, 2)
            map.put(9, 9)
            map.put(10, 10)
            map.print()
            map.remove(9)
            map.print()
            map.remove(1)
            map.print()
            map.get(2, 0) shouldBe 2.toShort()
            map.get(10, 0) shouldBe  10.toShort()
        }
                .config(enabled = false)

        "write bunch random entries, then read them" {
            val limit = 1_000_000
            val map = RobinHoodHashtable(limit)
            val keys = RANDOM
                    .ints(0, limit * 2)
                    .limit(limit.toLong())
                    .toArray()
            val values = RANDOM
                    .ints(Short.MIN_VALUE.toInt(), Short.MAX_VALUE.toInt())
                    .limit(limit.toLong())
                    .toArray()
            val removeKeys = RANDOM
                    .ints(0, limit)
                    .limit(limit.toLong() / 5)
                    .map { keys[it] }
                    .toArray()
            val entries = hashMapOf<Int, Short>()
            keys.withIndex().forEach { (i, k) ->
                val v = values[i].toShort()
//                println("map.put($k, $v)")
                map.put(k, v)
                entries.put(k, v)
            }
            map.print()
            removeKeys.forEach { k ->
//                println("map.remove($k)")
                map.remove(k)
                entries.remove(k)
            }
            map.print()
//            1 shouldBe 2

            entries.forEach { (k, v) ->
                if (entries.contains(k)) {
                    map.get(k, Short.MIN_VALUE) shouldBe v
                } else {
                    map.get(k, Short.MIN_VALUE) shouldBe Short.MIN_VALUE
                }
            }
        }
//                .config(enabled = false)
    }
}
