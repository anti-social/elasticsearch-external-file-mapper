package company.evo.persistent.hashmap.straight

import com.devexperts.dxlab.lincheck.LinChecker
import com.devexperts.dxlab.lincheck.annotations.*
import com.devexperts.dxlab.lincheck.paramgen.IntGen
import com.devexperts.dxlab.lincheck.strategy.stress.StressOptions

import io.kotlintest.specs.FunSpec

const val LIN_TEST_MULTIPLIER = 3

@Param(name = "key", gen = IntGen::class, conf = "1:3")
//@Param(name = "value", gen = IntGen::class, conf = "1:3")
class SimpleHashMapLinTest : FunSpec() {
    private val map = StraightHashMapEnv.Builder(StraightHashMapType_Int_Float)
            .initialEntries(7)
            .createAnonymousDirect()
    private val table = map.openMap()

    init {
        test("linearization test").config(enabled = false) {
            runTest()
        }
    }

    @Operation
    fun put(@Param(name = "key") key: Int, @Param(name = "key") value: Float) {
        table.put(key, value)
    }

    @Operation
    fun remove(@Param(name = "key") key: Int) {
        table.remove(key)
    }

    private fun runTest() {
        val options = StressOptions()
                .iterations(100 * LIN_TEST_MULTIPLIER)
                .invocationsPerIteration(1000 * LIN_TEST_MULTIPLIER)
                .threads(3)
        LinChecker.check(SimpleHashMapLinTest::class.java, options)
    }
}