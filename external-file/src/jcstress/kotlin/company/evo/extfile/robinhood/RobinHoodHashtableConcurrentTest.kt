package company.evo.extfile.robinhood

import org.openjdk.jcstress.annotations.*
import org.openjdk.jcstress.infra.results.SS_Result


@State
@JCStressTest
@Description("Test single writer and multiple readers")
@Outcome(id = ["1", "0"], expect = Expect.ACCEPTABLE)
//    @Outcome(id = ["1", "8"], expect = Expect.ACCEPTABLE)
//    @Outcome(expect = Expect.FORBIDDEN)
class RobinHoodHashtableConcurrentTest {
    val map = RobinHoodHashtable.Builder()
            .create<RobinHoodHashtable.IntToShort>(5)
//    class HashtableState {
//        val map = RobinHoodHashtable.Builder()
//                .create<RobinHoodHashtable.IntToShort>(5)
//    }

//    class SingleWriterMultipleReadersTest {
        @Actor
        fun writer(result: SS_Result) {
            map.put(1, 1)
            map.put(8, 8)
            map.put(15, 15)
            map.remove(8)
        }

        @Actor
        fun reader1(result: SS_Result) {
            result.r1 = map.get(1, 0)
        }

        @Actor
        fun reader2(result: SS_Result) {
            result.r2 = map.get(8, 0)
        }
//    }
}
