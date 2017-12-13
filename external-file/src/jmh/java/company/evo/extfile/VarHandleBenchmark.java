package company.evo.extfile;

//import java.lang.invoke.MethodHandles;
//import java.lang.invoke.VarHandle;
//import java.nio.ByteBuffer;
//import java.nio.ByteOrder;
//import java.util.Random;
//
//import org.openjdk.jmh.annotations.*;
//import org.openjdk.jmh.infra.Blackhole;
//
//
//public class VarHandleBenchmark {
//    static final int ENTRIES = 10_000_000;
//    static final int[] ixs = new Random().ints(0, ENTRIES).limit(1000).toArray();
//    static final double[] doubleValues = new Random().doubleValues().limit(ENTRIES).toArray();
//
//    @State(Scope.Benchmark)
//    public static class IntVarHandleState {
//        static final VarHandle VALUES_VH = MethodHandles.byteBufferViewVarHandle(
//                int[].class, ByteOrder.nativeOrder()
//        );
//        private ByteBuffer values;
//
//        @Setup(Level.Trial)
//        public void setup() {
//            values = ByteBuffer.allocateDirect(ENTRIES * 4);
//        }
//
//        int getValue(int ix) {
//            return (int) VALUES_VH.get(values, ix);
//        }
//    }
//
////    @Benchmark
//    public void bench(IntVarHandleState state, Blackhole blackhole) {
//        for (int ix : ixs) {
//            blackhole.consume(state.getValue(ix));
//        }
//    }
//}
