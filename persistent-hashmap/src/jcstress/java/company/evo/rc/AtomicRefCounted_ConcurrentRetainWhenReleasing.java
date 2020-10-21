package company.evo.rc;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.II_Result;

import kotlin.Unit;

@JCStressTest
@Outcome(id = "0, 0", expect = Expect.ACCEPTABLE, desc = "Ok")
@Outcome(id = "0, 100", expect = Expect.ACCEPTABLE, desc = "Ok")
@Outcome(id = "100, 0", expect = Expect.ACCEPTABLE, desc = "Ok")
@Outcome(id = "100, 100", expect = Expect.ACCEPTABLE, desc = "Ok")
@State
public class AtomicRefCounted_ConcurrentRetainWhenReleasing {
    private static class IntBox {
        volatile int v;

        IntBox(int v) {
            this.v = v;
        }
    }
    private AtomicRefCounted<IntBox> rc = new AtomicRefCounted<>(
            new IntBox(100), (v) -> { v.v = -1; return Unit.INSTANCE; }
    );

    @Actor
    public void release() {
        rc.release();
    }

    @Actor
    public void use1(II_Result r) {
        r.r1 = getValue();
    }

    @Actor
    public void use2(II_Result r) {
        r.r2 = getValue();
    }

    private int getValue() {
        IntBox v = rc.retain();
        boolean acquired = false;
        try {
            if (v != null) {
                acquired = true;
                return v.v;
            } else {
                return 0;
            }
        } finally {
            if (acquired) {
                rc.release();
            }
        }
    }
}
