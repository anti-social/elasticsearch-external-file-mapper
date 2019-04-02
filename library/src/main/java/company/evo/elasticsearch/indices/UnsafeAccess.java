package company.evo.elasticsearch.indices;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;

public class UnsafeAccess {
    public static final Unsafe UNSAFE;
    public static final int ARRAY_BYTE_BASE_OFFSET;

    static {
        Unsafe unsafe = null;
        try {
            // final Field f = Unsafe.class.getDeclaredField("theUnsafe");
            // f.setAccessible(true);
            // unsafe = (Unsafe)f.get(null);

            final PrivilegedExceptionAction<Unsafe> action =
                () ->
                {
                    final Field f = Unsafe.class.getDeclaredField("theUnsafe");
                    f.setAccessible(true);

                    return (Unsafe)f.get(null);
                };

            unsafe = AccessController.doPrivileged(action);
        }
        catch (final Exception ex) {
            throw (RuntimeException) ex;
        }

        UNSAFE = unsafe;
        ARRAY_BYTE_BASE_OFFSET = Unsafe.ARRAY_BYTE_BASE_OFFSET;
    }
}
