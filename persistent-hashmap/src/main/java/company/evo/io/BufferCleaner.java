package company.evo.io;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Objects;

import static java.lang.invoke.MethodHandles.*;
import static java.lang.invoke.MethodType.methodType;

/*
 * At the moment of 1.3.21 Kotlin did not support MethodHandle.invokeExact
 * See: https://youtrack.jetbrains.com/issue/KT-28410
 *
 * An implementation was stolen from Lucene:
 * https://github.com/apache/lucene-solr/blob/releases/lucene-solr/8.0.0/lucene/core/src/java/org/apache/lucene/store/MMapDirectory.java#L339
 */
public class BufferCleaner {
    private Class<?> unmappableBufferClass;
    private MethodHandle unmapper;

    public static BufferCleaner BUFFER_CLEANER;
    public static String UNMAP_NOT_SUPPORTED_REASON;

    static {
        UnmapImplResult unmapImplResult = AccessController.doPrivileged(
                (PrivilegedAction<UnmapImplResult>) BufferCleaner::unmapImpl
        );
        BUFFER_CLEANER = unmapImplResult.bufferCleaner;
        UNMAP_NOT_SUPPORTED_REASON = unmapImplResult.notSupportedReason;
    }

    private BufferCleaner(Class<?> unmappableBufferClass, MethodHandle unmapper) {
        this.unmappableBufferClass = unmappableBufferClass;
        this.unmapper = unmapper;
    }

    private static class UnmapImplResult {
        BufferCleaner bufferCleaner;
        String notSupportedReason;

        UnmapImplResult(BufferCleaner bufferCleaner) {
            this.bufferCleaner = bufferCleaner;
            this.notSupportedReason = null;
        }

        UnmapImplResult(String notSupportedReason) {
            this.bufferCleaner = null;
            this.notSupportedReason = notSupportedReason;
        }
    }

    private static UnmapImplResult unmapImpl() {
        final MethodHandles.Lookup lookup = lookup();
        try {
            try {
                // *** sun.misc.Unsafe unmapping (Java 9+) ***
                final Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
                // first check if Unsafe has the right method, otherwise we can give up
                // without doing any security critical stuff:
                final MethodHandle unmapper = lookup.findVirtual(unsafeClass, "invokeCleaner",
                        methodType(void.class, ByteBuffer.class));
                // fetch the unsafe instance and bind it to the virtual MH:
                final Field f = unsafeClass.getDeclaredField("theUnsafe");
                f.setAccessible(true);
                final Object theUnsafe = f.get(null);
                return new UnmapImplResult(
                        newBufferCleaner(ByteBuffer.class, unmapper.bindTo(theUnsafe))
                );
            } catch (SecurityException se) {
                // rethrow to report errors correctly (we need to catch it here, as we also catch RuntimeException below!):
                throw se;
            } catch (ReflectiveOperationException | RuntimeException e) {
                // *** sun.misc.Cleaner unmapping (Java 8) ***
                final Class<?> directBufferClass = Class.forName("java.nio.DirectByteBuffer");

                final Method m = directBufferClass.getMethod("cleaner");
                m.setAccessible(true);
                final MethodHandle directBufferCleanerMethod = lookup.unreflect(m);
                final Class<?> cleanerClass = directBufferCleanerMethod.type().returnType();

                /* "Compile" a MH that basically is equivalent to the following code:
                 * void unmapper(ByteBuffer byteBuffer) {
                 *   sun.misc.Cleaner cleaner = ((java.nio.DirectByteBuffer) byteBuffer).cleaner();
                 *   if (Objects.nonNull(cleaner)) {
                 *     cleaner.clean();
                 *   } else {
                 *     noop(cleaner); // the noop is needed because MethodHandles#guardWithTest always needs ELSE
                 *   }
                 * }
                 */
                final MethodHandle cleanMethod = lookup.findVirtual(cleanerClass, "clean", methodType(void.class));
                final MethodHandle nonNullTest = lookup.findStatic(Objects.class, "nonNull", methodType(boolean.class, Object.class))
                        .asType(methodType(boolean.class, cleanerClass));
                final MethodHandle noop = dropArguments(constant(Void.class, null).asType(methodType(void.class)), 0, cleanerClass);
                final MethodHandle unmapper = filterReturnValue(directBufferCleanerMethod, guardWithTest(nonNullTest, cleanMethod, noop))
                        .asType(methodType(void.class, ByteBuffer.class));
                return new UnmapImplResult(
                        newBufferCleaner(directBufferClass, unmapper)
                );
            }
        } catch (SecurityException se) {
            return new UnmapImplResult(
                    "Unmapping is not supported," +
                            " because not all required permissions are given to the JAR file: " + se +
                            " [Please grant at least the following permissions:" +
                            " RuntimePermission(\"accessClassInPackage.sun.misc\") and" +
                            " ReflectPermission(\"suppressAccessChecks\")]"
            );
        } catch (ReflectiveOperationException | RuntimeException e) {
            return new UnmapImplResult(
                    "Unmapping is not supported on this platform," +
                            " because internal Java APIs are not compatible with this library version: " + e
            );
        }
    }

    private static BufferCleaner newBufferCleaner(final Class<?> unmappableBufferClass, final MethodHandle unmapper) {
        assert Objects.equals(methodType(void.class, ByteBuffer.class), unmapper.type());
        return new BufferCleaner(unmappableBufferClass, unmapper);
    }

    public void clean(ByteBuffer buffer) throws IOException {
        if (!buffer.isDirect()) {
            throw new IllegalArgumentException("unmapping only works with direct buffers");
        }
        if (!unmappableBufferClass.isInstance(buffer)) {
            throw new IllegalArgumentException("buffer is not an instance of " + unmappableBufferClass.getName());
        }
        final Throwable error = AccessController.doPrivileged((PrivilegedAction<Throwable>) () -> {
            try {
                unmapper.invokeExact(buffer);
                return null;
            } catch (Throwable t) {
                return t;
            }
        });
        if (error != null) {
            throw new IOException("Unable to unmap the mapped buffer", error);
        }
    }
}
