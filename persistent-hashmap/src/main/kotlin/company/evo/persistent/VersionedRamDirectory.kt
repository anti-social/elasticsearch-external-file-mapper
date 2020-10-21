package company.evo.persistent

import company.evo.io.BufferCleaner
import company.evo.io.IOBuffer
import company.evo.io.MutableIOBuffer
import company.evo.io.MutableUnsafeBuffer
import company.evo.rc.AtomicRefCounted
import company.evo.rc.RefCounted

import java.nio.ByteBuffer
import java.nio.file.Paths
import java.util.concurrent.ConcurrentHashMap

class VersionedRamDirectory private constructor(
        private val bufferAllocator: (Int) -> ByteBuffer,
        private val isDirect: Boolean
) : AbstractVersionedDirectory(
        MappedFile(
                "<version>",
                MutableUnsafeBuffer(
                        bufferAllocator(VersionedDirectory.VERSION_LENGTH)
                )
        )
) {

    private val buffers = ConcurrentHashMap<String, RefCounted<MappedFile<MutableIOBuffer>>>()

    private var bufferCleaner: (MutableIOBuffer) -> Unit = {}
    var useUnmapHack = false
        set(useUnmapHack) {
            if (!isDirect) {
                throw IllegalArgumentException("Only direct buffers support unmappping")
            }
            if (useUnmapHack && BufferCleaner.BUFFER_CLEANER == null) {
                throw IllegalArgumentException(BufferCleaner.UNMAP_NOT_SUPPORTED_REASON)
            }
            field = useUnmapHack
            bufferCleaner = { buffer ->
                buffer.drop()
            }
        }

    companion object {
        fun createHeap(): VersionedRamDirectory {
            return VersionedRamDirectory(ByteBuffer::allocate, false)
        }

        fun createDirect(): VersionedRamDirectory {
            return VersionedRamDirectory(ByteBuffer::allocateDirect, true)
        }
    }

    override fun createFile(
            name: String, size: Int, deleteOnExit: Boolean
    ): RefCounted<MappedFile<MutableIOBuffer>> {
        if (buffers.containsKey(name)) {
            throw FileAlreadyExistsException(Paths.get(name))
        }
        val buffer = MutableUnsafeBuffer(bufferAllocator(size))
        val file = AtomicRefCounted(MappedFile(name, buffer)) {}
        buffers[name] = file
        return file
    }

    override fun openFileWritable(name: String): RefCounted<MappedFile<MutableIOBuffer>> {
        return buffers.computeIfPresent(name) { _, file ->
            file.also { it.retain() }
        } ?: throw FileDoesNotExistException(Paths.get(name))
    }

    override fun openFileReadOnly(name: String): RefCounted<MappedFile<IOBuffer>> {
        return openFileWritable(name)
    }

    override fun deleteFile(name: String) {
        (buffers.remove(name) ?: throw FileDoesNotExistException(Paths.get(name)))
                .also { it.release() }
    }

    override fun rename(source: String, dest: String) {
        if (buffers.containsKey(dest)) {
            throw FileAlreadyExistsException(Paths.get(dest))
        }
        buffers[dest] = buffers.remove(source) ?: throw FileDoesNotExistException(Paths.get(source))
    }

    override fun close() {
        super.close()
        buffers.forEach { (_, file) -> file.release() }
        buffers.clear()
    }
}
