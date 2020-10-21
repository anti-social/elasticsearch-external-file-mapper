package company.evo.persistent

import company.evo.persistent.util.withTempDir
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec

class VersionedDirectoryTest : StringSpec() {
    companion object {
        const val VERSION_FILENAME = "test.ver"
    }

    init {
        "one writer, several readers" {
            withTempDir {
                VersionedMmapDirectory.openWritable(it, VERSION_FILENAME).use { dir ->
                    dir.readVersion() shouldBe 0L

                    shouldThrow<WriteLockException> {
                        VersionedMmapDirectory.openWritable(it, VERSION_FILENAME)
                    }

                    val dirRO = VersionedMmapDirectory.openReadOnly(it, VERSION_FILENAME)
                    dirRO.readVersion() shouldBe 0L

                    dir.writeVersion(1L)
                    dirRO.readVersion() shouldBe 1L
                }

                VersionedMmapDirectory.openWritable(it, VERSION_FILENAME).use { dir ->
                    dir.readVersion() shouldBe 1L
                }
            }
        }

        "read uninitialized directory" {
            withTempDir {
                shouldThrow<FileDoesNotExistException> {
                    VersionedMmapDirectory.openReadOnly(it, VERSION_FILENAME)
                }
            }
        }

        "RAM heap directory" {
            VersionedRamDirectory.createHeap().use { dir ->
                dir.readVersion() shouldBe 0L

                dir.writeVersion(1L)
                dir.readVersion() shouldBe 1L
            }
        }

        "RAM direct directory" {
            VersionedRamDirectory.createDirect().use { dir ->
                dir.readVersion() shouldBe 0L

                dir.writeVersion(1L)
                dir.readVersion() shouldBe 1L
            }
        }
    }
}
