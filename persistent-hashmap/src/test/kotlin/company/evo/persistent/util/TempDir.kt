package company.evo.persistent.util

import java.nio.file.Files
import java.nio.file.Path

import org.apache.commons.io.FileUtils.deleteDirectory

class TempDir : AutoCloseable {
    val path: Path = Files.createTempDirectory(null)
    private val tmpDir = path.toFile().apply { deleteOnExit() }

    override fun close() {
        deleteDirectory(tmpDir)
    }
}

fun withTempDir(body: (Path) -> Unit) = TempDir().use { dir ->
    body(dir.path)
}
