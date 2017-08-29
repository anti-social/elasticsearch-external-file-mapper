package company.evo.elasticsearch.indices

import java.nio.file.Files
import java.nio.file.Path

import org.junit.Assert
import org.junit.Before
import org.junit.Test

import org.elasticsearch.index.Index


class ExternalFileTestCase : Assert() {

    lateinit var tempDir: Path

    @Before
    fun setUp() {
        this.tempDir = Files.createTempDirectory("external_files")
        this.tempDir.toFile().deleteOnExit()
    }

    private fun failOnNull(): Nothing {
        throw AssertionError("Value should not be null")
    }

    @Test
    fun testMemoryValues() {
        val indexName = "test"
        val fieldName = "ext_price"
        val fileUrl = "http://localhost:8080/ext_price.txt"
        val fileUpdater = ExternalFile(
                this.tempDir,
                Index(indexName, "_na_"),
                fieldName,
                FileSettings(ValuesStoreType.RAM, 60, fileUrl, null))

        var downloaded = fileUpdater.download()
        assert(downloaded)
        val values = fileUpdater.loadValues(null)?.get()
        values ?: failOnNull()
        assertNotNull(values)
        assertEquals(3, values.size())
        assertEquals(1.1, values.get(1, 0.0), 0.001)
        assertEquals(1.2, values.get(2, 0.0), 0.001)
        assertEquals(1.3, values.get(3, 0.0), 0.001)

        downloaded = fileUpdater.download()
        assertFalse(downloaded)

        fileUpdater.updateVersion("Thu, 01 Jan 1970 00:00:01 GMT")
        downloaded = fileUpdater.download()
        assert(downloaded)
    }

    @Test
    fun testFileValues() {
        val indexName = "test"
        val fieldName = "ext_price"
        val fileUrl = "http://localhost:8080/ext_price.txt"
        val fileUpdater = ExternalFile(
                this.tempDir,
                Index(indexName, "_na_"),
                fieldName,
                FileSettings(ValuesStoreType.FILE,60, fileUrl, null))

        var downloaded = fileUpdater.download()
        assert(downloaded)
        val values = fileUpdater.loadValues(null)?.get()
        values ?: failOnNull()
        assertNotNull(values)
        assertEquals(3, values.size())
        assertEquals(1.1, values.get(1, 0.0), 0.001)
        assertEquals(1.2, values.get(2, 0.0), 0.001)
        assertEquals(1.3, values.get(3, 0.0), 0.001)

        downloaded = fileUpdater.download()
        assertFalse(downloaded)

        fileUpdater.updateVersion("Thu, 01 Jan 1970 00:00:01 GMT")
        downloaded = fileUpdater.download()
        assert(downloaded)
    }
}
