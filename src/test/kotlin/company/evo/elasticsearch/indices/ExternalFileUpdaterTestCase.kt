package company.evo.elasticsearch.indices

import java.nio.file.Files
import java.nio.file.Path

import org.junit.Assert
import org.junit.Before
import org.junit.Test

import org.elasticsearch.index.Index


class ExternalFileUpdaterTestCase : Assert() {

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
    fun testDownload() {
        val indexName = "test"
        val fieldName = "ext_price"
        val fileUrl = "http://localhost:8080/ext_price.txt"
        val fileUpdater = ExternalFileUpdater(
                this.tempDir,
                Index(indexName, "_na_"),
                fieldName,
                FileSettings(60, fileUrl))

        var downloaded = fileUpdater.download()
        assert(downloaded)
        val values = fileUpdater.loadValues(null)?.values
        values ?: failOnNull()
        assertNotNull(values)
        assertEquals(3, values.size)
        assertEquals(1.1, values["1"])
        assertEquals(1.2, values["2"])
        assertEquals(1.3, values["3"])

        downloaded = fileUpdater.download()
        assertFalse(downloaded)

        fileUpdater.updateVersion("Thu, 01 Jan 1970 00:00:01 GMT")
        downloaded = fileUpdater.download()
        assert(downloaded)
    }
}
