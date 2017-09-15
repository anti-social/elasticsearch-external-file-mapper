package company.evo.elasticsearch.indices

import org.elasticsearch.index.Index

import org.junit.Assert
import org.junit.Test
import org.junit.Rule
import org.junit.rules.TemporaryFolder


class ExternalFileTestCase : Assert() {

    @Rule @JvmField
    val tempFolder: TemporaryFolder = TemporaryFolder()

    private fun failOnNull(): Nothing {
        throw AssertionError("Value should not be null")
    }

    @Test
    fun testMemoryValues() {
        val indexName = "test"
        val fieldName = "ext_price"
        val fileUrl = "http://localhost:8080/ext_price.txt"
        val fileUpdater = ExternalFile(
                this.tempFolder.root.toPath(),
                fieldName,
                indexName,
                FileSettings(ValuesStoreType.RAM, 60, null, fileUrl, null))

        var downloaded = fileUpdater.download()
        assert(downloaded)
        val values = fileUpdater.loadValues(null)?.get()
        values ?: failOnNull()
        assertNotNull(values)
        assertEquals(1.1, values.get(1, 0.0), 0.001)
        assertEquals(1.2, values.get(2, 0.0), 0.001)
        assertEquals(1.3, values.get(3, 0.0), 0.001)
        assertEquals(0.0, values.get(4, 0.0), 0.001)

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
                this.tempFolder.root.toPath(),
                fieldName,
                indexName,
                FileSettings(ValuesStoreType.FILE,60, null, fileUrl, null))

        var downloaded = fileUpdater.download()
        assert(downloaded)
        val values = fileUpdater.loadValues(null)?.get()
        values ?: failOnNull()
        assertNotNull(values)
        assertEquals(1.1, values.get(1, 0.0), 0.001)
        assertEquals(1.2, values.get(2, 0.0), 0.001)
        assertEquals(1.3, values.get(3, 0.0), 0.001)
        assertEquals(0.0, values.get(4, 0.0), 0.001)

        downloaded = fileUpdater.download()
        assertFalse(downloaded)

        fileUpdater.updateVersion("Thu, 01 Jan 1970 00:00:01 GMT")
        downloaded = fileUpdater.download()
        assert(downloaded)
    }
}
