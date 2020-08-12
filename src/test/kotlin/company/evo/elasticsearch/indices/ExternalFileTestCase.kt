package company.evo.elasticsearch.indices

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
                FileSettings(ValuesStoreType.RAM, 60, null, null, fileUrl, FileFormat.TEXT,  null))

        var downloaded = fileUpdater.download(emptyList())
        assert(downloaded)
        val values = fileUpdater.loadValues(null)?.get()
        values ?: failOnNull()
        assertNotNull(values)
        assertEquals(1.1, values.get(1, 0.0), 0.001)
        assertEquals(1.2, values.get(2, 0.0), 0.001)
        assertEquals(1.3, values.get(3, 0.0), 0.001)
        assertEquals(0.0, values.get(4, 0.0), 0.001)

        downloaded = fileUpdater.download(emptyList())
        assertFalse(downloaded)

        fileUpdater.updateVersion("Thu, 01 Jan 1970 00:00:01 GMT", null)
        downloaded = fileUpdater.download(emptyList())
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
                FileSettings(ValuesStoreType.FILE, 60, null, null, fileUrl, FileFormat.TEXT, null))

        var downloaded = fileUpdater.download(emptyList())
        assert(downloaded)
        val values = fileUpdater.loadValues(null)?.get()
        values ?: failOnNull()
        assertNotNull(values)
        assertEquals(1.1, values.get(1, 0.0), 0.001)
        assertEquals(1.2, values.get(2, 0.0), 0.001)
        assertEquals(1.3, values.get(3, 0.0), 0.001)
        assertEquals(0.0, values.get(4, 0.0), 0.001)

        downloaded = fileUpdater.download(emptyList())
        assertFalse(downloaded)

        fileUpdater.updateVersion("Thu, 01 Jan 1970 00:00:01 GMT", null)
        downloaded = fileUpdater.download(emptyList())
        assert(downloaded)
    }

    @Test
    fun testProtobufValues() {
        val indexName = "test"
        val fieldName = "ext_price"
        val fileName = "ext_price.protobuf.gz"
        val fileUrl = "http://localhost:8080/$fileName"
        val fileUpdater = ExternalFile(
            tempFolder.root.toPath(),
            fieldName,
            indexName,
            FileSettings(ValuesStoreType.RAM, 60, null, null, fileUrl, FileFormat.PROTOBUF, null))

        // val dataPath = Paths.get("src", "test", "resources", "indices", fileName)
        // Files.newOutputStream(dataPath).use { output ->
        //     (1..3L).forEach { key ->
        //         ExtFile.Entry.newBuilder()
        //             .setKey(key)
        //             .setValue(key.toFloat() / 10f + 1f)
        //             .build()
        //             .writeDelimitedTo(output)
        //     }
        // }

        var downloaded = fileUpdater.download(emptyList())
        assert(downloaded)
        val values = fileUpdater.loadValues(null)?.get()
        values ?: failOnNull()
        assertNotNull(values)
        assertEquals(1.1, values.get(1, 0.0), 0.001)
        assertEquals(1.2, values.get(2, 0.0), 0.001)
        assertEquals(1.3, values.get(3, 0.0), 0.001)
        assertEquals(0.0, values.get(4, 0.0), 0.001)

        downloaded = fileUpdater.download(emptyList())
        assertFalse(downloaded)

        fileUpdater.updateVersion("Thu, 01 Jan 1970 00:00:01 GMT", null)
        downloaded = fileUpdater.download(emptyList())
        assert(downloaded)
    }
}
