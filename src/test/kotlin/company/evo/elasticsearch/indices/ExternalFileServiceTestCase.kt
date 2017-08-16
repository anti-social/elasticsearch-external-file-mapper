package company.evo.elasticsearch.index.mapper.external

import java.nio.file.Files

import org.junit.Test
import org.junit.Assert

import org.elasticsearch.common.settings.Settings
import org.elasticsearch.env.Environment

import company.evo.elasticsearch.indices.ExternalFileService


class ExternalFileServiceTestCase : Assert() {
    @Test
    fun testDownload() {
        val indexName = "test"
        val fieldName = "ext_price"
        val fileUrl = "http://localhost:8080/ext_price.txt"
        val settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.key, createTempDir())
                .put(Environment.PATH_DATA_SETTING.key, createTempDir())
                .build()
        val env = Environment(settings)
        val extFileService = ExternalFileService(env.dataFiles()[0], 0)
        Files.createDirectories(extFileService.getExternalFileDir(indexName))

        var downloaded = extFileService.download(
                indexName, fieldName, fileUrl)
        assert(downloaded)
        extFileService.tryLoad(indexName, fieldName)
        val values = extFileService.getValues(indexName, fieldName)
        assertEquals(3, values.size)
        assertEquals(1.1, values["1"])
        assertEquals(1.2, values["2"])
        assertEquals(1.3, values["3"])

        downloaded = extFileService.download(indexName, fieldName, fileUrl)
        assertFalse(downloaded)

        extFileService.updateVersion(indexName, fieldName, "Thu, 01 Jan 1970 00:00:01 GMT")
        downloaded = extFileService.download(indexName, fieldName, fileUrl)
        assert(downloaded)
    }
}
