package company.evo.extfile

import java.io.File
import java.nio.file.Files
import java.nio.file.Path

import io.kotlintest.TestCaseContext
import io.kotlintest.matchers.*
import io.kotlintest.specs.StringSpec


fun shouldNotBeNull(): Nothing {
    throw AssertionError("Value should not be null")
}

class ExternalFileTests : StringSpec() {
    lateinit private var tempFolder: Path

    private val withTempFolder: (TestCaseContext, () -> Unit) -> Unit = { _, test ->
        this.tempFolder = Files.createTempDirectory("external-file-tests-")
        try {
            test()
        } finally {
            recursiveDelete(tempFolder.toFile())
        }
    }

    init {
        "test trove backend" {
            val indexName = "test"
            val fieldName = "ext_price"
            val fileUrl = "http://localhost:8088/ext_price.txt"
            val fileUpdater = ExternalFile(
                    tempFolder,
                    fieldName,
                    indexName,
                    FileSettings(FileValues.Backend.TROVE, 60, null, null, fileUrl, null)
            )

            var downloaded = fileUpdater.download()
            downloaded shouldBe true
            val values = fileUpdater.loadValues(null)?.values
            values ?: shouldNotBeNull()

            values.get(1, 0.0) shouldBe 1.1
            values.get(2, 0.0) shouldBe 1.2
            values.get(3, 0.0) shouldBe 1.3
            values.get(4, 0.0) shouldBe 0.0

            downloaded = fileUpdater.download()
            downloaded shouldBe false

            fileUpdater.updateVersion("Thu, 01 Jan 1970 00:00:01 GMT")
            downloaded = fileUpdater.download()
            downloaded shouldBe true
        }.config(interceptors = listOf(withTempFolder))

        "test chronicle map backend" {
            val indexName = "test"
            val fieldName = "ext_price"
            val fileUrl = "http://localhost:8080/ext_price.txt"
            val fileUpdater = ExternalFile(
                    tempFolder,
                    fieldName,
                    indexName,
                    FileSettings(FileValues.Backend.CHRONICLE, 60, null, null, fileUrl, null))

            var downloaded = fileUpdater.download()
            assert(downloaded)
            val values = fileUpdater.loadValues(null)?.values
            values ?: shouldNotBeNull()

            values.get(1, 0.0) shouldBe 1.1
            values.get(2, 0.0) shouldBe 1.2
            values.get(3, 0.0) shouldBe 1.3
            values.get(4, 0.0) shouldBe 0.0

            downloaded = fileUpdater.download()
            downloaded shouldBe false

            fileUpdater.updateVersion("Thu, 01 Jan 1970 00:00:01 GMT")
            downloaded = fileUpdater.download()
            downloaded shouldBe true
        }.config(interceptors = listOf(withTempFolder), enabled = false)
    }

    private fun recursiveDelete(dir: File) {
        dir.listFiles()?.forEach {
            recursiveDelete(it)
        }
        dir.delete()
    }

//    @Rule @JvmField
//    val tempFolder: TemporaryFolder = TemporaryFolder()

}
