package company.evo.processor

import java.io.FileNotFoundException
import java.nio.file.Path
import java.nio.file.Paths
import javax.annotation.processing.*
import javax.lang.model.SourceVersion
import javax.lang.model.element.PackageElement
import javax.lang.model.element.TypeElement
import javax.tools.Diagnostic

@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.SOURCE)
annotation class KeyValueTemplate(
        val keyTypes: Array<String>,
        val valueTypes: Array<String>
)

@SupportedSourceVersion(SourceVersion.RELEASE_8)
@SupportedAnnotationTypes("company.evo.processor.KeyValueTemplate")
@SupportedOptions(
        HashMapProcessor.KAPT_KOTLIN_GENERATED_OPTION_NAME,
        HashMapProcessor.KOTLIN_SOURCE_OPTION_NAME
)
class HashMapProcessor : AbstractProcessor() {
    companion object {
        const val KAPT_KOTLIN_GENERATED_OPTION_NAME = "kapt.kotlin.generated"
        const val KOTLIN_SOURCE_OPTION_NAME = "kotlin.source"
    }

    class TemplateFile(
            val dir: Path,
            val name: String
    ) {
        val filePath = dir.resolve(name)
        val content = filePath.toFile().readText()
    }

    data class ReplaceRule(val old: String, val new: String) {
        fun apply(s: String) = s.replace(old, new)
    }

    override fun process(
            annotations: MutableSet<out TypeElement>?, roundEnv: RoundEnvironment
    ): Boolean {
        val kotlinSourceDir = processingEnv.options[KOTLIN_SOURCE_OPTION_NAME] ?: run {
            error("Missing kotlin.source option")
            return false
        }

        val kaptKotlinGeneratedDir = processingEnv.options[KAPT_KOTLIN_GENERATED_OPTION_NAME] ?: run {
            error(
                    "Can't find the target directory for generated Kotlin files"
            )
            return false
        }

        val annotatedElements = roundEnv.getElementsAnnotatedWith(KeyValueTemplate::class.java)
        for(element in annotatedElements) {
            element as? TypeElement ?: continue

            val origClassName = element.simpleName
            val (mainType, origKeyType, origValueType) = origClassName.split("_")
            val packageElement = element.enclosingElement
            if (packageElement is PackageElement) {
                val packagePath = packageElement.qualifiedName.toString().split(".").run {
                    Paths.get(
                            first(),
                            *slice(1 until size).toTypedArray()
                    )
                }
                val absPackagePath = Paths.get(kotlinSourceDir).resolve(packagePath)
                val template = try {
                    TemplateFile(absPackagePath, "${origClassName}.kt")
                } catch (ex: FileNotFoundException) {
                    continue
                }

                val keyTemplate = TemplateFile(
                        getTypeDir(absPackagePath.resolve("keyTypes"), origKeyType),
                        getTypeFileName(origKeyType)
                )
                val valueTemplate = TemplateFile(
                        getTypeDir(absPackagePath.resolve("valueTypes"), origValueType),
                        getTypeFileName(origValueType)
                )

                val annotation = element.getAnnotation(KeyValueTemplate::class.java)
                val outputDir = Paths.get(
                        kaptKotlinGeneratedDir,
                        packagePath.toString()
                )
                annotation.keyTypes.forEach { keyType ->
                    annotation.valueTypes.forEach { valueType ->
                        val origKeyValueType = "_${origKeyType}_${origValueType}"
                        val keyValueType = "_${keyType}_${valueType}"
                        val generatedFileName = "${mainType}${keyValueType}.kt"
                        generateFile(
                                outputDir = outputDir,
                                generatedFileName = generatedFileName,
                                template = template,
                                replaceRules = listOf(
                                        // Imports
                                        ReplaceRule(".keyTypes.$origKeyType.*", ".keyTypes.$keyType.*"),
                                        ReplaceRule(".valueTypes.$origValueType.*", ".valueTypes.$valueType.*"),
                                        // Hash map new
                                        ReplaceRule(origKeyValueType, keyValueType)
                                )
                        )
                        generateTypeAliasFile(
                                outputDir = getTypeDir(outputDir.resolve("keyTypes"), keyType),
                                generatedFileName = getTypeFileName(keyType),
                                template = keyTemplate,
                                origType = origKeyType,
                                type = keyType
                        )
                        generateTypeAliasFile(
                                outputDir = getTypeDir(outputDir.resolve("valueTypes"), valueType),
                                generatedFileName = getTypeFileName(valueType),
                                template = valueTemplate,
                                origType = origValueType,
                                type = valueType
                        )
                    }
                }
            }
        }

        return true
    }

    private fun generateFile(
            outputDir: Path,
            generatedFileName: String,
            template: TemplateFile,
            replaceRules: List<ReplaceRule>
    ) {
        if(template.dir.resolve(generatedFileName).toFile().exists()) {
            return
        }
        outputDir.resolve(generatedFileName).toFile().apply {
            if (exists()) {
                return
            }
            parentFile.mkdirs()
            val generatedContent = replaceRules.fold(template.content) { generatedContent, rule ->
                rule.apply(generatedContent)
            }
            writeText(
                    "// Generated from ${template.name}\n" +
                    generatedContent
            )
        }
    }

    private fun generateTypeAliasFile(
            outputDir: Path, generatedFileName: String, template: TemplateFile,
            origType: String, type: String
    ) {
        if (template.dir.resolve(generatedFileName).toFile().exists()) {
            return
        }
        outputDir.resolve(generatedFileName).toFile().apply {
            if (exists()) {
                return
            }
            parentFile.mkdirs()
            writeText(template.content.replace(origType, type))
        }
    }

    private fun getTypeFileName(type: String) = "${type.toLowerCase()}.kt"

    private fun getTypeDir(dir: Path, type: String): Path {
        return dir.resolve(type)
    }

    private fun error(message: Any) {
        processingEnv.messager.printMessage(Diagnostic.Kind.ERROR, message.toString())
    }

    private fun warn(message: Any) {
        processingEnv.messager.printMessage(Diagnostic.Kind.WARNING, message.toString())
    }
}
