import com.carrotsearch.gradle.junit4.RandomizedTestingTask
import org.elasticsearch.gradle.VersionProperties
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

buildscript {
    val esVersion = System.getProperty("esVersion", "6.2.3")

    repositories {
        mavenCentral()
        jcenter()
    }
    dependencies {
        classpath("org.elasticsearch.gradle:build-tools:$esVersion")
    }
}

plugins {
    idea
    java
    kotlin("jvm") version "1.2.31"
}

apply {
    plugin("elasticsearch.esplugin")
}

configure<org.elasticsearch.gradle.plugin.PluginPropertiesExtension> {
    name = "mapper-external-file"
    description = "External file field mapper for ElasticSearch"
    classname = "company.evo.elasticsearch.plugin.mapper.ExternalFileMapperPlugin"
}

project.setProperty("licenseFile", project.file("LICENSE.txt"))
project.setProperty("noticeFile", project.file("NOTICE.txt"))

val version =  project.file("project.version")
        .readLines()
        .first()
        .toUpperCase()
        .let { ver ->
            if (hasProperty("release")) {
                ver.removeSuffix("-SNAPSHOT")
            } else {
                ver
            }
        }
val versions = VersionProperties.getVersions() as Map<String, String>
project.version = "$version-es${versions["elasticsearch"]}"

repositories {
    mavenCentral()
}

dependencies {
    compile(kotlin("stdlib"))
    compile("org.apache.httpcomponents", "httpcore", versions["httpcore"])
    compile("org.apache.httpcomponents", "httpclient", versions["httpclient"])
    compile("commons-logging", "commons-logging", versions["commonslogging"])
    compile("net.sf.trove4j", "trove4j", "3.0.3")
}

tasks.withType(KotlinCompile::class.java).all {
    kotlinOptions {
        jvmTarget = JavaVersion.VERSION_1_8.toString()
    }
}

// Have no idea how to fix this better, the fix needed from kotlin plugin >= 1.1.4
val kotlinTestClassesDir = File(buildDir, "classes/kotlin/test")
run {
    val test by java.sourceSets
    test.output.classesDir = kotlinTestClassesDir
}
tasks.withType(RandomizedTestingTask::class.java).all {
    testClassesDir = kotlinTestClassesDir
}

val testFunc by tasks.creating(Test::class) {
    include("**/*TestCase.class")
}
testFunc.outputs.upToDateWhen { false }

