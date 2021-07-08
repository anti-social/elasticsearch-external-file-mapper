import java.util.Date
import com.carrotsearch.gradle.junit4.RandomizedTestingTask
import org.elasticsearch.gradle.VersionProperties
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

buildscript {
    val defaultEsVersion = "6.8.12"
    val esVersion = if (hasProperty("esVersion")) {
        property("esVersion")
    } else {
        defaultEsVersion
    }

    dependencies {
        classpath("org.elasticsearch.gradle:build-tools:$esVersion")
    }
}

plugins {
    idea
    java
    kotlin("jvm") version "1.3.50"
    id("org.ajoberstar.grgit") version "3.1.1"
}

apply {
    plugin("elasticsearch.esplugin")
}

subprojects {
    apply {
        plugin("java")
    }
}

configure<org.elasticsearch.gradle.plugin.PluginPropertiesExtension> {
    name = "mapper-external-file"
    description = "External file field mapper for ElasticSearch"
    classname = "company.evo.elasticsearch.plugin.mapper.ExternalFileMapperPlugin"
    licenseFile = rootProject.file("LICENSE.txt")
    noticeFile = rootProject.file("NOTICE.txt")
}

val grgit: org.ajoberstar.grgit.Grgit by extra
val tag = grgit.describe(mapOf("tags" to true, "match" to listOf("v*"))) ?: "v0.0.0"
val appVersion = tag.trimStart('v')
val versions = VersionProperties.getVersions() as Map<String, String>
project.version = "$appVersion-es${versions["elasticsearch"]}"

repositories {
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    runtime(kotlin("stdlib-jdk8"))
    compile("company.evo:persistent-hashmap")
    compile("commons-logging", "commons-logging", versions["commonslogging"])
}

tasks.withType(KotlinCompile::class.java).all {
    kotlinOptions {
        jvmTarget = JavaVersion.VERSION_1_8.toString()
    }
}

// Have no idea how to fix this better, the fix needed from kotlin plugin >= 1.1.4
tasks.withType(RandomizedTestingTask::class.java).all {
    testClassesDirs = files(File(buildDir, "classes/kotlin/test"))
  }

tasks.register("listRepos") {
    doLast {
        println("Repositories:")
        project.repositories.map{ it as MavenArtifactRepository }
            .forEach{
            println("Name: ${it.name}; url: ${it.url}")
        }
    }
}
