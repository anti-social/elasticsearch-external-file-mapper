import java.nio.file.Paths

buildscript {
    // val defaultEsVersion = "6.8.12"
    val defaultEsVersion = "7.9.3"
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
    id("nebula.ospackage") version "8.5.6"
}

apply {
    plugin("elasticsearch.esplugin")
}

subprojects {
    apply {
        plugin("java")
    }
}

val pluginName = "mapper-external-file"

configure<org.elasticsearch.gradle.plugin.PluginPropertiesExtension> {
    name = pluginName
    description = "External file field mapper for ElasticSearch"
    classname = "company.evo.elasticsearch.plugin.mapper.ExternalFileMapperPlugin"
    licenseFile = rootProject.file("LICENSE.txt")
    noticeFile = rootProject.file("NOTICE.txt")
}

val grgit: org.ajoberstar.grgit.Grgit? by extra
val tag = grgit?.describe(mapOf("tags" to true, "match" to listOf("v*"))) ?: "v0.0.0"
version = tag.trimStart('v')
val esVersions = org.elasticsearch.gradle.VersionProperties.getVersions() as Map<String, String>

val distDir = Paths.get(buildDir.path, "distributions")

repositories {
    mavenCentral()
}

dependencies {
    compile(kotlin("stdlib-jdk8"))
    compile("company.evo:persistent-hashmap")
    compile("commons-logging", "commons-logging", esVersions["commonslogging"])
}

tasks.withType(org.jetbrains.kotlin.gradle.tasks.KotlinCompile::class.java).all {
    kotlinOptions {
        jvmTarget = JavaVersion.VERSION_1_8.toString()
    }
}

tasks.register("listRepos") {
    doLast {
        println("Repositories:")
        project.repositories
            .forEach{ repo ->
                val repoUrl = when (repo) {
                    is MavenArtifactRepository -> repo.url.toString()
                    is IvyArtifactRepository -> repo.url.toString()
                    else -> "???"
                }
                println("Name: ${repo.name}; url: $repoUrl")
            }
    }
}

tasks.named("assemble") {
    dependsOn("deb")
}

tasks.register("deb", com.netflix.gradle.plugins.deb.Deb::class) {
    dependsOn("bundlePlugin")

    packageName = "elasticsearch-${pluginName}-plugin"
    requires("elasticsearch", esVersions["elasticsearch"])
        .or("elasticsearch-oss", esVersions["elasticsearch"])

    from(zipTree(tasks["bundlePlugin"].outputs.files.singleFile))

    val esHome = project.properties["esHome"] ?: "/usr/share/elasticsearch"
    into("$esHome/plugins/${pluginName}")

    doLast {
        if (properties.containsKey("assembledInfo")) {
            distDir.resolve("assembled-deb.filename").toFile()
                .writeText(assembleArchiveName())
        }
    }
}
