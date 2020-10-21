import java.util.Date
import com.carrotsearch.gradle.junit4.RandomizedTestingTask
import com.jfrog.bintray.gradle.BintrayExtension
import com.jfrog.bintray.gradle.tasks.RecordingCopyTask
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
    id("com.jfrog.bintray") version "1.8.4"
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

val appVersion =  project.file("project.version")
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
project.version = "$appVersion-es${versions["elasticsearch"]}"

repositories {
    mavenCentral()
    maven {
        url = uri("https://dl.bintray.com/evo/maven")
    }
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

bintray {
    user = if (hasProperty("bintrayUser")) {
        property("bintrayUser").toString()
    } else {
        System.getenv("BINTRAY_USER")
    }
    key = if (hasProperty("bintrayApiKey")) {
        property("bintrayApiKey").toString()
    } else {
        System.getenv("BINTRAY_API_KEY")
    }
    pkg(delegateClosureOf<BintrayExtension.PackageConfig> {
        repo = "elasticsearch"
        name = project.name
        userOrg = "evo"
        setLicenses("Apache-2.0")
        setLabels("elasticsearch-plugin", "external-file-mapper")
        vcsUrl = "https://github.com/anti-social/elasticsearch-external-field-mapper.git"
        version(delegateClosureOf<BintrayExtension.VersionConfig> {
            name = appVersion
            released = Date().toString()
            vcsTag = "v$appVersion"
        })
    })
    filesSpec(delegateClosureOf<RecordingCopyTask> {
        val distributionsDir = buildDir.resolve("distributions")
        from(distributionsDir)
        include("*-$appVersion-*.zip")
        into(".")
    })
    publish = true
    dryRun = hasProperty("bintrayDryRun")
}
