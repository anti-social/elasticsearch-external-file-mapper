import java.util.Date
import com.carrotsearch.gradle.junit4.RandomizedTestingTask
import com.jfrog.bintray.gradle.BintrayExtension
import com.jfrog.bintray.gradle.RecordingCopyTask
import org.elasticsearch.gradle.VersionProperties
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

buildscript {
    val defaultEsVersion = "6.5.4"
    val esVersion = if (hasProperty("esVersion")) {
        property("esVersion")
    } else {
        defaultEsVersion
    }

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
    id("com.jfrog.bintray") version "1.7.3"
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
}

project.setProperty("licenseFile", project.file("LICENSE.txt"))
project.setProperty("noticeFile", project.file("NOTICE.txt"))

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
}

dependencies {
    compile(kotlin("stdlib"))
    implementation(project(":library"))
    compile("org.apache.httpcomponents", "httpcore", versions["httpcore"])
    compile("org.apache.httpcomponents", "httpclient", versions["httpclient"])
    compile("commons-logging", "commons-logging", versions["commonslogging"])
    compile("net.sf.trove4j", "trove4j", "3.0.3")
    compile("org.agrona", "agrona", "0.9.33")
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

val testFunc by tasks.creating(Test::class) {
    include("**/*TestCase.class")
}
testFunc.outputs.upToDateWhen { false }

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
