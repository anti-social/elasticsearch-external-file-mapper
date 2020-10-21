import com.github.erizo.gradle.JcstressPluginExtension
import com.jfrog.bintray.gradle.BintrayExtension
import java.util.Date
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

buildscript {
    repositories {
        mavenCentral()
    }
    dependencies {
        classpath("com.github.erizo.gradle:jcstress-gradle-plugin:0.8.3")
    }
}

plugins {
    java
    `maven-publish`
    kotlin("jvm") version "1.3.50"
    kotlin("kapt") version "1.3.50"
    id("org.ajoberstar.grgit") version "3.1.1"
    id("com.jfrog.bintray") version "1.8.4"
}

apply {
    plugin("jcstress")
}

val grgit: org.ajoberstar.grgit.Grgit by extra
val tag = grgit.describe(mapOf("match" to listOf("v*"))) ?: "v0.0.0"

group = "company.evo"
version = tag.trimStart('v')

repositories {
    mavenCentral()
    maven("https://dl.bintray.com/devexperts/Maven/")
}

dependencies {
    val kotlintestVersion = "3.1.11"
    val lincheckVersion = "2.0"

    compile(kotlin("stdlib-jdk8"))
    compile(kotlin("reflect"))

    compileOnly(project(":processor"))
    kapt(project(":processor"))

    testImplementation("io.kotlintest", "kotlintest-core", kotlintestVersion)
    testImplementation("io.kotlintest", "kotlintest-assertions", kotlintestVersion)
    testImplementation("io.kotlintest", "kotlintest-runner-junit5", kotlintestVersion)
    testImplementation("com.devexperts.lincheck", "lincheck", lincheckVersion)
    testImplementation("commons-io", "commons-io", "2.6")
}

configure<JavaPluginConvention> {
    sourceCompatibility = JavaVersion.VERSION_1_8
}
tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = JavaVersion.VERSION_1_8.toString()
}

val test by tasks.getting(Test::class) {
    properties["seed"]?.let {
        systemProperties["test.random.seed"] = it
    }
    useJUnitPlatform()
    outputs.upToDateWhen { false }
}

configure<JcstressPluginExtension> {
    jcstressDependency = "org.openjdk.jcstress:jcstress-core:0.4"
}

kapt {
    arguments {
        arg("kotlin.source", kotlin.sourceSets["main"].kotlin.srcDirs.first())
    }
}

publishing {
    publications {
        create<MavenPublication>("mavenJar") {
            groupId = "company.evo"
            artifactId = "persistent-hashmap"
            version = project.version.toString()

            from(components["java"])
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
    setPublications("mavenJar")

    pkg(closureOf<BintrayExtension.PackageConfig> {
        userOrg = "evo"
        repo = "maven"
        name = project.name
        setLicenses("Apache-2.0")
        setLabels("persistent", "datastructures", "hashmap")
        vcsUrl = "https://github.com/anti-social/persistent-hashmap"
        version(closureOf<BintrayExtension.VersionConfig> {
            name = project.version.toString()
            released = Date().toString()
            vcsTag = tag
        })
        publish = true
        dryRun = hasProperty("bintrayDryRun")
    })
}