import net.researchgate.release.GitAdapter.GitConfig
import net.researchgate.release.ReleaseExtension

description = "Kafka Streams application that analyzes dead letters in your Kafka cluster"

plugins {
    `java-library`
    id("net.researchgate.release") version "2.8.1"
    id("com.bakdata.sonar") version "1.1.7"
    id("com.bakdata.sonatype") version "1.1.7"
    id("org.hildan.github.changelog") version "0.8.0"
    id("io.freefair.lombok") version "6.4.1"
    id("com.google.cloud.tools.jib") version "3.2.0"
    id("com.bakdata.avro") version "1.0.1"
}

allprojects {
    group = "com.bakdata.kafka"

    tasks.withType<Test> {
        maxParallelForks = 4
        useJUnitPlatform()
    }

    repositories {
        mavenCentral()
        maven(url = "https://packages.confluent.io/maven/")
    }
}

configure<JavaPluginExtension> {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

configure<com.bakdata.gradle.SonatypeSettings> {
    developers {
        developer {
            name.set("Philipp Schirmer")
            id.set("philipp94831")
        }
    }
}

configure<org.hildan.github.changelog.plugin.GitHubChangelogExtension> {
    githubUser = "bakdata"
    githubRepository = "kafka-dead-letter-analyzer"
    futureVersionTag = findProperty("changelog.releaseVersion")?.toString()
    sinceTag = findProperty("changelog.sinceTag")?.toString()
}

dependencies {
    val confluentVersion: String by project
    implementation(group = "io.confluent", name = "kafka-streams-avro-serde", version = confluentVersion)
    val kafkaVersion: String by project
    implementation(group = "org.apache.kafka", name = "connect-runtime", version = kafkaVersion) {
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
    }
    api(group = "com.bakdata.kafka", name = "streams-bootstrap", version = "2.2.0")
    implementation(group = "com.bakdata.kafka", name = "brute-force-serde", version = "1.2.0")
    implementation(group = "com.bakdata.kafka", name = "large-message-serde", version = "2.4.0")
    implementation(group = "org.jooq", name = "jool", version = "0.9.14")
    avroApi(group = "com.bakdata.kafka", name = "error-handling-avro", version = "1.3.0")

    val junitVersion = "5.7.2"
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junitVersion)
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junitVersion)
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-params", version = junitVersion)
    testImplementation(
        group = "com.bakdata.fluent-kafka-streams-tests",
        name = "fluent-kafka-streams-tests-junit5",
        version = "2.6.0"
    )
    testImplementation(group = "org.assertj", name = "assertj-core", version = "3.20.2")
}

avro {
    setGettersReturnOptional(true)
    setOptionalGettersForNullableFieldsOnly(true)
    setFieldVisibility("PRIVATE")
}

fun ReleaseExtension.git(configure: GitConfig.() -> Unit) = (getProperty("git") as GitConfig).configure()

release {
    git {
        requireBranch = "main"
    }
}
