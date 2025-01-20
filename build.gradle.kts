import com.bakdata.gradle.BakdataJibExtension

buildscript {
    repositories {
        maven {
            url = uri("https://s01.oss.sonatype.org/content/repositories/snapshots")
        }
    }
    dependencies {
        classpath("com.bakdata.gradle:jib:1.4.3-SNAPSHOT")
    }
}

description = "Kafka Streams application that analyzes dead letters in your Kafka cluster"

plugins {
    `java-library`
    id("com.bakdata.release") version "1.4.0"
    id("com.bakdata.sonar") version "1.4.2"
    id("com.bakdata.sonatype") version "1.4.1"
    id("io.freefair.lombok") version "8.4"
    id("com.bakdata.avro") version "1.4.0"
}
apply(plugin = "com.bakdata.jib")

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
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

configure<com.bakdata.gradle.SonatypeSettings> {
    developers {
        developer {
            name.set("Philipp Schirmer")
            id.set("philipp94831")
        }
    }
}


dependencies {
    val confluentVersion: String by project
    implementation(group = "io.confluent", name = "kafka-streams-avro-serde", version = confluentVersion)
    val kafkaVersion: String by project
    implementation(group = "org.apache.kafka", name = "connect-runtime", version = kafkaVersion) {
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
    }
    val streamsBootstrapVersion = "3.4.0"
    api(
        group = "com.bakdata.kafka",
        name = "streams-bootstrap-large-messages",
        version = streamsBootstrapVersion
    )
    implementation(group = "com.bakdata.kafka", name = "streams-bootstrap-cli", version = streamsBootstrapVersion)
    implementation(group = "com.bakdata.kafka", name = "brute-force-serde", version = "1.3.0")
    implementation(group = "com.bakdata.kafka", name = "large-message-serde", version = "2.9.1")
    implementation(group = "org.jooq", name = "jool", version = "0.9.15")
    avroApi(group = "com.bakdata.kafka", name = "error-handling-avro", version = "1.6.0")
    val log4jVersion = "2.24.3"
    implementation(group = "org.apache.logging.log4j", name = "log4j-slf4j2-impl", version = log4jVersion)

    val junitVersion = "5.11.4"
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junitVersion)
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junitVersion)
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-params", version = junitVersion)
    testImplementation(group = "com.bakdata.kafka", name = "streams-bootstrap-test", version = streamsBootstrapVersion)
    testImplementation(group = "org.assertj", name = "assertj-core", version = "3.27.2")
}

avro {
    setGettersReturnOptional(true)
    setOptionalGettersForNullableFieldsOnly(true)
    setFieldVisibility("PRIVATE")
}

configure<BakdataJibExtension> {
    imageName.set("kafka-dead-letter-analyzer")
}
