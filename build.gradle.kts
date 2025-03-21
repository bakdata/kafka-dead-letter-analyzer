description = "Kafka Streams application that analyzes dead letters in your Kafka cluster"

plugins {
    `java-library`
    id("com.bakdata.release") version "1.7.1"
    id("com.bakdata.sonar") version "1.7.1"
    id("com.bakdata.sonatype") version "1.9.0"
    id("io.freefair.lombok") version "8.12.2.1"
    id("com.bakdata.jib") version "1.7.1"
    id("com.bakdata.avro") version "1.5.0"
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
        maven(url = "https://s01.oss.sonatype.org/content/repositories/snapshots")
    }
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

publication {
    developers {
        developer {
            name.set("Philipp Schirmer")
            id.set("philipp94831")
        }
    }
}


dependencies {
    implementation(group = "io.confluent", name = "kafka-streams-avro-serde")
    implementation(group = "org.apache.kafka", name = "connect-runtime") {
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
    }
    val streamsBootstrapVersion = "4.1.0"
    api(platform("com.bakdata.kafka:streams-bootstrap-bom:$streamsBootstrapVersion"))
    api(group = "com.bakdata.kafka", name = "streams-bootstrap-large-messages")
    implementation(group = "com.bakdata.kafka", name = "streams-bootstrap-cli")
    implementation(group = "com.bakdata.kafka", name = "brute-force-serde", version = "1.4.0")
    implementation(group = "com.bakdata.kafka", name = "large-message-serde")
    implementation(group = "org.jooq", name = "jool", version = "0.9.15")
    avroApi(platform("com.bakdata.kafka:error-handling-bom:1.8.0"))
    avroApi(group = "com.bakdata.kafka", name = "error-handling-avro")
    val log4jVersion = "2.24.3"
    implementation(group = "org.apache.logging.log4j", name = "log4j-slf4j2-impl", version = log4jVersion)

    val junitVersion = "5.11.4"
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junitVersion)
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junitVersion)
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-params", version = junitVersion)
    testImplementation(group = "com.bakdata.kafka", name = "streams-bootstrap-test")
    testImplementation(group = "org.assertj", name = "assertj-core", version = "3.27.2")
}

avro {
    setGettersReturnOptional(true)
    setOptionalGettersForNullableFieldsOnly(true)
    setFieldVisibility("PRIVATE")
}

jibImage {
    name.set("kafka-dead-letter-analyzer")
}
