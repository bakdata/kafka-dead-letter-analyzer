description = "Kafka Streams application that analyzes dead letters in your Kafka cluster"

plugins {
    `java-library`
    id("com.bakdata.release") version "1.11.1"
    id("com.bakdata.sonar") version "1.11.1"
    id("com.bakdata.sonatype") version "1.11.1"
    id("io.freefair.lombok") version "8.14.2"
    id("com.bakdata.jib") version "1.11.1"
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
    implementation(group = "io.confluent", name = "kafka-streams-avro-serde") {
        exclude(group = "org.apache.kafka", module = "kafka-clients")
    }
    implementation(group = "org.apache.kafka", name = "connect-runtime") {
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
    }
    api(platform("com.bakdata.kafka:kafka-bom:1.2.1")) // Central repository requires this as a direct dependency to resolve versions
    api(platform("com.bakdata.kafka:large-message-bom:3.1.0")) // Central repository requires this as a direct dependency to resolve versions
    api(platform("com.bakdata.kafka:error-handling-bom:2.0.0")) // Central repository requires this as a direct dependency to resolve versions
    val streamsBootstrapVersion = "5.1.0"
    api(platform("com.bakdata.kafka:streams-bootstrap-bom:$streamsBootstrapVersion"))
    api(group = "com.bakdata.kafka", name = "streams-bootstrap-large-messages")
    implementation(group = "com.bakdata.kafka", name = "streams-bootstrap-cli")
    implementation(group = "com.bakdata.kafka", name = "brute-force-serde", version = "1.4.0")
    implementation(group = "com.bakdata.kafka", name = "large-message-serde")
    implementation(group = "org.jooq", name = "jool", version = "0.9.15")
    avroApi(platform("com.bakdata.kafka:error-handling-bom:2.0.0"))
    avroApi(group = "com.bakdata.kafka", name = "error-handling-avro")
    val log4jVersion = "2.25.1"
    implementation(group = "org.apache.logging.log4j", name = "log4j-slf4j2-impl", version = log4jVersion)

    val junitVersion = "5.13.4"
    testRuntimeOnly(group = "org.junit.platform", name = "junit-platform-launcher")
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter", version = junitVersion)
    testImplementation(group = "com.bakdata.kafka", name = "streams-bootstrap-test")
    testImplementation(group = "org.assertj", name = "assertj-core", version = "3.27.6")
}

avro {
    setGettersReturnOptional(true)
    setOptionalGettersForNullableFieldsOnly(true)
    setFieldVisibility("PRIVATE")
}

jibImage {
    name.set("kafka-dead-letter-analyzer")
}
