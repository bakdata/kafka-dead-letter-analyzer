description = "Kafka Streams application that analyzes dead letters in your Kafka cluster"

plugins {
    `java-library`
    alias(libs.plugins.release)
    alias(libs.plugins.sonar)
    alias(libs.plugins.sonatype)
    alias(libs.plugins.lombok)
    alias(libs.plugins.jib)
    alias(libs.plugins.avro)
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
    api(platform(libs.kafka.bom)) // Central repository requires this as a direct dependency to resolve versions
    api(platform(libs.largeMessage.bom)) // Central repository requires this as a direct dependency to resolve versions
    api(platform(libs.streamsBootstrap.bom))
    api(libs.streamsBootstrap.largeMessage)
    avroApi(platform(libs.errorHandling.bom))
    avroApi(libs.errorHandling.avro)
    implementation(libs.kafka.streams.avro.serde) {
        exclude(group = "org.apache.kafka", module = "kafka-clients")
    }
    implementation(libs.kafka.connect.runtime) {
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
    }
    implementation(libs.streamsBootstrap.cli)
    implementation(libs.bruteForce.serde)
    implementation(libs.largeMessage.serde)
    implementation(libs.jool)
    implementation(libs.log4j.slf4j2)

    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.streamsBootstrap.test)
    testImplementation(libs.assertj)
}

avro {
    setGettersReturnOptional(true)
    setOptionalGettersForNullableFieldsOnly(true)
    setFieldVisibility("PRIVATE")
}

jibImage {
    name.set("kafka-dead-letter-analyzer")
}
