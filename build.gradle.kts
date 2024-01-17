description = "Kafka Streams application that analyzes dead letters in your Kafka cluster"

plugins {
    `java-library`
    id("net.researchgate.release") version "3.0.2"
    id("com.bakdata.sonar") version "1.1.9"
    id("com.bakdata.sonatype") version "1.1.9"
    id("org.hildan.github.changelog") version "1.12.1"
    id("io.freefair.lombok") version "6.6.3"
    id("com.google.cloud.tools.jib") version "3.4.0"
    id("com.bakdata.avro") version "1.3.1"
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
        maven(url = "https://oss.sonatype.org/content/repositories/snapshots") //TODO remove
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
    val streamsBootstrapVersion = "2.15.3"
    api(group = "com.bakdata.kafka", name = "streams-bootstrap-large-messages", version = streamsBootstrapVersion)
    implementation(group = "com.bakdata.kafka", name = "brute-force-serde", version = "1.2.1-SNAPSHOT") //TODO
    implementation(group = "com.bakdata.kafka", name = "large-message-serde", version = "2.6.0")
    implementation(group = "org.jooq", name = "jool", version = "0.9.14")
    avroApi(group = "com.bakdata.kafka", name = "error-handling-avro", version = "1.4.2")

    val junitVersion = "5.10.1"
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junitVersion)
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junitVersion)
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-params", version = junitVersion)
    testImplementation(group = "com.bakdata.kafka", name = "streams-bootstrap-test", version = streamsBootstrapVersion)
    testImplementation(group = "org.assertj", name = "assertj-core", version = "3.25.1")
}

avro {
    setGettersReturnOptional(true)
    setOptionalGettersForNullableFieldsOnly(true)
    setFieldVisibility("PRIVATE")
}
