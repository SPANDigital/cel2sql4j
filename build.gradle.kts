plugins {
    java
    `java-library`
    id("com.vanniktech.maven.publish") version "0.36.0"
}

group = property("GROUP") as String
version = property("VERSION_NAME") as String

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

repositories {
    mavenCentral()
}

sourceSets {
    create("integrationTest") {
        compileClasspath += sourceSets.main.get().output + sourceSets.test.get().output
        runtimeClasspath += sourceSets.main.get().output + sourceSets.test.get().output
    }
}

configurations["integrationTestImplementation"].extendsFrom(configurations.testImplementation.get())
configurations["integrationTestRuntimeOnly"].extendsFrom(configurations.testRuntimeOnly.get())

dependencies {
    api("dev.cel:cel:0.12.0")
    api("dev.cel:compiler:0.12.0")
    api("dev.cel:runtime:0.12.0")
    implementation("com.google.guava:guava:33.6.0-jre")
    implementation("org.slf4j:slf4j-api:2.0.18")

    testImplementation("org.junit.jupiter:junit-jupiter:6.1.0")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.assertj:assertj-core:3.27.7")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.18")

    "integrationTestImplementation"(platform("org.testcontainers:testcontainers-bom:2.0.5"))
    "integrationTestImplementation"("org.testcontainers:testcontainers-junit-jupiter")
    "integrationTestImplementation"("org.testcontainers:testcontainers-postgresql")
    "integrationTestImplementation"("org.testcontainers:testcontainers-mysql")
    "integrationTestImplementation"("org.postgresql:postgresql:42.7.11")
    "integrationTestImplementation"("com.mysql:mysql-connector-j:9.7.0")
    "integrationTestImplementation"("org.xerial:sqlite-jdbc:3.53.1.0")
    "integrationTestImplementation"("org.duckdb:duckdb_jdbc:1.5.2.1")
}

tasks.test {
    useJUnitPlatform()
}

tasks.register<Test>("integrationTest") {
    description = "Runs integration tests against real databases."
    group = "verification"
    testClassesDirs = sourceSets["integrationTest"].output.classesDirs
    classpath = sourceSets["integrationTest"].runtimeClasspath
    useJUnitPlatform()
    shouldRunAfter(tasks.test)
}

tasks.withType<Javadoc> {
    (options as StandardJavadocDocletOptions).addStringOption("Xdoclint:none", "-quiet")
}

mavenPublishing {
    publishToMavenCentral()
}
