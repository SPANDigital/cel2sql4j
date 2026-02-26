plugins {
    java
    `java-library`
    id("com.vanniktech.maven.publish") version "0.30.0"
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
    api("dev.cel:cel:0.9.1")
    api("dev.cel:compiler:0.9.1")
    api("dev.cel:runtime:0.9.1")
    implementation("com.google.guava:guava:33.4.0-jre")
    implementation("org.slf4j:slf4j-api:2.0.16")

    testImplementation("org.junit.jupiter:junit-jupiter:5.11.4")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.assertj:assertj-core:3.27.3")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.16")

    "integrationTestImplementation"(platform("org.testcontainers:testcontainers-bom:1.20.4"))
    "integrationTestImplementation"("org.testcontainers:junit-jupiter")
    "integrationTestImplementation"("org.testcontainers:postgresql")
    "integrationTestImplementation"("org.testcontainers:mysql")
    "integrationTestImplementation"("org.postgresql:postgresql:42.7.5")
    "integrationTestImplementation"("com.mysql:mysql-connector-j:9.2.0")
    "integrationTestImplementation"("org.xerial:sqlite-jdbc:3.47.2.0")
    "integrationTestImplementation"("org.duckdb:duckdb_jdbc:1.1.3")
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
    publishToMavenCentral(com.vanniktech.maven.publish.SonatypeHost.CENTRAL_PORTAL)
}
