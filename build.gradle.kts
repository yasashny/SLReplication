plugins {
    kotlin("jvm") version "2.1.20"
    kotlin("plugin.serialization") version "2.1.20"
    application
}

group = "com.yasashny"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.6.3")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.0")
    implementation("com.github.ajalt.clikt:clikt:4.2.2")
    implementation("org.slf4j:slf4j-api:2.0.12")
    implementation("org.slf4j:slf4j-simple:2.0.12")
    testImplementation(kotlin("test"))
}

kotlin {
    jvmToolchain(21)
}

tasks.withType<Test> {
    useJUnitPlatform()
}

tasks.register<Jar>("nodeJar") {
    archiveBaseName.set("kv-node")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    manifest {
        attributes["Main-Class"] = "com.yasashny.slreplication.node.NodeMainKt"
    }
    from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
    with(tasks.jar.get())
}

tasks.register<Jar>("cliJar") {
    archiveBaseName.set("kv-cli")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    manifest {
        attributes["Main-Class"] = "com.yasashny.slreplication.cli.CliMainKt"
    }
    from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
    with(tasks.jar.get())
}

tasks.register<JavaExec>("runNode") {
    group = "application"
    mainClass.set("com.yasashny.slreplication.node.NodeMainKt")
    classpath = sourceSets["main"].runtimeClasspath
    standardInput = System.`in`
}

tasks.register<JavaExec>("runCli") {
    group = "application"
    mainClass.set("com.yasashny.slreplication.cli.CliMainKt")
    classpath = sourceSets["main"].runtimeClasspath
    standardInput = System.`in`
}

tasks.register<JavaExec>("simpleTest") {
    group = "verification"
    mainClass.set("com.yasashny.slreplication.SimpleTestKt")
    classpath = sourceSets["test"].runtimeClasspath + sourceSets["main"].runtimeClasspath
}

tasks.register<JavaExec>("asyncTest") {
    group = "verification"
    mainClass.set("com.yasashny.slreplication.AsyncTestKt")
    classpath = sourceSets["test"].runtimeClasspath + sourceSets["main"].runtimeClasspath
}
