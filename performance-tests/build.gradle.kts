plugins {
  id("java")
  id("com.diffplug.spotless") version "6.25.0"
}

spotless {
  java {
    googleJavaFormat()
    licenseHeaderFile(rootProject.file("../scripts/spotless.modify_license.java"), "(package|import|public)")
            .named("modify_license")
            .onlyIfContentMatches("!.*RuntimeUtil.java")
    licenseHeaderFile(rootProject.file("../scripts/spotless.license.java"), "(package|import|public)")
            .named("license")
            .onlyIfContentMatches(".*RuntimeUtil.java")
    target("src/**/*.java")
  }
}

repositories {
  mavenCentral()
}

dependencies {
  implementation(enforcedPlatform("org.junit:junit-bom:5.10.2"))

  testImplementation("org.testcontainers:testcontainers:1.19.6")
  testImplementation("org.testcontainers:postgresql:1.19.6")
  testImplementation("org.junit.jupiter:junit-jupiter-api")
  testImplementation("org.junit.jupiter:junit-jupiter-params")
  testImplementation("com.squareup.okhttp3:okhttp:4.12.0")
  testImplementation("org.jooq:joox:2.0.0")
  testImplementation("com.jayway.jsonpath:json-path:2.9.0")
  testImplementation("org.slf4j:slf4j-simple:2.0.12")

  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

tasks {
  test {
    useJUnitPlatform()
  }
}
