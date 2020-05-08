import org.jetbrains.kotlin.gradle.plugin.mpp.KotlinNativeCompilation

plugins {
    kotlin("multiplatform") version "1.3.72"
}

version = "0.0.1"
group = "de.stefannegele"

repositories {
    mavenCentral()
}

kotlin {
    //  https://kotlinlang.org/docs/reference/building-mpp-with-gradle.html#setting-up-targets
    when (System.getProperty("os.name")) {
        "Linux" -> linuxX64("native") {
            compilations.getByName("main") {
                hiredisCInterops("/usr/include/hiredis")
            }
        }
        "Mac OS X" -> macosX64("native") {
            compilations.getByName("main") {
                hiredisCInterops("/usr/local/include/hiredis")
            }
        }
    }

    sourceSets {
        val commonMain by getting {
            dependencies {
                implementation(kotlin("stdlib-common"))
                implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core-common:1.3.5")
            }
        }
        val commonTest by getting {
            dependencies {
                implementation(kotlin("test-common"))
                implementation(kotlin("test-annotations-common"))
            }
        }
        val nativeMain by getting {
            dependencies {
                implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core-native:1.3.5")
            }
        }
    }

}

fun KotlinNativeCompilation.hiredisCInterops(path: String) {
    val hiredis by cinterops.creating {
        includeDirs(path)
    }
}
