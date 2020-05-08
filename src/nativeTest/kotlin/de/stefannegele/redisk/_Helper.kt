package de.stefannegele.redisk

import de.stefannegele.redisk.context.RedisContext
import kotlinx.coroutines.runBlocking
import kotlin.random.Random

private const val TEST_CONTEXT = "redis test context"

fun testWithRedisContext(name: String = TEST_CONTEXT + Random.nextInt(), action: suspend RedisContext.() -> Unit) =
    runBlocking {
        connectRedis(name) { connectionPoolSize = 8 }
        withRedisContext(name, action)
        disconnectRedis(name)
    }
