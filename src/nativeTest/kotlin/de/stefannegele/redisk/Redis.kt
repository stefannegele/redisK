package de.stefannegele.redisk

import de.stefannegele.redisk.context.PooledConnectionRedisContext
import de.stefannegele.redisk.context.SingleConnectionRedisContext
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertFailsWith

private const val CUSTOM_CONTEXT_NAME = "some custom context"

class RedisTest {

    @Test
    fun `withRedisContext only works when connection is setup`() = runBlocking {
        assertFailsWith<IllegalArgumentException> { withRedisContext {} }

        connectRedis()
        withRedisContext {}

        assertFailsWith<IllegalArgumentException> { withRedisContext(CUSTOM_CONTEXT_NAME) {} }

        connectRedis(CUSTOM_CONTEXT_NAME)
        withRedisContext(CUSTOM_CONTEXT_NAME) {}

        disconnectRedis()
        disconnectRedis(CUSTOM_CONTEXT_NAME)
    }

    @Test
    fun `connectRedis only works once with one name`() = runBlocking {
        connectRedis()
        connectRedis(CUSTOM_CONTEXT_NAME)

        assertFailsWith<IllegalArgumentException> { connectRedis() }
        assertFailsWith<IllegalArgumentException> { connectRedis(CUSTOM_CONTEXT_NAME) }

        disconnectRedis()
        disconnectRedis(CUSTOM_CONTEXT_NAME)
    }

    @Test
    fun `connectRedis creates a single connection context per default`() = runBlocking {
        connectRedis()
        withRedisContext {
            assertEquals(this::class, SingleConnectionRedisContext::class)
        }
        disconnectRedis()
    }

    @Test
    fun `connectRedis creates a pooled connection context if pool size is higher than 1`() = runBlocking {
        connectRedis { connectionPoolSize = 2 }
        withRedisContext {
            assertEquals(this::class, PooledConnectionRedisContext::class)
        }
        disconnectRedis()
    }

    @Test
    fun `connectRedis throws an exception if pool size is smaller than 1`() = runBlocking<Unit> {
        assertFailsWith<IndexOutOfBoundsException> { connectRedis { connectionPoolSize = 0 } }
        assertFailsWith<IndexOutOfBoundsException> { connectRedis { connectionPoolSize = -1 } }
    }

    @Test
    fun `disconnectRedis frees a redis context`() = runBlocking {
        connectRedis()

        assertFails { connectRedis() }

        disconnectRedis()

        connectRedis()

        disconnectRedis()
    }

    @Test
    fun `disconnectRedis only frees its own redis context`() = runBlocking {
        connectRedis()
        connectRedis(CUSTOM_CONTEXT_NAME)
        disconnectRedis(CUSTOM_CONTEXT_NAME)

        assertFailsWith<IllegalArgumentException> { connectRedis() }

        disconnectRedis()
    }

}
