package de.stefannegele.redisk.context

import hiredis.redisCommand
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class PooledConnectionRedisContextTest {

    @Test
    fun `withHiredisContext allows to perform commands on a hiredis context`() = runBlocking<Unit> {
        val configuration = RedisContextConfiguration()
        val pool = RedisConnectionPool(configuration)
        val context = PooledConnectionRedisContext(pool)

        context.withHiredisContext {
            redisCommand(it, "SET foo bar")
        }
    }

}

class RedisConnectionPoolTest {

    @Test
    fun `getHiredisContext returns a hiredis context or suspends if all are in use`() = runBlocking {
        val configuration = RedisContextConfiguration()
        val pool = RedisConnectionPool(configuration)

        val context = pool.getHiredisContext()
        pool.remove(context)

        val result = async { pool.getHiredisContext() }

        assertFalse { result.isCompleted }

        pool.add(context)

        result.await()

        assertTrue("this must be reached") { true }
    }

    @Test
    fun `clear cleans the pool and allows new creations`() = runBlocking {
        val configuration = RedisContextConfiguration()
        val pool = RedisConnectionPool(configuration)

        val context = pool.getHiredisContext()
        pool.remove(context)

        val result = async { pool.getHiredisContext() }

        assertFalse { result.isCompleted }

        pool.clear()

        result.await()

        assertTrue("this must be reached") { true }
    }

    @Test
    fun `add throws away contexts if actual size would get bigger than the connections the pool created itself`() =
        runBlocking {
            val configuration = RedisContextConfiguration()
            val pool = RedisConnectionPool(configuration)

            val contextTooMuch = createHiredisContext(configuration.address, configuration.port)
            pool.add(contextTooMuch)

            val context = pool.getHiredisContext()
            pool.remove(context)

            // this never completes because first context was thrown away
            val result = async { pool.getHiredisContext() }
            assertFalse { result.isCompleted }

            result.cancel()
        }

}
