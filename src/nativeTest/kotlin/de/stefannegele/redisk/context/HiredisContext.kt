package de.stefannegele.redisk.context

import hiredis.redisCommand
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertFailsWith

class HiredisContextTest {

    @Test
    fun `createHiredisContext uses hiredis for a redis connection and returns a hiredisContext pointer`() =
        runBlocking<Unit> {
            val context = createHiredisContext("localhost", 6379)
            redisCommand(context, "SET foo bar")
        }

    @Test
    fun `createHiredisContext throws an error on connection problems`() = runBlocking<Unit> {
        assertFailsWith<RedisConnectionError> { createHiredisContext("localhost", 9999) }
    }

}
