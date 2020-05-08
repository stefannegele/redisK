package de.stefannegele.redisk.context

import de.stefannegele.redisk.reply.RedisReply
import de.stefannegele.redisk.reply.asType
import de.stefannegele.redisk.testWithRedisContext
import kotlin.test.Test
import kotlin.test.assertEquals

class RedisContextTest {

    @Test
    fun `execute can successfully send a command to redis and get the result`() = testWithRedisContext {
        execute("SET foo bar")
        val result = execute("GET foo")

        assertEquals("bar", result.asType<RedisReply.Text>().text)
    }

    @Test
    fun `execute pipeline multiple commands`() = testWithRedisContext {
        execute("SET foo something-else")

        val result = execute(
            "SET foo pipeline",
            "GET foo"
        )

        assertEquals("pipeline", result[1].asType<RedisReply.Text>().text)
    }

}
