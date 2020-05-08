package de.stefannegele.redisk.reply

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertFailsWith

class RedisReplyTest {

    @Test
    fun toStringMap() {
        val result = listOf(
            RedisReply.Text("bwa"),
            RedisReply.Text("wat"),
            RedisReply.Text("baw"),
            RedisReply.Text("wah")
        ).toStringMap()

        assertEquals("wat", result["bwa"])
        assertEquals("wah", result["baw"])

        assertFailsWith<IllegalArgumentException> {
            listOf(
                RedisReply.Text("bwa"),
                RedisReply.Text("wat"),
                RedisReply.Text("baw")
            ).toStringMap()
        }
    }

    @Test
    fun `asType works with RedisReply Array`() {
        val reply: RedisReply = RedisReply.Array(emptyList())

        assertEquals(RedisReply.Array::class, reply.asType<RedisReply.Array>()::class)

        assertFailsWith<IllegalArgumentException> { reply.asType<RedisReply.Empty>() }
        assertFailsWith<IllegalArgumentException> { reply.asType<RedisReply.Error>() }
        assertFailsWith<IllegalArgumentException> { reply.asType<RedisReply.Integer>() }
        assertFailsWith<IllegalArgumentException> { reply.asType<RedisReply.Status>() }
        assertFailsWith<IllegalArgumentException> { reply.asType<RedisReply.Text>() }
    }

    @Test
    fun `asType works with RedisReply Error`() {
        val reply: RedisReply = RedisReply.Error("message")

        assertEquals(RedisReply.Error::class, reply.asType<RedisReply.Error>()::class)
        assertFailsWith<RedisReplyError> { reply.asType<RedisReply.Array>() }
        assertFailsWith<RedisReplyError> { reply.asType<RedisReply.Empty>() }
        assertFailsWith<RedisReplyError> { reply.asType<RedisReply.Integer>() }
        assertFailsWith<RedisReplyError> { reply.asType<RedisReply.Status>() }
        assertFailsWith<RedisReplyError> { reply.asType<RedisReply.Text>() }
    }

}
