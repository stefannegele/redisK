package de.stefannegele.redisk.reply

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class RedisReplyTest {

    @Test
    fun toStringMap() {
        val result = listOf(
            RedisReply(RedisReply.Type.String, string = "bwa"),
            RedisReply(RedisReply.Type.String, string = "wat"),
            RedisReply(RedisReply.Type.String, string = "baw"),
            RedisReply(RedisReply.Type.String, string = "wah")
        ).toStringMap()

        assertEquals("wat", result["bwa"])
        assertEquals("wah", result["baw"])

        assertFailsWith<IllegalArgumentException> {
            listOf(
                RedisReply(RedisReply.Type.String, string = "bwa"),
                RedisReply(RedisReply.Type.String, string = "wat"),
                RedisReply(RedisReply.Type.String, string = "baw")
            ).toStringMap()
        }
    }

    @Test
    fun checkForError() {
        val reply = RedisReply(RedisReply.Type.Error, string = "Some error.")

        assertFailsWith<RedisReplyError> { reply.checkForError() }

        // check that other replies do not fail
        RedisReply(RedisReply.Type.Array).checkForError()
        RedisReply(RedisReply.Type.Empty).checkForError()
        RedisReply(RedisReply.Type.Integer).checkForError()
        RedisReply(RedisReply.Type.Status).checkForError()
        RedisReply(RedisReply.Type.String).checkForError()
    }

}
