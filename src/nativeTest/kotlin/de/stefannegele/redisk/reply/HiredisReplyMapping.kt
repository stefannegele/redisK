package de.stefannegele.redisk.reply

import hiredis.*
import kotlinx.cinterop.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class HiredisReplyMappingTest {

    @Test
    fun convertEmpty() = memScoped {
        val reply = alloc<redisReply> {
            type = REDIS_REPLY_NIL
        }

        assertTrue(reply.convert() is RedisReply.Empty)
    }

    @Test
    fun convertStatus() = memScoped {
        val string = alloc<ByteVar>()
        val reply = alloc<redisReply> {
            type = REDIS_REPLY_STATUS
            str = string.ptr
        }

        assertTrue(reply.convert() is RedisReply.Status)
        assertEquals(string.ptr.toKStringFromUtf8(), reply.convert().asType<RedisReply.Status>().status)
    }

    @Test
    fun convertError() = memScoped {
        val string = alloc<ByteVar>()
        val reply = alloc<redisReply> {
            type = REDIS_REPLY_ERROR
            str = string.ptr
        }

        assertEquals(string.ptr.toKStringFromUtf8(), reply.convert().asType<RedisReply.Error>().error)
    }

    @Test
    fun convertInteger() = memScoped {
        val reply = alloc<redisReply> {
            type = REDIS_REPLY_INTEGER
            integer = 24
        }

        assertEquals(24, reply.convert().asType<RedisReply.Integer>().integer)
    }

    @Test
    fun convertString() = memScoped {
        val string = alloc<ByteVar>()
        val reply = alloc<redisReply> {
            type = REDIS_REPLY_STRING
            str = string.ptr
        }

        assertEquals(string.ptr.toKStringFromUtf8(), reply.convert().asType<RedisReply.Text>().text)
    }

    @Test
    fun `convertArray converts sub elements`() = memScoped<Unit> {
        val string = alloc<redisReply> {
            type = REDIS_REPLY_STRING
            str = alloc<ByteVar>().ptr
        }

        val empty = alloc<redisReply> {
            type = REDIS_REPLY_NIL
        }

        val integer = alloc<redisReply> {
            type = REDIS_REPLY_INTEGER
            integer = 24
        }

        val ptrList = allocPointerTo<redisReply>().ptr

        ptrList[0] = string.ptr
        ptrList[1] = empty.ptr
        ptrList[2] = integer.ptr

        val reply = alloc<redisReply> {
            type = REDIS_REPLY_ARRAY
            elements = 3.toULong()
            element = ptrList
        }

        val result = reply.convert().asType<RedisReply.Array>()

        result.elements[0].asType<RedisReply.Text>()
        result.elements[1].asType<RedisReply.Empty>()
        result.elements[2].asType<RedisReply.Integer>()
    }

    @Test
    fun `convertArray creates multi dimensional array responses`() = memScoped<Unit> {
        val level3 = alloc<redisReply> {
            type = REDIS_REPLY_NIL
        }

        val ptrList2 = allocPointerTo<redisReply>().ptr
        ptrList2[0] = level3.ptr

        val level2 = alloc<redisReply> {
            type = REDIS_REPLY_ARRAY
            elements = 1.toULong()
            element = ptrList2
        }

        val ptrList3 = allocPointerTo<redisReply>().ptr
        ptrList3[0] = level2.ptr

        val level1 = alloc<redisReply> {
            type = REDIS_REPLY_ARRAY
            elements = 1.toULong()
            element = ptrList3
        }

        val result = level1.convert()

        result.asType<RedisReply.Array>()
            .elements[0]
            .asType<RedisReply.Array>()
            .elements[0]
            .asType<RedisReply.Empty>()
    }

}
