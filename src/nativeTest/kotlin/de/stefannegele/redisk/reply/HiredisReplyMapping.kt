package de.stefannegele.redisk.reply

import hiredis.*
import kotlinx.cinterop.*
import kotlin.test.Test
import kotlin.test.assertEquals

class HiredisReplyMappingTest {

    @Test
    fun convertEmpty() = memScoped {
        val reply = alloc<redisReply> {
            type = REDIS_REPLY_NIL
        }

        assertEquals(RedisReply.Type.Empty, reply.convert().type)
    }

    @Test
    fun convertStatus() = memScoped {
        val string = alloc<ByteVar>()
        val reply = alloc<redisReply> {
            type = REDIS_REPLY_STATUS
            str = string.ptr
        }

        assertEquals(RedisReply.Type.Status, reply.convert().type)
        assertEquals(string.ptr.toKStringFromUtf8(), reply.convert().string)
    }

    @Test
    fun convertError() = memScoped {
        val string = alloc<ByteVar>()
        val reply = alloc<redisReply> {
            type = REDIS_REPLY_ERROR
            str = string.ptr
        }

        assertEquals(RedisReply.Type.Error, reply.convert().type)
        assertEquals(string.ptr.toKStringFromUtf8(), reply.convert().string)
    }

    @Test
    fun convertInteger() = memScoped {
        val reply = alloc<redisReply> {
            type = REDIS_REPLY_INTEGER
            integer = 24
        }

        assertEquals(RedisReply.Type.Integer, reply.convert().type)
        assertEquals(24, reply.convert().integer)
    }

    @Test
    fun convertString() = memScoped {
        val string = alloc<ByteVar>()
        val reply = alloc<redisReply> {
            type = REDIS_REPLY_STRING
            str = string.ptr
        }

        assertEquals(RedisReply.Type.String, reply.convert().type)
        assertEquals(string.ptr.toKStringFromUtf8(), reply.convert().string)
    }

    @Test
    fun `convertArray converts sub elements`() = memScoped {
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

        val result = reply.convert()

        assertEquals(RedisReply.Type.Array, result.type)
        assertEquals(RedisReply.Type.String, result.elements?.get(0)?.type)
        assertEquals(RedisReply.Type.Empty, result.elements?.get(1)?.type)
        assertEquals(RedisReply.Type.Integer, result.elements?.get(2)?.type)
    }

    @Test
    fun `convertArray creates multiple dimensional array responses`() = memScoped {
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

        assertEquals(RedisReply.Type.Array, result.type)
    }

}
