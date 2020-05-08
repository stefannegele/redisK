package de.stefannegele.redisk.commands

import de.stefannegele.redisk.context.RedisContext
import de.stefannegele.redisk.context.hiredisCoroutineContext
import de.stefannegele.redisk.reply.RedisReply
import de.stefannegele.redisk.reply.convert
import hiredis.*
import kotlinx.cinterop.*
import kotlinx.coroutines.withContext

suspend fun RedisContext.subscribeBlocking(channel: String, callback: (reply: RedisReply) -> Unit) =
    withContext(hiredisCoroutineContext) {
        withHiredisContext { hiredisContext ->
            val reply = redisCommand(hiredisContext, "SUBSCRIBE $channel")
            freeReplyObject(reply)
            memScoped {
                val replyPointerPointer = allocPointerTo<redisReply>()
                while (redisGetReply(hiredisContext, replyPointerPointer.ptr.reinterpret()) == REDIS_OK) {
                    replyPointerPointer.pointed?.convert()?.let { callback(it) }
                    freeReplyObject(replyPointerPointer.pointed?.ptr)
                }
            }
        }
    }

suspend fun RedisContext.publish(channel: String, value: String) = execute("PUBLISH $channel $value")
