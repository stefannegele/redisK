package de.stefannegele.redisk.reply

import hiredis.*
import kotlinx.cinterop.get
import kotlinx.cinterop.pointed
import kotlinx.cinterop.toKStringFromUtf8

internal fun redisReply.convert(): RedisReply = when (type) {
    REDIS_REPLY_STATUS -> convertStatus()
    REDIS_REPLY_ERROR -> convertError()
    REDIS_REPLY_INTEGER -> convertInteger()
    REDIS_REPLY_NIL -> convertEmpty()
    REDIS_REPLY_ARRAY -> convertArray()
    REDIS_REPLY_STRING -> convertString()
    else -> throw IllegalStateException("Illegal hiredis reply type: $type")
}

private fun convertEmpty() =
    RedisReply(RedisReply.Type.Empty)

private fun redisReply.convertStatus() = RedisReply(
    RedisReply.Type.Status,
    string = str?.toKStringFromUtf8()
)

private fun redisReply.convertError() = RedisReply(
    RedisReply.Type.Error,
    string = str?.toKStringFromUtf8()
)

private fun redisReply.convertInteger() = RedisReply(
    RedisReply.Type.Integer,
    integer = integer
)

private fun redisReply.convertString() = RedisReply(
    RedisReply.Type.String,
    string = str?.toKStringFromUtf8()
)

private fun redisReply.convertArray() = RedisReply(
    RedisReply.Type.Array,
    elements = convertElements()
)

private fun redisReply.convertElements(): List<RedisReply> = List(elements.toInt()) {
    element?.get(it)?.pointed?.convert() ?: throw IllegalStateException("")
}
