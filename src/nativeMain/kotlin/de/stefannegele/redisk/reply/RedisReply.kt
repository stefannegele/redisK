package de.stefannegele.redisk.reply

sealed class RedisReply {
    data class Array(val elements: List<RedisReply>) : RedisReply()
    object Empty : RedisReply()
    data class Error(val error: String) : RedisReply()
    data class Integer(val integer: Long) : RedisReply()
    data class Status(val status: String) : RedisReply()
    data class Text(val text: String) : RedisReply()
}

data class RedisReplyError(override val message: String) : Throwable()

fun List<RedisReply.Text>.toStringMap(): Map<String, String> =
    require(this.size % 2 == 0) { "Result map must be even." }.run {
        chunked(2).map { Pair(it.first().text, it.last().text) }.toMap()
    }

inline fun <reified T : RedisReply> RedisReply.asType(): T {
    if (this is RedisReply.Error && T::class != RedisReply.Error::class) throw RedisReplyError(error)
    require(this is T) {
        "${RedisReply::class.simpleName} must be of type ${T::class.simpleName} but is of type ${this::class.simpleName}."
    }
    return this
}
