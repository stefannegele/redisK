package de.stefannegele.redisk.reply

class RedisReply(
    val type: Type,
    val elements: List<RedisReply>? = null,
    val string: String? = null,
    val integer: Long? = null
) {
    enum class Type {
        Array,
        Empty,
        Error,
        Integer,
        Status,
        String
    }
}

data class RedisReplyError(override val message: String) : Throwable()

fun List<RedisReply>.toStringMap(): Map<String, String> =
    require(this.size % 2 == 0) { "Result map must be even." }.run {
        chunked(2).map { Pair(it.first().string ?: "", it.last().string ?: "") }.toMap()
    }

fun RedisReply.checkForError() = takeIf { this.type == RedisReply.Type.Error }
    ?.run { throw RedisReplyError(string ?: "Response error.") }
    ?: this
