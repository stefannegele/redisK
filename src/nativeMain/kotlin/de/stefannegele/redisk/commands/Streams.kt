package de.stefannegele.redisk.commands

import de.stefannegele.redisk.context.RedisContext
import de.stefannegele.redisk.reply.RedisReply
import de.stefannegele.redisk.reply.RedisReplyError
import de.stefannegele.redisk.reply.checkForError
import de.stefannegele.redisk.reply.toStringMap

const val STREAM_ID_LAST = "$"
const val STREAM_ID_LAST_NOT_DELIVERED = ">"
const val STREAM_ID_FIRST = "0"
const val STREAM_ID_AUTO_GENERATE = "*"
const val STREAM_ID_MINIMUM = "-"
const val STREAM_ID_MAXIMUM = "+"


// XADD

suspend fun RedisContext.xAdd(
    stream: String,
    payload: Map<String, String>,
    id: String = STREAM_ID_AUTO_GENERATE
): String = execute("XADD $stream $id ${payload.asArguments()}")
    .checkForError()
    .id

private fun Map<String, String>.asArguments() = map { "${it.key} ${it.value}" }
    .joinToString(" ")


// XDEL

suspend fun RedisContext.xDel(
    stream: String,
    vararg id: String
): Long = execute("XDEL $stream ${id.asArguments()}")
    .checkForError()
    .count


// XRANGE

data class XRangeConfiguration(
    var start: String = STREAM_ID_MINIMUM,
    var end: String = STREAM_ID_MAXIMUM,
    override var count: Int? = null
) : CountConfiguration

suspend fun RedisContext.xRange(
    stream: String,
    configure: XRangeConfiguration.() -> Unit = {}
): List<RedisStreamElementResponse> = XRangeConfiguration().apply(configure)
    .run { execute("XRANGE $stream $start $end $countStatement") }
    .checkForError()
    .let { it.elements ?: emptyList() }
    .map { it.redisStreamElement }


// XGROUP CREATE

data class XGroupCreateConfig(var mkStream: Boolean = false)

suspend fun RedisContext.xGroupCreate(
    stream: String,
    group: String,
    id: String = STREAM_ID_LAST,
    configure: XGroupCreateConfig.() -> Unit = {}
) = XGroupCreateConfig().apply(configure)
    .run { execute("XGROUP CREATE $stream $group $id $mkStreamStatement") }
    .checkForError()
    .let { Unit }

private val XGroupCreateConfig.mkStreamStatement
    get() = if (mkStream) "MKSTREAM" else ""

// XREADGROUP

data class StreamAndId(val stream: String, val id: String = STREAM_ID_LAST_NOT_DELIVERED)
data class XReadGroupConfiguration(
    override var count: Int? = null,
    override var block: Int? = null,
    var noAck: Boolean = false
) : CountConfiguration, BlockConfiguration

suspend fun RedisContext.xReadGroup(
    group: String,
    consumer: String,
    vararg streamAndId: StreamAndId,
    configure: XReadGroupConfiguration.() -> Unit = {}
): List<RedisStreamResponse> = XReadGroupConfiguration().apply(configure)
    .run { execute("XREADGROUP GROUP $group $consumer $blockStatement $countStatement $noAckStatement STREAMS ${streamAndId.streams()} ${streamAndId.ids()}") }
    .checkForError()
    .run { elements ?: emptyList() }
    .map { reply -> reply.redisStream }

private val XReadGroupConfiguration.noAckStatement
    get() = if (noAck) "NOACK" else ""

// XACK

suspend fun RedisContext.xAck(stream: String, group: String, vararg id: String): Long =
    execute("XACK $stream $group ${id.asArguments()}")
        .checkForError()
        .count


// common

data class RedisStreamResponse(val name: String, val elements: List<RedisStreamElementResponse>)
data class RedisStreamElementResponse(val id: String, val payload: Map<String, String>)

private interface CountConfiguration {
    val count: Int?
}

private interface BlockConfiguration {
    val block: Int?
}

private val CountConfiguration.countStatement
    get() = count?.let { "COUNT $it" } ?: ""
private val BlockConfiguration.blockStatement
    get() = block?.let { "BLOCK $it" } ?: ""

private fun Array<out StreamAndId>.streams() = joinToString(" ") { it.stream }
private fun Array<out StreamAndId>.ids() = joinToString(" ") { it.id }

private fun Array<out String>.asArguments() = joinToString(" ")

private val RedisReply.redisStream: RedisStreamResponse
    get() = RedisStreamResponse(name = nameElement, elements = redisStreamElements)

private val RedisReply.redisStreamElements: List<RedisStreamElementResponse>
    get() = elements
        ?.get(1)
        ?.elements
        ?.map { it.redisStreamElement }
        ?: emptyList()

private val RedisReply.redisStreamElement: RedisStreamElementResponse
    get() = RedisStreamElementResponse(id = nameElement, payload = payload)

private val RedisReply.nameElement: String
    get() = elements?.first()?.string ?: throw RedisReplyError("No name in reply.")

private val RedisReply.payload: Map<String, String>
    get() = elements?.get(1)?.elements?.toStringMap() ?: emptyMap()

private val RedisReply.count
    get() = integer ?: 0

private val RedisReply.id
    get() = string ?: throw RedisReplyError("No id in reply.")
