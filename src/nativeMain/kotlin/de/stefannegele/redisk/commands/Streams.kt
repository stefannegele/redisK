package de.stefannegele.redisk.commands

import de.stefannegele.redisk.context.RedisContext
import de.stefannegele.redisk.reply.RedisReply
import de.stefannegele.redisk.reply.asType
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
    .asType<RedisReply.Text>()
    .text

private fun Map<String, String>.asArguments() = map { "${it.key} ${it.value}" }
    .joinToString(" ")


// XDEL

suspend fun RedisContext.xDel(
    stream: String,
    vararg id: String
): Long = execute("XDEL $stream ${id.asArguments()}")
    .asType<RedisReply.Integer>()
    .integer


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
    .asType<RedisReply.Array>()
    .elements
    .map { it.asStreamElementResponse() }


// XGROUP CREATE

data class XGroupCreateConfig(var mkStream: Boolean = false)

suspend fun RedisContext.xGroupCreate(
    stream: String,
    group: String,
    id: String = STREAM_ID_LAST,
    configure: XGroupCreateConfig.() -> Unit = {}
) = XGroupCreateConfig().apply(configure)
    .run { execute("XGROUP CREATE $stream $group $id $mkStreamStatement") }
    .asType<RedisReply.Status>()
    .status

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
    .takeUnless { it is RedisReply.Empty }
    ?.asType<RedisReply.Array>()
    ?.elements
    ?.map { reply -> reply.asStreamResponse() }
    ?: emptyList()

private val XReadGroupConfiguration.noAckStatement
    get() = if (noAck) "NOACK" else ""

// XACK

suspend fun RedisContext.xAck(stream: String, group: String, vararg id: String): Long =
    execute("XACK $stream $group ${id.asArguments()}")
        .asType<RedisReply.Integer>()
        .integer

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

private fun RedisReply.asStreamResponse(): RedisStreamResponse =
    asType<RedisReply.Array>().run {
        RedisStreamResponse(name = nameElement, elements = streamElements)
    }

private val RedisReply.Array.streamElements: List<RedisStreamElementResponse>
    get() = elements[1]
        .asType<RedisReply.Array>()
        .elements
        .map { it.asStreamElementResponse() }

private fun RedisReply.asStreamElementResponse(): RedisStreamElementResponse =
    asType<RedisReply.Array>().run {
        RedisStreamElementResponse(id = nameElement, payload = payload)
    }

private val RedisReply.Array.nameElement: String
    get() = elements.first()
        .asType<RedisReply.Text>()
        .text

private val RedisReply.Array.payload: Map<String, String>
    get() = elements[1]
        .asType<RedisReply.Array>()
        .elements
        .map { it.asType<RedisReply.Text>() }
        .toStringMap()
