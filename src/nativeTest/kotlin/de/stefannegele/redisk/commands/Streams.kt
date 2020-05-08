package de.stefannegele.redisk.commands

import de.stefannegele.redisk.testWithRedisContext
import de.stefannegele.redisk.context.RedisContext
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlin.system.getTimeMillis
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertTrue

class StreamsTest {

    @Test
    fun `xAdd, xDel and xRange work together correctly`() = testWithRedisContext {
        val stream = "test-xadd-xdel-xrange"
        clearStream(stream)

        val id1 = xAdd(stream, mapOf("baw" to "bah", "mew" to "mao"))
        val id2 = xAdd(stream, mapOf("mwa" to "zza", "rrr" to "grr"))

        val rangeResult = xRange(stream)

        assertEquals(id1, rangeResult[0].id)
        assertEquals(id2, rangeResult[1].id)

        assertEquals("bah", rangeResult[0].payload["baw"])
        assertEquals("mao", rangeResult[0].payload["mew"])
        assertEquals("zza", rangeResult[1].payload["mwa"])
        assertEquals("grr", rangeResult[1].payload["rrr"])

        val delResult = xDel(stream, id1, id2)

        assertEquals(2, delResult)

        assertTrue(xRange(stream).isEmpty())
    }

    @Test
    fun `xGroupCreate creates a new group and a new stream if needed`() = testWithRedisContext {
        val stream = "test-xreadgroup-${getTimeMillis()}"
        val group = "group-${getTimeMillis()}"

        assertFails { xGroupCreate(stream, group) }
        xGroupCreate(stream, group) { mkStream = true }
    }

    @Test
    fun `xGroupCreate creates a new group with the correct elements`() = testWithRedisContext {
        val stream = "test-xreadgroup"
        val groupEmpty = "group-${getTimeMillis()}"
        val groupNotEmpty = "group-${getTimeMillis()+1}"

        xAdd(stream, mapOf("pay" to "load"))

        xGroupCreate(stream, groupEmpty) { mkStream = true }
        xGroupCreate(stream, groupNotEmpty, STREAM_ID_FIRST) { mkStream = true }

        val empty = xReadGroup(groupEmpty, "c", StreamAndId(stream))
        val notEmpty = xReadGroup(groupNotEmpty, "c", StreamAndId(stream))

        assertTrue { empty.isEmpty() }
        assertTrue { notEmpty.isNotEmpty() }
    }

    @Test
    fun `xreadgroup works correctly with xack`() = testWithRedisContext {
        val stream = "test-xreadgroup-xack"
        val group = "group"
        val consumer = "consumer"

        clearStream(stream)

        execute("XGROUP DESTROY $stream $group")
        execute("XGROUP CREATE $stream $group $ MKSTREAM")

        // add some element
        xAdd(stream, mapOf("pay" to "load"))

        // history should be empty now
        xReadGroup(group, consumer, StreamAndId(stream, STREAM_ID_FIRST)).apply {
            assertTrue { flatMap { it.elements }.isEmpty() }
        }

        // find latest entry, but do not acknowledge
        xReadGroup(group, consumer, StreamAndId(stream)) { block = 1000 }.apply {
            assertTrue { flatMap { it.elements }.isNotEmpty() }
        }

        // now acknowledge from history
        xReadGroup(group, consumer, StreamAndId(stream, STREAM_ID_FIRST)).apply {
            assertTrue { flatMap { it.elements }.isNotEmpty() }
            xAck(stream, group, *flatMap { it.elements }.map { it.id }.toTypedArray())
        }

        // history is empty again
        xReadGroup(group, consumer, StreamAndId(stream, STREAM_ID_FIRST)).apply {
            assertTrue { flatMap { it.elements }.isEmpty() }
        }

        // read and add while reading, also acknowledge directly
        coroutineScope {
            val result = async {
                xReadGroup(group, consumer, StreamAndId(stream)) { block = 1000 }.apply {
                    assertTrue { flatMap { it.elements }.isNotEmpty() }
                    xAck(stream, group, *flatMap { it.elements }.map { it.id }.toTypedArray())
                }
            }
            val id = xAdd(stream, mapOf("pay" to "load"))

            // the element read is the element written:
            assertEquals(id, result.await().first().elements.first().id)
        }

        // history is still empty
        xReadGroup(group, consumer, StreamAndId(stream, STREAM_ID_FIRST)).apply {
            assertTrue { flatMap { it.elements }.isEmpty() }
        }
    }

    private suspend fun RedisContext.clearStream(name: String) {
        xRange(name).takeIf { it.isNotEmpty() }
            ?.run { xDel(name, *map { it.id }.toTypedArray()) }
    }

}
