package de.stefannegele.redisk.commands

import de.stefannegele.redisk.testWithRedisContext
import kotlin.native.concurrent.AtomicInt
import kotlin.native.concurrent.TransferMode
import kotlin.native.concurrent.Worker
import kotlin.test.Test
import kotlin.test.assertTrue


val result = AtomicInt(0)

class PubSubTest {

    @Test
    fun `subscribeBlocking blocks a worker and invokes callback on published messages (through publish)`() =
        testWithRedisContext {

            val worker = Worker.start()

            worker.execute(TransferMode.SAFE, { }) {
                val context = "blocked test context (subscribeBlocking)"
                testWithRedisContext(context) {
                    subscribeBlocking("test-pubsub") {
                        result.increment()
                    }
                }
            }

            while (result.value < 1) {
                publish("test-pubsub", "test")
            }

            worker.requestTermination()

            assertTrue("this must be reached") { true }
        }

}
