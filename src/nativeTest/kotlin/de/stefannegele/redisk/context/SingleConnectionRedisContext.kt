package de.stefannegele.redisk.context

import hiredis.redisCommand
import kotlinx.coroutines.runBlocking
import kotlin.test.Test

class SingleConnectionRedisContextTest {

    @Test
    fun `withHiredisContext allows to perform commands on a hiredis context`() = runBlocking<Unit> {
        val configuration = RedisContextConfiguration()
        val context = SingleConnectionRedisContext(configuration)

        context.withHiredisContext {
            redisCommand(it, "SET foo bar")
        }
    }

}
