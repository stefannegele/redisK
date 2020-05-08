package de.stefannegele.redisk.context

import hiredis.redisContext
import hiredis.redisFree
import kotlinx.cinterop.CPointer

internal class SingleConnectionRedisContext(private val configuration: RedisContextConfiguration) : RedisContext() {

    private var hiredisContext: CPointer<redisContext>? = null

    override suspend fun <T> withHiredisContext(action: suspend (CPointer<redisContext>) -> T): T =
        action(hiredisContext ?: createHiredisContext())

    override suspend fun createRecoveryHiredisContext(): CPointer<redisContext> = createHiredisContext()

    override fun disconnect() {
        hiredisContext?.run { redisFree(this) }
    }

    private suspend fun createHiredisContext(): CPointer<redisContext> =
        createHiredisContext(configuration.address, configuration.port)

}
