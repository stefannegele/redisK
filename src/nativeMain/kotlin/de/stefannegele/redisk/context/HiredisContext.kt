package de.stefannegele.redisk.context

import hiredis.redisConnect
import hiredis.redisContext
import kotlinx.cinterop.CPointer
import kotlinx.cinterop.pointed
import kotlinx.cinterop.toKStringFromUtf8
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withContext

@SharedImmutable
internal val hiredisCoroutineContext = Dispatchers.Main

internal suspend fun createHiredisContext(address: String, port: Int): CPointer<redisContext> = coroutineScope {
    withContext(hiredisCoroutineContext) {
        requireNotNull(redisConnect(address, port)) { "Can't allocate redis context." }.apply {
            if (pointed.err > 0) {
                throw RedisConnectionError("Can not connect to redis: ${pointed.errstr.toKStringFromUtf8()}")
            }
        }
    }
}

data class RedisConnectionError(override val message: String): Throwable()
