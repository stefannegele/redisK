package de.stefannegele.redisk

import de.stefannegele.redisk.context.*
import de.stefannegele.redisk.context.*
import kotlin.native.concurrent.AtomicReference
import kotlin.native.concurrent.freeze

private const val DEFAULT_NAME = "_default_"

private val contextsRef = AtomicReference<Map<String, RedisContext>>(mapOf())

suspend fun <T> withRedisContext(name: String = DEFAULT_NAME, action: suspend RedisContext.() -> T): T =
    findRedisContext(name).run { action() }

fun connectRedis(name: String = DEFAULT_NAME, configure: RedisContextConfiguration.() -> Unit = {}) =
    alterRedisContexts { contexts ->
        require(contexts[name] == null) { "Redis context with name `$name` is already configured." }

        val configuration = RedisContextConfiguration().apply(configure)

        val context = when {
            configuration.connectionPoolSize == 1 -> SingleConnectionRedisContext(configuration)
            configuration.connectionPoolSize > 1 -> PooledConnectionRedisContext(RedisConnectionPool(configuration))
            else -> throw IndexOutOfBoundsException("Pool size must be higher than 0.")
        }

        contexts[name] = context
    }

fun disconnectRedis(name: String = DEFAULT_NAME) = findRedisContext(name)
    .disconnect()
    .also { alterRedisContexts { it.remove(name) } }

private fun alterRedisContexts(action: (MutableMap<String, RedisContext>) -> Unit) {
    val contexts = contextsRef.value
    val newContexts = contexts.toMutableMap()

    action(newContexts)

    contextsRef.compareAndSwap(contexts, newContexts.freeze())
}

private fun findRedisContext(name: String) =
    requireNotNull(contextsRef.value[name]) { "No redis context with name `$name` configured." }
