package de.stefannegele.redisk.context

import hiredis.redisContext
import hiredis.redisFree
import kotlinx.cinterop.CPointer
import kotlin.native.concurrent.AtomicInt
import kotlin.native.concurrent.AtomicReference
import kotlin.native.concurrent.freeze

internal class PooledConnectionRedisContext(private val pool: RedisConnectionPool) : RedisContext() {

    override suspend fun <T> withHiredisContext(action: suspend (CPointer<redisContext>) -> T): T =
        with(pool.getHiredisContext()) {
            removeFromPool(this)
            val result = action(this)
            addToPool(this)

            result
        }

    override suspend fun createRecoveryHiredisContext(): CPointer<redisContext> = pool.createRecoveryContext()

    private fun addToPool(context: CPointer<redisContext>) = pool.add(context)
    private fun removeFromPool(context: CPointer<redisContext>) = pool.remove(context)

    override fun disconnect() {
        pool.clear()
    }

}

internal class RedisConnectionPool(private val configuration: RedisContextConfiguration) {

    private val poolReference: AtomicReference<Set<CPointer<redisContext>>> = AtomicReference(setOf())
    private val poolSize = AtomicInt(0)

    tailrec suspend fun getHiredisContext(): CPointer<redisContext> = when {
        poolReference.value.isNotEmpty() -> poolReference.value.first()
        poolSize.value < configuration.connectionPoolSize ->
            createHiredisContext()
                .also { poolSize.increment() }
        else -> getHiredisContext()
    }

    suspend fun createRecoveryContext(): CPointer<redisContext> = createHiredisContext()

    fun add(context: CPointer<redisContext>) {
        if (poolReference.value.size < poolSize.value) alterPool { add(context) }
        else redisFree(context)
    }

    fun remove(context: CPointer<redisContext>) = alterPool { remove(context) }

    fun clear() = alterPool {
        forEach { hiredisContext -> redisFree(hiredisContext) }
        clear()
        poolSize.compareAndSwap(poolSize.value, 0)
    }

    private fun alterPool(action: MutableSet<CPointer<redisContext>>.() -> Unit) = with(poolReference.value) {
        val result = this.toMutableSet()
        result.apply(action)
        poolReference.compareAndSwap(this, result.freeze())
    }

    private suspend fun createHiredisContext() =
        createHiredisContext(configuration.address, configuration.port)

}
