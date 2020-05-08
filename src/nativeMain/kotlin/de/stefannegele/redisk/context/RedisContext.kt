package de.stefannegele.redisk.context

import de.stefannegele.redisk.reply.RedisReply
import de.stefannegele.redisk.reply.convert
import hiredis.*
import kotlinx.cinterop.*
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext

private const val RECOVERY_DELAY = 1000L

abstract class RedisContext {

    internal abstract suspend fun <T> withHiredisContext(action: suspend (CPointer<redisContext>) -> T): T

    internal abstract suspend fun createRecoveryHiredisContext(): CPointer<redisContext>

    internal abstract fun disconnect()

    suspend fun execute(command: String): RedisReply = withContext(hiredisCoroutineContext) {
        withHiredisContext { hiredisContext ->
            val reply = getReplyWithRecovery(hiredisContext) { redisCommand(it, command) }
            val result = reply.reinterpret<redisReply>().pointed.convert()
            freeReplyObject(reply)

            result
        }
    }

    suspend fun execute(vararg commands: String): List<RedisReply> = withContext(hiredisCoroutineContext) {
        withHiredisContext { hiredisContext ->
            for (command in commands) redisAppendCommand(hiredisContext, command)
            memScoped {
                val replyPointerPointer = allocPointerTo<redisReply>()
                val result = mutableListOf<RedisReply>()
                var usableHiredisContext = hiredisContext

                repeat(commands.count()) {
                    val hiredisContextAndReply =
                        getReplyFromPointerWithRecovery(usableHiredisContext, replyPointerPointer)
                    usableHiredisContext = hiredisContextAndReply.first
                    result.add(hiredisContextAndReply.second.pointed.convert())

                    freeReplyObject(replyPointerPointer.pointed?.ptr)
                }

                result
            }
        }
    }

    private tailrec suspend fun getReplyFromPointerWithRecovery(
        hiredisContext: CPointer<redisContext>,
        replyPointerPointer: CPointerVar<redisReply>
    ): Pair<CPointer<redisContext>, CPointer<redisReply>> =
        (redisGetReply(hiredisContext, replyPointerPointer.ptr.reinterpret())
            .let { hiredisContext to replyPointerPointer.pointed?.ptr }
            .takeIf { it.second != null }
            ?.let { it.first to it.second!! }
            ?: getReplyFromPointerWithRecovery(recoverHiredisContext(hiredisContext), replyPointerPointer))

    private tailrec suspend fun getReplyWithRecovery(
        hiredisContext: CPointer<redisContext>,
        action: (CPointer<redisContext>) -> CPointer<out CPointed>?
    ): CPointer<redisReply> = action(hiredisContext)?.reinterpret()
        ?: getReplyWithRecovery(recoverHiredisContext(hiredisContext), action)

    private suspend fun recoverHiredisContext(brokenHiredisContext: CPointer<redisContext>): CPointer<redisContext> {
        println("Hiredis error: ${brokenHiredisContext.pointed.errstr}")
        redisFree(brokenHiredisContext)
        delay(RECOVERY_DELAY)
        return createRecoveryHiredisContext()
    }

}
