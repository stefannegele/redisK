package de.stefannegele.redisk.context

private const val DEFAULT_ADDRESS = "localhost"
private const val DEFAULT_PORT = 6379
private const val DEFAULT_CONNECTION_POOL_SIZE = 1

data class RedisContextConfiguration(
    var address: String = DEFAULT_ADDRESS,
    var port: Int = DEFAULT_PORT,
    var connectionPoolSize: Int = DEFAULT_CONNECTION_POOL_SIZE
)
