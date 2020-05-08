# redisK
A kotlin multiplatform redis client library.

*This is not ready for production usage!*

The kotlin/native implementation is based on [hiredis](https://github.com/redis/hiredis) - so kudos.

Even though this is working, and the interfaces are kind of stable, the implementations are not.
For now, the only platforms I tested this lib on, are macOs and Linux(Ubuntu). 

## Planned Milestones
- Stabilize interfaces (and add a documentation, for sure)
- Revisit implementations
- Secured connection support
- Extension functions for all redis commands (maybe move those to own modules)
- Multithreading for connection pool (with Kotlin 1.4 and a stable coroutine multithreading support for k/n)
- Support for other platforms (JVM, JS, ...)

## Test
In order to run the tests, a redis instance must be running on `localhost:6379` (redis default).

```bash
./gradlew nativeTest
```

## Build
### Prerequisites
#### Ubuntu
```bash
apt-get install libhiredis-dev
```

#### macOS
```bash
brew install hiredis
```
