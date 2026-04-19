package com.yasashny.slreplication.node.replication

import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap


class OperationDeduplicator(
    private val ttlMs: Long = 5 * 60 * 1000,
    private val cleanupIntervalMs: Long = 60 * 1000
) {
    private val logger = LoggerFactory.getLogger(OperationDeduplicator::class.java)
    private val seenOperations = ConcurrentHashMap<String, Long>()
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())

    init {
        startCleanupTask()
    }


    fun isDuplicate(operationId: String): Boolean {
        val now = System.currentTimeMillis()
        val existing = seenOperations.putIfAbsent(operationId, now)
        return existing != null
    }

    private fun startCleanupTask() {
        scope.launch {
            while (isActive) {
                delay(cleanupIntervalMs)
                cleanup()
            }
        }
    }

    private fun cleanup() {
        val now = System.currentTimeMillis()
        val expiredBefore = now - ttlMs
        var removed = 0

        val iterator = seenOperations.entries.iterator()
        while (iterator.hasNext()) {
            val entry = iterator.next()
            if (entry.value < expiredBefore) {
                iterator.remove()
                removed++
            }
        }

        if (removed > 0) {
            logger.debug("Cleaned up $removed expired operations, remaining: ${seenOperations.size}")
        }
    }

    fun stop() {
        scope.cancel()
    }

    fun reset() {
        seenOperations.clear()
    }

    fun size(): Int = seenOperations.size
}
