package com.yasashny.slreplication.node.replication

import com.yasashny.slreplication.common.model.ClusterConfig
import com.yasashny.slreplication.common.model.ErrorCode
import com.yasashny.slreplication.common.model.Message
import com.yasashny.slreplication.common.model.ReplicationMode
import com.yasashny.slreplication.common.network.TcpClient
import com.yasashny.slreplication.node.computeReplicationDelay
import com.yasashny.slreplication.node.storage.KeyValueStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import org.slf4j.LoggerFactory
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

class SingleLeaderReplicator(
    private val nodeId: String,
    private val scope: CoroutineScope,
    private val retryQueue: RetryQueue,
    private val store: KeyValueStore,
    private val deduplicator: OperationDeduplicator,
    private val getNodeConnection: (String) -> TcpClient?,
    private val getConfig: () -> ClusterConfig
) {
    private val logger = LoggerFactory.getLogger(SingleLeaderReplicator::class.java)
    private val ackCounters = ConcurrentHashMap<String, AtomicInteger>()

    private val baseSyncTimeoutMs = 5000L
    private val baseSemiSyncTimeoutMs = 3000L
    private val counterTtlMs = 60000L

    suspend fun replicate(key: String, value: String): ReplicationResult {
        val config = getConfig()
        val operationId = UUID.randomUUID().toString()

        val followers = config.nodes.filter { it.nodeId != nodeId }
        if (followers.isEmpty()) return ReplicationResult.Success

        val delayMs = computeReplicationDelay(config)
        ackCounters[operationId] = AtomicInteger(0)

        followers.forEach { follower ->
            scope.launch {
                if (delayMs > 0) delay(delayMs)
                retryQueue.send(operationId, key, value, nodeId, nodeId, null, follower.nodeId)
            }
        }

        return when (config.replicationMode) {
            ReplicationMode.ASYNC -> {
                scheduleCounterCleanup(operationId)
                ReplicationResult.Success
            }
            ReplicationMode.SYNC -> {
                val requiredAcks = config.replicationFactor - 1
                val timeout = baseSyncTimeoutMs + config.replicationDelayMaxMs
                waitForAcks(operationId, requiredAcks, timeout, followers.size)
            }
            ReplicationMode.SEMI_SYNC -> {
                val requiredAcks = config.semiSyncAcks
                val timeout = baseSemiSyncTimeoutMs + config.replicationDelayMaxMs
                val result = waitForAcks(operationId, requiredAcks, timeout, followers.size)
                if (result is ReplicationResult.Success) {
                    val remaining = config.replicationFactor - 1 - requiredAcks
                    if (remaining > 0) {
                        scope.launch {
                            continueReplication(operationId, config.replicationFactor - 1, followers.size)
                        }
                    }
                }
                result
            }
        }
    }

    fun handleIncoming(message: Message) {
        val operationId = message.operationId
        val key = message.key
        val value = message.value
        val originNodeId = message.originNodeId

        if (operationId == null || key == null || value == null || originNodeId == null) {
            logger.warn("Invalid REPL_PUT: missing required fields")
            return
        }

        if (deduplicator.isDuplicate(operationId)) {
            logger.debug("Duplicate REPL_PUT ignored: $operationId")
            ack(originNodeId, operationId)
            return
        }

        store.put(key, value)
        logger.debug("Applied REPL_PUT: $key=$value (opId=$operationId)")
        ack(originNodeId, operationId)
    }

    fun handleAck(operationId: String, fromNodeId: String) {
        val counter = ackCounters[operationId]
        if (counter != null) {
            val newCount = counter.incrementAndGet()
            logger.debug("ACK opId=$operationId from $fromNodeId, total=$newCount")
        }
    }

    private fun ack(targetNodeId: String, operationId: String) {
        if (!sendReplicationAck(nodeId, targetNodeId, operationId, getNodeConnection)) {
            logger.warn("Failed to send ACK for opId=$operationId to $targetNodeId")
        }
    }

    private suspend fun waitForAcks(
        operationId: String, requiredAcks: Int, timeoutMs: Long, maxPossibleAcks: Int
    ): ReplicationResult {
        if (requiredAcks <= 0) {
            scheduleCounterCleanup(operationId)
            return ReplicationResult.Success
        }
        if (requiredAcks > maxPossibleAcks) {
            scheduleCounterCleanup(operationId)
            return ReplicationResult.Error(
                ErrorCode.NOT_ENOUGH_REPLICAS,
                "Not enough followers available: need $requiredAcks, have $maxPossibleAcks"
            )
        }

        return try {
            withTimeout(timeoutMs) {
                while (true) {
                    val currentAcks = ackCounters[operationId]?.get() ?: 0
                    if (currentAcks >= requiredAcks) {
                        return@withTimeout ReplicationResult.Success
                    }
                    delay(10)
                }
                @Suppress("UNREACHABLE_CODE")
                ReplicationResult.Success
            }
        } catch (_: TimeoutCancellationException) {
            val currentAcks = ackCounters[operationId]?.get() ?: 0
            logger.warn("Replication timeout: got $currentAcks ACKs, needed $requiredAcks")
            ReplicationResult.Error(
                ErrorCode.NOT_ENOUGH_REPLICAS,
                "Timeout waiting for replication: got $currentAcks/$requiredAcks ACKs"
            )
        } finally {
            scheduleCounterCleanup(operationId)
        }
    }

    private suspend fun continueReplication(operationId: String, targetAcks: Int, maxPossibleAcks: Int) {
        val config = getConfig()
        val deadline = System.currentTimeMillis() + 30000 + config.replicationDelayMaxMs
        while (System.currentTimeMillis() < deadline) {
            val currentAcks = ackCounters[operationId]?.get() ?: 0
            if (currentAcks >= targetAcks || currentAcks >= maxPossibleAcks) {
                logger.debug("Background replication complete: $currentAcks/$targetAcks ACKs")
                break
            }
            delay(100)
        }
    }

    private fun scheduleCounterCleanup(operationId: String) {
        scope.launch {
            delay(counterTtlMs)
            ackCounters.remove(operationId)
        }
    }
}
