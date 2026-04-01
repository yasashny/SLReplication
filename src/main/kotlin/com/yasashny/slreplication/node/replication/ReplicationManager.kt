package com.yasashny.slreplication.node.replication

import com.yasashny.slreplication.common.model.*
import com.yasashny.slreplication.common.network.TcpClient
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random

sealed class ReplicationResult {
    object Success : ReplicationResult()
    data class Error(val code: ErrorCode, val message: String) : ReplicationResult()
}

data class PendingOperation(
    val operationId: String,
    val key: String,
    val value: String,
    val targetNodeId: String,
    val retryCount: Int = 0,
    val createdAt: Long = System.currentTimeMillis(),
    val originNodeId: String? = null,
    val version: Version? = null,
    val sourceNodeId: String? = null
)

class ReplicationManager(
    private val nodeId: String,
    private val getConfig: () -> ClusterConfig,
    private val getNodeConnection: (String) -> TcpClient?
) {
    private val logger = LoggerFactory.getLogger(ReplicationManager::class.java)
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    private val pendingQueues = ConcurrentHashMap<String, ConcurrentLinkedQueue<PendingOperation>>()
    private val ackCounters = ConcurrentHashMap<String, AtomicInteger>()

    private val maxRetries = 10
    private val retryDelayMs = 1000L
    private val baseSyncTimeoutMs = 5000L
    private val baseSemiSyncTimeoutMs = 3000L

    init {
        startRetryWorker()
    }

    // ========== Single-leader replication ==========

    suspend fun replicatePut(key: String, value: String): ReplicationResult {
        val config = getConfig()
        val operationId = UUID.randomUUID().toString()

        logger.debug(
            "Replicating PUT key={}, opId={}, mode={}, RF={}",
            key, operationId, config.replicationMode, config.replicationFactor
        )

        val followers = config.nodes.filter { it.nodeId != nodeId }
        if (followers.isEmpty()) {
            logger.debug("No followers to replicate to")
            return ReplicationResult.Success
        }

        val delay = computeDelay(config)

        ackCounters[operationId] = AtomicInteger(0)

        followers.forEach { follower ->
            scope.launch {
                if (delay > 0) delay(delay)
                sendSingleReplication(operationId, key, value, follower.nodeId)
            }
        }

        return when (config.replicationMode) {
            ReplicationMode.ASYNC -> ReplicationResult.Success

            ReplicationMode.SYNC -> {
                val requiredAcks = config.replicationFactor - 1
                val effectiveTimeout = baseSyncTimeoutMs + config.replicationDelayMaxMs
                waitForAcks(operationId, requiredAcks, effectiveTimeout, followers.size)
            }

            ReplicationMode.SEMI_SYNC -> {
                val requiredAcks = config.semiSyncAcks
                val effectiveTimeout = baseSemiSyncTimeoutMs + config.replicationDelayMaxMs
                val result = waitForAcks(operationId, requiredAcks, effectiveTimeout, followers.size)
                if (result is ReplicationResult.Success) {
                    val remainingAcks = config.replicationFactor - 1 - requiredAcks
                    if (remainingAcks > 0) {
                        scope.launch {
                            continueReplication(operationId, config.replicationFactor - 1, followers.size)
                        }
                    }
                }
                result
            }
        }
    }

    // ========== Multi-leader replication ==========

    fun replicateMulti(operationId: String, key: String, value: String, version: Version, originNodeId: String) {
        val config = getConfig()
        val targets = getInitialTargets(config)
        val delay = computeDelay(config)

        logger.debug("Multi replicating opId={}, topology={}, targets={}", operationId, config.topology, targets)

        targets.forEach { targetNodeId ->
            scope.launch {
                if (delay > 0) delay(delay)
                sendMultiReplication(operationId, key, value, version, originNodeId, nodeId, targetNodeId)
            }
        }
    }

    fun forwardMulti(operationId: String, key: String, value: String, version: Version, originNodeId: String, sourceNodeId: String) {
        val config = getConfig()
        val targets = getForwardTargets(config, originNodeId, sourceNodeId)

        if (targets.isEmpty()) return

        val delay = computeDelay(config)

        logger.debug("Multi forwarding opId={}, topology={}, targets={}", operationId, config.topology, targets)

        targets.forEach { targetNodeId ->
            scope.launch {
                if (delay > 0) delay(delay)
                sendMultiReplication(operationId, key, value, version, originNodeId, nodeId, targetNodeId)
            }
        }
    }

    // ========== Topology target computation ==========

    private fun getInitialTargets(config: ClusterConfig): List<String> {
        val allOthers = config.nodes.map { it.nodeId }.filter { it != nodeId }
        return when (config.topology) {
            Topology.MESH -> allOthers
            Topology.RING -> {
                val next = getNextInRing(config)
                if (next != null) listOf(next) else emptyList()
            }
            Topology.STAR -> {
                if (nodeId == config.starCenterId) {
                    allOthers
                } else {
                    val center = config.starCenterId
                    if (center != null && center != nodeId) listOf(center) else emptyList()
                }
            }
        }
    }

    private fun getForwardTargets(config: ClusterConfig, originNodeId: String, sourceNodeId: String): List<String> {
        return when (config.topology) {
            Topology.MESH -> emptyList()
            Topology.RING -> {
                val next = getNextInRing(config)
                if (next != null && next != originNodeId) listOf(next) else emptyList()
            }
            Topology.STAR -> {
                if (nodeId == config.starCenterId) {
                    config.nodes.map { it.nodeId }
                        .filter { it != nodeId && it != sourceNodeId && it != originNodeId }
                } else {
                    emptyList()
                }
            }
        }
    }

    private fun getNextInRing(config: ClusterConfig): String? {
        val sorted = config.nodes.map { it.nodeId }.sorted()
        val idx = sorted.indexOf(nodeId)
        if (idx == -1) return null
        return sorted[(idx + 1) % sorted.size]
    }

    // ========== Send helpers ==========

    private fun sendSingleReplication(operationId: String, key: String, value: String, targetNodeId: String) {
        val message = Message(
            type = MessageType.REPL_PUT,
            operationId = operationId,
            originNodeId = nodeId,
            key = key,
            value = value
        )
        sendOrQueue(message, targetNodeId, PendingOperation(operationId, key, value, targetNodeId))
    }

    private fun sendMultiReplication(
        operationId: String, key: String, value: String,
        version: Version, originNodeId: String, sourceNodeId: String, targetNodeId: String
    ) {
        val message = Message(
            type = MessageType.REPL_PUT,
            operationId = operationId,
            originNodeId = originNodeId,
            sourceNodeId = sourceNodeId,
            key = key,
            value = value,
            version = version
        )
        sendOrQueue(message, targetNodeId, PendingOperation(
            operationId = operationId, key = key, value = value, targetNodeId = targetNodeId,
            originNodeId = originNodeId, version = version, sourceNodeId = sourceNodeId
        ))
    }

    private fun sendOrQueue(message: Message, targetNodeId: String, pendingOp: PendingOperation) {
        val client = getNodeConnection(targetNodeId)
        if (client == null || !client.send(message)) {
            logger.debug("Failed to send replication to $targetNodeId, queuing for retry")
            val queue = pendingQueues.getOrPut(targetNodeId) { ConcurrentLinkedQueue() }
            queue.add(pendingOp)
        } else {
            logger.debug("Sent replication to $targetNodeId: opId=${message.operationId}")
        }
    }

    // ========== ACK handling ==========

    fun handleAck(operationId: String, fromNodeId: String) {
        val counter = ackCounters[operationId]
        if (counter != null) {
            val newCount = counter.incrementAndGet()
            logger.debug("ACK opId=$operationId from $fromNodeId, total=$newCount")
        } else {
            logger.debug("ACK for unknown/expired opId=$operationId from $fromNodeId")
        }
    }

    private suspend fun waitForAcks(
        operationId: String, requiredAcks: Int, timeoutMs: Long, maxPossibleAcks: Int
    ): ReplicationResult {
        if (requiredAcks <= 0) return ReplicationResult.Success
        if (requiredAcks > maxPossibleAcks) {
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
        } catch (e: TimeoutCancellationException) {
            val currentAcks = ackCounters[operationId]?.get() ?: 0
            logger.warn("Replication timeout: got $currentAcks ACKs, needed $requiredAcks")
            ReplicationResult.Error(
                ErrorCode.NOT_ENOUGH_REPLICAS,
                "Timeout waiting for replication: got $currentAcks/$requiredAcks ACKs"
            )
        } finally {
            scope.launch {
                delay(60000)
                ackCounters.remove(operationId)
            }
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

    // ========== Retry worker ==========

    private fun startRetryWorker() {
        scope.launch {
            while (isActive) {
                delay(retryDelayMs)

                for ((targetNodeId, queue) in pendingQueues) {
                    val client = getNodeConnection(targetNodeId)
                    if (client == null || !client.isConnected()) continue

                    val toRetry = mutableListOf<PendingOperation>()
                    while (true) {
                        val op = queue.poll() ?: break
                        toRetry.add(op)
                    }

                    for (op in toRetry) {
                        if (op.retryCount >= maxRetries) {
                            logger.warn("Max retries for opId=${op.operationId} to ${op.targetNodeId}")
                            continue
                        }

                        val message = if (op.version != null) {
                            Message(
                                type = MessageType.REPL_PUT,
                                operationId = op.operationId,
                                originNodeId = op.originNodeId ?: nodeId,
                                sourceNodeId = op.sourceNodeId ?: nodeId,
                                key = op.key, value = op.value, version = op.version
                            )
                        } else {
                            Message(
                                type = MessageType.REPL_PUT,
                                operationId = op.operationId,
                                originNodeId = nodeId,
                                key = op.key, value = op.value
                            )
                        }

                        if (!client.send(message)) {
                            queue.add(op.copy(retryCount = op.retryCount + 1))
                        } else {
                            logger.debug("Retried replication to ${op.targetNodeId}: opId=${op.operationId}")
                        }
                    }
                }
            }
        }
    }

    // ========== Utility ==========

    private fun computeDelay(config: ClusterConfig): Long {
        return if (config.replicationDelayMinMs > 0 || config.replicationDelayMaxMs > 0) {
            Random.nextLong(config.replicationDelayMinMs, config.replicationDelayMaxMs + 1)
        } else 0L
    }

    fun stop() {
        scope.cancel()
    }
}
