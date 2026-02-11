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
    val createdAt: Long = System.currentTimeMillis()
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
    private val syncTimeoutMs = 5000L
    private val semiSyncTimeoutMs = 3000L

    init {
        startRetryWorker()
    }

    suspend fun replicatePut(key: String, value: String): ReplicationResult {
        val config = getConfig()
        val operationId = UUID.randomUUID().toString()

        logger.debug(
            "Replicating PUT key={}, opId={}, mode={}, RF={}",
            key,
            operationId,
            config.replicationMode,
            config.replicationFactor
        )

        val followers = config.nodes.filter { it.nodeId != nodeId }
        if (followers.isEmpty()) {
            logger.debug("No followers to replicate to")
            return ReplicationResult.Success
        }

        val delay = if (config.replicationDelayMinMs > 0 || config.replicationDelayMaxMs > 0) {
            Random.nextLong(config.replicationDelayMinMs, config.replicationDelayMaxMs + 1)
        } else 0L

        ackCounters[operationId] = AtomicInteger(0)

        followers.forEach { follower ->
            scope.launch {
                if (delay > 0) {
                    delay(delay)
                }
                sendReplication(operationId, key, value, follower.nodeId)
            }
        }

        return when (config.replicationMode) {
            ReplicationMode.ASYNC -> {
                ReplicationResult.Success
            }

            ReplicationMode.SYNC -> {
                val requiredAcks = config.replicationFactor - 1
                waitForAcks(operationId, requiredAcks, syncTimeoutMs, followers.size)
            }

            ReplicationMode.SEMI_SYNC -> {
                val requiredAcks = config.semiSyncAcks
                val result = waitForAcks(operationId, requiredAcks, semiSyncTimeoutMs, followers.size)

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

    private suspend fun waitForAcks(
        operationId: String,
        requiredAcks: Int,
        timeoutMs: Long,
        maxPossibleAcks: Int
    ): ReplicationResult {
        if (requiredAcks <= 0) {
            return ReplicationResult.Success
        }
        if (requiredAcks > maxPossibleAcks) {
            logger.warn("Required ACKs ($requiredAcks) > available followers ($maxPossibleAcks)")
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
        val deadline = System.currentTimeMillis() + 30000

        while (System.currentTimeMillis() < deadline) {
            val currentAcks = ackCounters[operationId]?.get() ?: 0
            if (currentAcks >= targetAcks || currentAcks >= maxPossibleAcks) {
                logger.debug("Background replication complete: $currentAcks/$targetAcks ACKs")
                break
            }
            delay(100)
        }
    }

    private fun sendReplication(
        operationId: String,
        key: String,
        value: String,
        targetNodeId: String
    ) {
        val message = Message(
            type = MessageType.REPL_PUT,
            operationId = operationId,
            originNodeId = nodeId,
            key = key,
            value = value
        )

        val client = getNodeConnection(targetNodeId)
        if (client == null || !client.send(message)) {
            logger.debug("Failed to send replication to $targetNodeId, queuing for retry")
            val queue = pendingQueues.getOrPut(targetNodeId) { ConcurrentLinkedQueue() }
            queue.add(PendingOperation(operationId, key, value, targetNodeId))
        } else {
            logger.debug("Sent replication to $targetNodeId: opId=$operationId")
        }
    }

    fun handleAck(operationId: String, fromNodeId: String) {
        val counter = ackCounters[operationId]
        if (counter != null) {
            val newCount = counter.incrementAndGet()
            logger.debug("Received ACK for opId=$operationId from $fromNodeId, total ACKs: $newCount")
        } else {
            logger.debug("Received ACK for unknown/expired opId=$operationId from $fromNodeId")
        }
    }

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
                            logger.warn("Max retries reached for operation ${op.operationId} to ${op.targetNodeId}")
                            continue
                        }

                        val message = Message(
                            type = MessageType.REPL_PUT,
                            operationId = op.operationId,
                            originNodeId = nodeId,
                            key = op.key,
                            value = op.value
                        )

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

    fun stop() {
        scope.cancel()
    }
}
