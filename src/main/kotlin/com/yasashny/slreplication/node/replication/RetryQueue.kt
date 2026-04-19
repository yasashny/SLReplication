package com.yasashny.slreplication.node.replication

import com.yasashny.slreplication.common.model.Message
import com.yasashny.slreplication.common.model.MessageType
import com.yasashny.slreplication.common.model.Version
import com.yasashny.slreplication.common.network.TcpClient
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue

data class PendingOperation(
    val operationId: String,
    val key: String,
    val value: String,
    val targetNodeId: String,
    val retryCount: Int = 0,
    val originNodeId: String? = null,
    val version: Version? = null,
    val sourceNodeId: String? = null
)

class RetryQueue(
    private val nodeId: String,
    private val scope: CoroutineScope,
    private val getNodeConnection: (String) -> TcpClient?
) {
    private val logger = LoggerFactory.getLogger(RetryQueue::class.java)
    private val queues = ConcurrentHashMap<String, ConcurrentLinkedQueue<PendingOperation>>()
    private val maxRetries = 10
    private val retryDelayMs = 1000L

    init {
        startRetryWorker()
    }

    fun send(
        operationId: String, key: String, value: String,
        originNodeId: String, sourceNodeId: String,
        version: Version?, targetNodeId: String
    ) {
        val message = Message(
            type = MessageType.REPL_PUT,
            operationId = operationId,
            originNodeId = originNodeId,
            sourceNodeId = sourceNodeId,
            key = key, value = value, version = version
        )
        val pending = PendingOperation(
            operationId = operationId, key = key, value = value, targetNodeId = targetNodeId,
            originNodeId = originNodeId, version = version, sourceNodeId = sourceNodeId
        )
        sendOrQueue(message, targetNodeId, pending)
    }

    private fun sendOrQueue(message: Message, targetNodeId: String, pendingOp: PendingOperation) {
        val client = getNodeConnection(targetNodeId)
        if (client != null && client.send(message)) {
            logger.debug("Sent replication to $targetNodeId: opId=${message.operationId}")
        } else {
            logger.debug("Failed to send replication to $targetNodeId, queuing for retry")
            queues.getOrPut(targetNodeId) { ConcurrentLinkedQueue() }.add(pendingOp)
        }
    }

    private fun startRetryWorker() {
        scope.launch {
            while (isActive) {
                delay(retryDelayMs)
                retryPending()
            }
        }
    }

    private fun retryPending() {
        for ((targetNodeId, queue) in queues) {
            val client = getNodeConnection(targetNodeId)
            if (client == null || !client.isConnected()) continue

            val batch = drainQueue(queue)
            for (op in batch) {
                if (op.retryCount >= maxRetries) {
                    logger.warn("Max retries for opId=${op.operationId} to ${op.targetNodeId}")
                    continue
                }
                val message = Message(
                    type = MessageType.REPL_PUT,
                    operationId = op.operationId,
                    originNodeId = op.originNodeId ?: nodeId,
                    sourceNodeId = op.sourceNodeId ?: nodeId,
                    key = op.key, value = op.value, version = op.version
                )
                if (!client.send(message)) {
                    queue.add(op.copy(retryCount = op.retryCount + 1))
                } else {
                    logger.debug("Retried replication to ${op.targetNodeId}: opId=${op.operationId}")
                }
            }
        }
    }

    private fun drainQueue(queue: ConcurrentLinkedQueue<PendingOperation>): List<PendingOperation> {
        val result = mutableListOf<PendingOperation>()
        while (true) {
            result.add(queue.poll() ?: break)
        }
        return result
    }
}
