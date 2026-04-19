package com.yasashny.slreplication.node.replication

import com.yasashny.slreplication.common.model.ClusterConfig
import com.yasashny.slreplication.common.model.Message
import com.yasashny.slreplication.common.model.Version
import com.yasashny.slreplication.common.network.TcpClient
import com.yasashny.slreplication.node.LamportClock
import com.yasashny.slreplication.node.computeReplicationDelay
import com.yasashny.slreplication.node.storage.KeyValueStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory

class MultiLeaderReplicator(
    private val nodeId: String,
    private val scope: CoroutineScope,
    private val retryQueue: RetryQueue,
    private val store: KeyValueStore,
    private val lamportClock: LamportClock,
    private val deduplicator: OperationDeduplicator,
    private val getNodeConnection: (String) -> TcpClient?,
    private val getConfig: () -> ClusterConfig
) {
    private val logger = LoggerFactory.getLogger(MultiLeaderReplicator::class.java)
    private val router = TopologyRouter(nodeId)

    fun replicate(operationId: String, key: String, value: String, version: Version, originNodeId: String) {
        val config = getConfig()
        val targets = router.getInitialTargets(config)
        val delayMs = computeReplicationDelay(config)

        logger.debug("Multi replicating opId={}, topology={}, targets={}", operationId, config.topology, targets)

        targets.forEach { targetNodeId ->
            scope.launch {
                if (delayMs > 0) delay(delayMs)
                retryQueue.send(operationId, key, value, originNodeId, nodeId, version, targetNodeId)
            }
        }
    }

    fun forward(
        operationId: String, key: String, value: String,
        version: Version, originNodeId: String, sourceNodeId: String
    ) {
        val config = getConfig()
        val targets = router.getForwardTargets(config, originNodeId, sourceNodeId)
        if (targets.isEmpty()) return

        val delayMs = computeReplicationDelay(config)

        logger.debug("Multi forwarding opId={}, topology={}, targets={}", operationId, config.topology, targets)

        targets.forEach { targetNodeId ->
            scope.launch {
                if (delayMs > 0) delay(delayMs)
                retryQueue.send(operationId, key, value, originNodeId, nodeId, version, targetNodeId)
            }
        }
    }

    fun handleIncoming(message: Message) {
        val operationId = message.operationId
        val key = message.key
        val value = message.value
        val originNodeId = message.originNodeId

        if (operationId == null || key == null || value == null || originNodeId == null) {
            logger.warn("Invalid multi REPL_PUT: missing required fields")
            return
        }

        if (deduplicator.isDuplicate(operationId)) {
            logger.debug("Duplicate multi REPL_PUT ignored: $operationId")
            ack(originNodeId, operationId)
            return
        }

        val version = message.version
        if (version == null) {
            logger.warn("Multi REPL_PUT missing version: opId=$operationId")
            return
        }

        lamportClock.merge(version.lamport)

        val applied = store.putVersioned(key, value, version)
        if (applied) {
            logger.debug("Applied multi REPL_PUT: $key=$value v=(${version.lamport},${version.nodeId})")
        } else {
            logger.debug("Rejected older version for $key: v=(${version.lamport},${version.nodeId})")
        }

        ack(originNodeId, operationId)

        val sourceNodeId = message.sourceNodeId ?: originNodeId
        forward(operationId, key, value, version, originNodeId, sourceNodeId)
    }

    private fun ack(targetNodeId: String, operationId: String) {
        if (!sendReplicationAck(nodeId, targetNodeId, operationId, getNodeConnection)) {
            logger.warn("Failed to send ACK for opId=$operationId to $targetNodeId")
        }
    }
}
