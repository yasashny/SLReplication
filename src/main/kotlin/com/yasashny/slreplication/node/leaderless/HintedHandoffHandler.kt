package com.yasashny.slreplication.node.leaderless

import com.yasashny.slreplication.common.model.ClusterConfig
import com.yasashny.slreplication.common.model.HintRecord
import com.yasashny.slreplication.common.model.LeaderlessPayload
import com.yasashny.slreplication.common.model.Message
import com.yasashny.slreplication.common.model.MessageType
import com.yasashny.slreplication.common.network.MessageSender
import com.yasashny.slreplication.common.network.TcpClient
import com.yasashny.slreplication.node.LamportClock
import com.yasashny.slreplication.node.computeReplicationDelay
import com.yasashny.slreplication.node.replication.OperationDeduplicator
import com.yasashny.slreplication.node.storage.HintStore
import com.yasashny.slreplication.node.storage.KeyValueStore
import kotlinx.coroutines.delay
import org.slf4j.LoggerFactory
import java.util.UUID

class HintedHandoffHandler(
    private val nodeId: String,
    private val store: KeyValueStore,
    private val hintStore: HintStore,
    private val deduplicator: OperationDeduplicator,
    private val lamportClock: LamportClock,
    private val metrics: LeaderlessMetrics,
    private val getConfig: () -> ClusterConfig,
    private val getNodeConnection: (String) -> TcpClient?
) {
    private val logger = LoggerFactory.getLogger(HintedHandoffHandler::class.java)
    private val baseTimeoutMs = 5000L

    suspend fun handleTransfer(message: Message, sender: MessageSender) {
        val operationId = message.operationId
        val key = message.key
        val value = message.value
        val version = message.version
        if (operationId == null || key == null || value == null || version == null) return

        if (!deduplicator.isDuplicate(operationId)) {
            lamportClock.merge(version.lamport)
            store.putVersioned(key, value, version, operationId)
            logger.debug("Applied hinted handoff: key=$key")
        }

        sender.send(Message(
            type = MessageType.HINTED_HANDOFF_ACK,
            requestId = message.requestId,
            operationId = operationId,
            fromNodeId = nodeId
        ))
    }

    suspend fun runHandoff(): Map<String, Long> {
        val config = getConfig()
        val allHints = hintStore.getAllHints()
        var delivered = 0L
        var failed = 0L
        val delayMs = computeReplicationDelay(config)

        for ((homeId, hints) in allHints.groupBy { it.intendedHomeNodeId }) {
            val client = getNodeConnection(homeId)
            if (client == null) { failed += hints.size; continue }

            for (hint in hints) {
                if (delayMs > 0) delay(delayMs)
                val success = deliverHint(client, hint)
                if (success) {
                    hintStore.removeHint(hint.operationId)
                    delivered++
                    metrics.hintsDelivered.incrementAndGet()
                } else {
                    failed++
                }
            }
        }

        logger.info("Hinted handoff: delivered=$delivered, failed=$failed")
        return mapOf("hintsDelivered" to delivered, "hintsFailed" to failed)
    }

    private suspend fun deliverHint(client: TcpClient, hint: HintRecord): Boolean {
        val msg = Message(
            type = MessageType.HINTED_HANDOFF_TRANSFER,
            requestId = UUID.randomUUID().toString(),
            operationId = hint.operationId,
            key = hint.key, value = hint.value,
            version = hint.version,
            sourceNodeId = nodeId,
            leaderless = LeaderlessPayload(intendedHomeNodeId = hint.intendedHomeNodeId)
        )
        return try {
            val resp = client.sendAndWait(msg, baseTimeoutMs)
            resp?.type == MessageType.HINTED_HANDOFF_ACK
        } catch (_: Exception) { false }
    }
}
