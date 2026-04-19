package com.yasashny.slreplication.node.leaderless

import com.yasashny.slreplication.common.model.ClusterConfig
import com.yasashny.slreplication.common.model.HintRecord
import com.yasashny.slreplication.common.model.Message
import com.yasashny.slreplication.common.model.MessageType
import com.yasashny.slreplication.common.network.MessageSender
import com.yasashny.slreplication.common.network.TcpClient
import com.yasashny.slreplication.node.LamportClock
import com.yasashny.slreplication.node.replication.OperationDeduplicator
import com.yasashny.slreplication.node.storage.HintStore
import com.yasashny.slreplication.node.storage.KeyValueStore
import org.slf4j.LoggerFactory

class LeaderlessCoordinator(
    private val nodeId: String,
    private val store: KeyValueStore,
    private val hintStore: HintStore,
    private val deduplicator: OperationDeduplicator,
    private val lamportClock: LamportClock,
    private val getConfig: () -> ClusterConfig,
    private val getNodeConnection: (String) -> TcpClient?
) {
    private val logger = LoggerFactory.getLogger(LeaderlessCoordinator::class.java)

    private val quorum = QuorumCoordinator(
        nodeId, store, hintStore, deduplicator, lamportClock, getConfig, getNodeConnection
    )

    private val metrics get() = quorum.metrics

    private val hintedHandoff = HintedHandoffHandler(
        nodeId, store, hintStore, deduplicator, lamportClock, metrics, getConfig, getNodeConnection
    )

    private val antiEntropy = AntiEntropyHandler(store, lamportClock, metrics)


    suspend fun coordinateWrite(key: String, value: String, requestId: String?): Message =
        quorum.coordinateWrite(key, value, requestId)

    suspend fun coordinateRead(key: String, requestId: String?): Message =
        quorum.coordinateRead(key, requestId)


    suspend fun handleReplicationWrite(message: Message, sender: MessageSender) {
        val operationId = message.operationId
        val key = message.key
        val value = message.value
        val version = message.version

        if (operationId == null || key == null || value == null || version == null) {
            logger.warn("Invalid REPL_WRITE: missing required fields")
            return
        }

        if (deduplicator.isDuplicate(operationId)) {
            sendWriteAck(sender, message.requestId, operationId)
            return
        }

        val intendedHome = message.leaderless?.intendedHomeNodeId

        if (intendedHome != null && nodeId in getConfig().spareNodeIds) {
            hintStore.addHint(HintRecord(key, value, version, operationId, intendedHome))
            logger.debug("Stored hint: key=$key for $intendedHome")
        } else {
            lamportClock.merge(version.lamport)
            store.putVersioned(key, value, version, operationId)
            logger.debug("Applied REPL_WRITE: key=$key v=(${version.lamport},${version.nodeId})")
        }

        sendWriteAck(sender, message.requestId, operationId)
    }

    suspend fun handleReadQuery(message: Message, sender: MessageSender) {
        val key = message.key ?: return
        sender.send(Message(
            type = MessageType.READ_RESPONSE,
            requestId = message.requestId,
            fromNodeId = nodeId,
            key = key,
            value = store.get(key),
            version = store.getVersion(key),
            operationId = store.getOperationId(key)
        ))
    }


    suspend fun handleHintedHandoffTransfer(message: Message, sender: MessageSender) =
        hintedHandoff.handleTransfer(message, sender)

    suspend fun runHintedHandoff(): Map<String, Long> = hintedHandoff.runHandoff()


    suspend fun handleMerkleRootRequest(message: Message, sender: MessageSender) =
        antiEntropy.handleRootRequest(message, sender)

    suspend fun handleMerkleDiffRequest(message: Message, sender: MessageSender) =
        antiEntropy.handleDiffRequest(message, sender)

    suspend fun handleMerkleRecordsTransfer(message: Message, sender: MessageSender) =
        antiEntropy.handleRecordsTransfer(message, sender)


    fun getStats(): Map<String, Long> = metrics.toMap() + ("hintsStored" to hintStore.size().toLong())

    fun resetStats() = metrics.reset()

    fun stop() = quorum.stop()

    private suspend fun sendWriteAck(sender: MessageSender, requestId: String?, operationId: String) {
        sender.send(Message(
            type = MessageType.REPL_WRITE_ACK,
            requestId = requestId,
            operationId = operationId,
            fromNodeId = nodeId
        ))
    }
}
