package com.yasashny.slreplication.node.leaderless

import com.yasashny.slreplication.common.model.*
import com.yasashny.slreplication.common.network.MessageSender
import com.yasashny.slreplication.common.network.TcpClient
import com.yasashny.slreplication.node.LamportClock
import com.yasashny.slreplication.node.replication.OperationDeduplicator
import com.yasashny.slreplication.node.storage.HintStore
import com.yasashny.slreplication.node.storage.KeyValueStore
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import kotlin.random.Random

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
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    private val quorum = QuorumCoordinator(
        nodeId, store, hintStore, deduplicator, lamportClock, getConfig, getNodeConnection
    )

    val hintsDelivered = AtomicLong(0)
    val antiEntropyRecoveredKeys = AtomicLong(0)

    private val baseTimeoutMs = 5000L


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

        val config = getConfig()
        val intendedHome = message.leaderless?.intendedHomeNodeId

        if (intendedHome != null && nodeId in config.spareNodeIds) {
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


    suspend fun handleHintedHandoffTransfer(message: Message, sender: MessageSender) {
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

    suspend fun runHintedHandoff(): Map<String, Long> {
        val config = getConfig()
        val allHints = hintStore.getAllHints()
        var delivered = 0L
        var failed = 0L
        val delay = computeDelay(config)

        for ((homeId, hints) in allHints.groupBy { it.intendedHomeNodeId }) {
            val client = getNodeConnection(homeId)
            if (client == null) { failed += hints.size; continue }

            for (hint in hints) {
                if (delay > 0) delay(delay)
                val success = deliverHint(client, hint)
                if (success) {
                    hintStore.removeHint(hint.operationId)
                    delivered++
                    hintsDelivered.incrementAndGet()
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


    fun buildMerkleTree(): MerkleTree = MerkleTree(store.dumpVersioned())

    suspend fun handleMerkleRootRequest(message: Message, sender: MessageSender) {
        sender.send(Message(
            type = MessageType.MERKLE_ROOT_RESPONSE,
            requestId = message.requestId,
            leaderless = LeaderlessPayload(merkleRoot = buildMerkleTree().rootHash)
        ))
    }

    suspend fun handleMerkleDiffRequest(message: Message, sender: MessageSender) {
        val requestedBuckets = message.leaderless?.diffBuckets

        if (requestedBuckets.isNullOrEmpty()) {
            sender.send(Message(
                type = MessageType.MERKLE_DIFF_RESPONSE,
                requestId = message.requestId,
                leaderless = LeaderlessPayload(bucketHashes = buildMerkleTree().leafHashes)
            ))
        } else {
            sender.send(Message(
                type = MessageType.MERKLE_DIFF_RESPONSE,
                requestId = message.requestId,
                leaderless = LeaderlessPayload(records = store.getRecordsForBuckets(requestedBuckets.toSet()))
            ))
        }
    }

    suspend fun handleMerkleRecordsTransfer(message: Message, sender: MessageSender) {
        val records = message.leaderless?.records ?: emptyList()
        var applied = 0

        for (record in records) {
            val version = record.version ?: continue
            val opId = record.operationId ?: "${record.key}:${version.lamport}:${version.nodeId}"

            lamportClock.merge(version.lamport)
            if (store.putVersioned(record.key, record.value, version, opId)) {
                applied++
                antiEntropyRecoveredKeys.incrementAndGet()
            }
        }

        logger.info("Merkle records transfer: applied $applied/${records.size}")
        sender.send(Message(
            type = MessageType.MERKLE_RECORDS_ACK,
            requestId = message.requestId,
            status = "OK",
            leaderless = LeaderlessPayload(stats = mapOf("appliedRecords" to applied.toLong()))
        ))
    }


    fun getStats(): Map<String, Long> = mapOf(
        "staleReadCount" to quorum.staleReadCount.get(),
        "readRepairCount" to quorum.readRepairCount.get(),
        "hintsCreated" to quorum.hintsCreated.get(),
        "hintsDelivered" to hintsDelivered.get(),
        "hintsStored" to hintStore.size().toLong(),
        "antiEntropyRecoveredKeys" to antiEntropyRecoveredKeys.get()
    )

    fun resetStats() {
        quorum.staleReadCount.set(0)
        quorum.readRepairCount.set(0)
        quorum.hintsCreated.set(0)
        hintsDelivered.set(0)
        antiEntropyRecoveredKeys.set(0)
    }

    fun stop() {
        quorum.stop()
        scope.cancel()
    }

    private suspend fun sendWriteAck(sender: MessageSender, requestId: String?, operationId: String) {
        sender.send(Message(
            type = MessageType.REPL_WRITE_ACK,
            requestId = requestId,
            operationId = operationId,
            fromNodeId = nodeId
        ))
    }

    private fun computeDelay(config: ClusterConfig): Long {
        return if (config.replicationDelayMinMs > 0 || config.replicationDelayMaxMs > 0) {
            Random.nextLong(config.replicationDelayMinMs, config.replicationDelayMaxMs + 1)
        } else 0L
    }
}
