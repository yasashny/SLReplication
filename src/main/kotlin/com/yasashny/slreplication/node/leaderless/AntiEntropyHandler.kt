package com.yasashny.slreplication.node.leaderless

import com.yasashny.slreplication.common.model.LeaderlessPayload
import com.yasashny.slreplication.common.model.Message
import com.yasashny.slreplication.common.model.MessageType
import com.yasashny.slreplication.common.network.MessageSender
import com.yasashny.slreplication.node.LamportClock
import com.yasashny.slreplication.node.storage.KeyValueStore
import org.slf4j.LoggerFactory

class AntiEntropyHandler(
    private val store: KeyValueStore,
    private val lamportClock: LamportClock,
    private val metrics: LeaderlessMetrics
) {
    private val logger = LoggerFactory.getLogger(AntiEntropyHandler::class.java)

    fun buildMerkleTree(): MerkleTree = MerkleTree(store.dumpVersioned())

    suspend fun handleRootRequest(message: Message, sender: MessageSender) {
        sender.send(Message(
            type = MessageType.MERKLE_ROOT_RESPONSE,
            requestId = message.requestId,
            leaderless = LeaderlessPayload(merkleRoot = buildMerkleTree().rootHash)
        ))
    }

    suspend fun handleDiffRequest(message: Message, sender: MessageSender) {
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

    suspend fun handleRecordsTransfer(message: Message, sender: MessageSender) {
        val records = message.leaderless?.records ?: emptyList()
        var applied = 0

        for (record in records) {
            val version = record.version ?: continue
            val opId = record.operationId ?: "${record.key}:${version.lamport}:${version.nodeId}"

            lamportClock.merge(version.lamport)
            if (store.putVersioned(record.key, record.value, version, opId)) {
                applied++
                metrics.antiEntropyRecoveredKeys.incrementAndGet()
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
}
