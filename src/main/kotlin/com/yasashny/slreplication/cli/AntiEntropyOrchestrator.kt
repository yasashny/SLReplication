package com.yasashny.slreplication.cli

import com.yasashny.slreplication.common.model.*
import org.slf4j.LoggerFactory
import java.util.UUID

class AntiEntropyOrchestrator(
    private val sendRequest: (String, Message, Long) -> Result<Message>,
    private val clientId: String
) {
    private val logger = LoggerFactory.getLogger(AntiEntropyOrchestrator::class.java)

    fun run(homeReplicaIds: List<String>, delayMs: Long = 0): Result<String> {
        if (homeReplicaIds.isEmpty()) return Result.failure(Exception("No home replicas configured"))

        val roots = collectMerkleRoots(homeReplicaIds)
        val livingHomes = roots.keys.toList()
        if (livingHomes.size < 2) return Result.success("Less than 2 home replicas responded, nothing to sync")

        val divergentPairs = findDivergentPairs(livingHomes, roots)
        if (divergentPairs.isEmpty()) return Result.success("All home replicas in sync (${livingHomes.size} checked)")

        var totalRecovered = 0
        for ((nodeA, nodeB) in divergentPairs) {
            totalRecovered += syncPair(nodeA, nodeB, delayMs)
        }

        return Result.success("Anti-entropy completed: $totalRecovered records recovered across ${divergentPairs.size} pairs")
    }

    private fun collectMerkleRoots(homeReplicaIds: List<String>): Map<String, String> {
        val roots = mutableMapOf<String, String>()
        for (homeId in homeReplicaIds) {
            val result = sendRequest(homeId, Message(
                type = MessageType.MERKLE_ROOT_REQUEST,
                requestId = UUID.randomUUID().toString(),
                clientId = clientId
            ), 10000)
            result.getOrNull()?.leaderless?.merkleRoot?.let { roots[homeId] = it }
        }
        return roots
    }

    private fun findDivergentPairs(nodes: List<String>, roots: Map<String, String>): List<Pair<String, String>> {
        val pairs = mutableListOf<Pair<String, String>>()
        for (i in nodes.indices) {
            for (j in i + 1 until nodes.size) {
                if (roots[nodes[i]] != roots[nodes[j]]) {
                    pairs.add(nodes[i] to nodes[j])
                }
            }
        }
        return pairs
    }

    private fun syncPair(nodeA: String, nodeB: String, delayMs: Long): Int {
        val hashesA = requestBucketHashes(nodeA) ?: return 0
        val hashesB = requestBucketHashes(nodeB) ?: return 0

        val diffBuckets = hashesA.indices.filter { it < hashesB.size && hashesA[it] != hashesB[it] }
        if (diffBuckets.isEmpty()) return 0

        val recordsA = requestRecords(nodeA, diffBuckets) ?: return 0
        val recordsB = requestRecords(nodeB, diffBuckets) ?: return 0

        val merged = mergeByNewestVersion(recordsA + recordsB)
        val missingFromA = findMissing(merged, recordsA)
        val missingFromB = findMissing(merged, recordsB)

        var recovered = 0
        if (missingFromA.isNotEmpty()) {
            transferRecords(nodeA, missingFromA, delayMs)
            recovered += missingFromA.size
        }
        if (missingFromB.isNotEmpty()) {
            transferRecords(nodeB, missingFromB, delayMs)
            recovered += missingFromB.size
        }
        return recovered
    }

    private fun mergeByNewestVersion(records: List<VersionedEntry>): Map<String, VersionedEntry> {
        val merged = mutableMapOf<String, VersionedEntry>()
        for (record in records) {
            val existing = merged[record.key]
            if (existing == null ||
                (record.version != null && (existing.version == null || record.version > existing.version))) {
                merged[record.key] = record
            }
        }
        return merged
    }

    private fun findMissing(merged: Map<String, VersionedEntry>, localRecords: List<VersionedEntry>): List<VersionedEntry> {
        val localByKey = localRecords.associateBy { it.key }
        return merged.values.filter { newest ->
            val local = localByKey[newest.key]
            local == null || (newest.version != null && (local.version == null || newest.version > local.version))
        }
    }

    private fun requestBucketHashes(nodeId: String): List<String>? {
        val result = sendRequest(nodeId, Message(
            type = MessageType.MERKLE_DIFF_REQUEST,
            requestId = UUID.randomUUID().toString(),
            clientId = clientId
        ), 10000)
        return result.getOrNull()?.leaderless?.bucketHashes
    }

    private fun requestRecords(nodeId: String, buckets: List<Int>): List<VersionedEntry>? {
        val result = sendRequest(nodeId, Message(
            type = MessageType.MERKLE_DIFF_REQUEST,
            requestId = UUID.randomUUID().toString(),
            clientId = clientId,
            leaderless = LeaderlessPayload(diffBuckets = buckets)
        ), 10000)
        return result.getOrNull()?.leaderless?.records
    }

    private fun transferRecords(nodeId: String, records: List<VersionedEntry>, delayMs: Long) {
        if (delayMs > 0) Thread.sleep(delayMs)
        sendRequest(nodeId, Message(
            type = MessageType.MERKLE_RECORDS_TRANSFER,
            requestId = UUID.randomUUID().toString(),
            clientId = clientId,
            leaderless = LeaderlessPayload(records = records)
        ), 30000)
    }
}
