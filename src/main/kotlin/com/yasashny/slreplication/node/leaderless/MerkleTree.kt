package com.yasashny.slreplication.node.leaderless

import com.yasashny.slreplication.common.model.VersionedEntry
import java.security.MessageDigest

class MerkleTree(entries: List<VersionedEntry>) {

    val leafHashes: List<String>
    val rootHash: String

    init {
        val buckets = Array(NUM_BUCKETS) { mutableListOf<VersionedEntry>() }
        for (entry in entries) {
            val bucket = keyToBucket(entry.key)
            buckets[bucket].add(entry)
        }

        leafHashes = buckets.map { bucket ->
            val sorted = bucket.sortedBy { it.key }
            val concat = sorted.joinToString("\n") { e ->
                "${e.key}|${e.value}|${e.version?.lamport ?: 0}|${e.version?.nodeId ?: ""}|${e.operationId ?: ""}"
            }
            sha256(concat)
        }

        rootHash = buildTree(leafHashes)
    }

    fun diffBuckets(otherLeafHashes: List<String>): List<Int> {
        return leafHashes.indices.filter { i ->
            i < otherLeafHashes.size && leafHashes[i] != otherLeafHashes[i]
        }
    }

    private fun buildTree(leaves: List<String>): String {
        if (leaves.size == 1) return leaves[0]
        val parents = mutableListOf<String>()
        var i = 0
        while (i < leaves.size) {
            val left = leaves[i]
            val right = if (i + 1 < leaves.size) leaves[i + 1] else left
            parents.add(sha256(left + right))
            i += 2
        }
        return buildTree(parents)
    }

    private fun sha256(input: String): String {
        val md = MessageDigest.getInstance("SHA-256")
        return md.digest(input.toByteArray(Charsets.UTF_8))
            .joinToString("") { "%02x".format(it) }
    }

    companion object {
        const val NUM_BUCKETS = 16

        fun keyToBucket(key: String): Int = (key.hashCode() and 0x7FFFFFFF) % NUM_BUCKETS
    }
}
