package com.yasashny.slreplication.node.storage

import com.yasashny.slreplication.common.model.Version
import com.yasashny.slreplication.common.model.VersionedEntry
import com.yasashny.slreplication.node.leaderless.MerkleTree
import java.util.concurrent.ConcurrentHashMap

class KeyValueStore {
    private val data = ConcurrentHashMap<String, String>()
    private val versions = ConcurrentHashMap<String, Version>()
    private val operationIds = ConcurrentHashMap<String, String>()

    fun put(key: String, value: String): String? {
        return data.put(key, value)
    }

    fun get(key: String): String? {
        return data[key]
    }

    fun getVersion(key: String): Version? {
        return versions[key]
    }

    fun putVersioned(key: String, value: String, version: Version, operationId: String? = null): Boolean {
        var applied = false
        versions.compute(key) { _, existing ->
            if (existing == null || version > existing) {
                data[key] = value
                applied = true
                if (operationId != null) operationIds[key] = operationId
                version
            } else {
                existing
            }
        }
        return applied
    }

    fun dump(): Map<String, String> {
        return data.toMap()
    }

    fun dumpVersioned(): List<VersionedEntry> {
        return data.map { (k, v) ->
            VersionedEntry(k, v, versions[k], operationIds[k])
        }
    }

    fun size(): Int = data.size

    fun getOperationId(key: String): String? = operationIds[key]

    fun wipe() {
        data.clear()
        versions.clear()
        operationIds.clear()
    }

    fun getRecordsForBuckets(buckets: Set<Int>): List<VersionedEntry> {
        return data.entries
            .filter { MerkleTree.keyToBucket(it.key) in buckets }
            .map { (k, v) -> VersionedEntry(k, v, versions[k], operationIds[k]) }
    }
}
