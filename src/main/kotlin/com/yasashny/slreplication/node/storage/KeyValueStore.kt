package com.yasashny.slreplication.node.storage

import com.yasashny.slreplication.common.model.Version
import com.yasashny.slreplication.common.model.VersionedEntry
import java.util.concurrent.ConcurrentHashMap

class KeyValueStore {
    private val data = ConcurrentHashMap<String, String>()
    private val versions = ConcurrentHashMap<String, Version>()

    fun put(key: String, value: String): String? {
        return data.put(key, value)
    }

    fun get(key: String): String? {
        return data[key]
    }

    fun getVersion(key: String): Version? {
        return versions[key]
    }

    /**
     * Put with LWW (Last Write Wins) version comparison.
     * Returns true if the value was applied (newer version), false if rejected.
     */
    fun putVersioned(key: String, value: String, version: Version): Boolean {
        var applied = false
        versions.compute(key) { _, existing ->
            if (existing == null || version > existing) {
                data[key] = value
                applied = true
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
            VersionedEntry(k, v, versions[k])
        }
    }

    fun size(): Int = data.size
}
