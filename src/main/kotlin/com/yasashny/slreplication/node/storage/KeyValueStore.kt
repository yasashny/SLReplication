package com.yasashny.slreplication.node.storage

import java.util.concurrent.ConcurrentHashMap

class KeyValueStore {
    private val data = ConcurrentHashMap<String, String>()

    fun put(key: String, value: String): String? {
        return data.put(key, value)
    }

    fun get(key: String): String? {
        return data[key]
    }

    fun dump(): Map<String, String> {
        return data.toMap()
    }

    fun size(): Int = data.size
}
