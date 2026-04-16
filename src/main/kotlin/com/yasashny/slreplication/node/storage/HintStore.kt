package com.yasashny.slreplication.node.storage

import com.yasashny.slreplication.common.model.HintRecord
import java.util.concurrent.ConcurrentHashMap

class HintStore {
    private val hintsByHome = ConcurrentHashMap<String, MutableList<HintRecord>>()
    private val hintsByOpId = ConcurrentHashMap<String, HintRecord>()

    fun addHint(hint: HintRecord): Boolean {
        if (hintsByOpId.putIfAbsent(hint.operationId, hint) != null) return false
        hintsByHome.getOrPut(hint.intendedHomeNodeId) { mutableListOf() }.add(hint)
        return true
    }

    fun getHintsForNode(homeNodeId: String): List<HintRecord> {
        return hintsByHome[homeNodeId]?.toList() ?: emptyList()
    }

    fun removeHint(operationId: String) {
        val hint = hintsByOpId.remove(operationId) ?: return
        hintsByHome[hint.intendedHomeNodeId]?.removeIf { it.operationId == operationId }
    }

    fun getAllHints(): List<HintRecord> = hintsByOpId.values.toList()

    fun wipe() {
        hintsByHome.clear()
        hintsByOpId.clear()
    }

    fun size(): Int = hintsByOpId.size
}
