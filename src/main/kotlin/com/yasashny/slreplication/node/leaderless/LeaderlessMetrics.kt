package com.yasashny.slreplication.node.leaderless

import java.util.concurrent.atomic.AtomicLong

class LeaderlessMetrics {
    val staleReadCount = AtomicLong(0)
    val readRepairCount = AtomicLong(0)
    val hintsCreated = AtomicLong(0)
    val hintsDelivered = AtomicLong(0)
    val antiEntropyRecoveredKeys = AtomicLong(0)

    fun toMap(): Map<String, Long> = mapOf(
        "staleReadCount" to staleReadCount.get(),
        "readRepairCount" to readRepairCount.get(),
        "hintsCreated" to hintsCreated.get(),
        "hintsDelivered" to hintsDelivered.get(),
        "antiEntropyRecoveredKeys" to antiEntropyRecoveredKeys.get()
    )

    fun reset() {
        staleReadCount.set(0)
        readRepairCount.set(0)
        hintsCreated.set(0)
        hintsDelivered.set(0)
        antiEntropyRecoveredKeys.set(0)
    }
}
