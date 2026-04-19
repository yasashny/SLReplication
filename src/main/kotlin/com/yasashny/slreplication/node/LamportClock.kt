package com.yasashny.slreplication.node

import com.yasashny.slreplication.common.model.Version
import java.util.concurrent.atomic.AtomicLong

class LamportClock(private val nodeId: String) {
    private val counter = AtomicLong(0)

    fun nextVersion(): Version {
        val lamport = counter.incrementAndGet()
        return Version(lamport, nodeId)
    }

    fun merge(remoteLamport: Long) {
        counter.updateAndGet { current -> maxOf(current, remoteLamport) + 1 }
    }

    fun reset() {
        counter.set(0)
    }
}
