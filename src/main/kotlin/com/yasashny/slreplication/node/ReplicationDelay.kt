package com.yasashny.slreplication.node

import com.yasashny.slreplication.common.model.ClusterConfig
import kotlin.random.Random

internal fun computeReplicationDelay(config: ClusterConfig): Long {
    return if (config.replicationDelayMinMs > 0 || config.replicationDelayMaxMs > 0) {
        Random.nextLong(config.replicationDelayMinMs, config.replicationDelayMaxMs + 1)
    } else 0L
}
