package com.yasashny.slreplication.cli

import com.yasashny.slreplication.common.model.ClusterConfig
import com.yasashny.slreplication.common.model.ClusterMode
import com.yasashny.slreplication.common.model.NodeInfo
import com.yasashny.slreplication.common.model.ReplicationMode
import com.yasashny.slreplication.common.model.Topology
import com.yasashny.slreplication.common.model.WriteQuorumMode

class ClusterState {
    @Volatile var leaderId: String? = null
    @Volatile var mode: ClusterMode = ClusterMode.SINGLE
    @Volatile var topology: Topology = Topology.MESH
    @Volatile var starCenterId: String? = null

    @Volatile var leaderNodeIds: List<String> = emptyList()
    @Volatile var homeReplicaIds: List<String> = emptyList()
    @Volatile var spareNodeIds: List<String> = emptyList()

    @Volatile var writeQuorum: Int = 3
    @Volatile var readQuorum: Int = 3
    @Volatile var writeQuorumMode: WriteQuorumMode = WriteQuorumMode.STRICT

    @Volatile var replicationMode: ReplicationMode = ReplicationMode.ASYNC
    @Volatile var replicationFactor: Int = 1
    @Volatile var semiSyncAcks: Int = 1

    @Volatile var replicationDelayMinMs: Long = 0
    @Volatile var replicationDelayMaxMs: Long = 0

    fun buildConfig(nodes: List<NodeInfo>): ClusterConfig = ClusterConfig(
        nodes = nodes,
        leaderId = leaderId,
        replicationMode = replicationMode,
        replicationFactor = replicationFactor,
        semiSyncAcks = semiSyncAcks,
        replicationDelayMinMs = replicationDelayMinMs,
        replicationDelayMaxMs = replicationDelayMaxMs,
        mode = mode,
        topology = topology,
        starCenterId = starCenterId,
        leaderNodeIds = leaderNodeIds,
        homeReplicaIds = homeReplicaIds,
        spareNodeIds = spareNodeIds,
        writeQuorum = writeQuorum,
        readQuorum = readQuorum,
        writeQuorumMode = writeQuorumMode
    )
}
