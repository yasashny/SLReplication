package com.yasashny.slreplication.cli

import com.yasashny.slreplication.common.model.ClusterConfig
import com.yasashny.slreplication.common.model.ClusterMode
import com.yasashny.slreplication.common.model.NodeInfo
import com.yasashny.slreplication.common.model.ReplicationMode
import com.yasashny.slreplication.common.model.Topology
import com.yasashny.slreplication.common.model.WriteQuorumMode
import java.util.UUID

class ClusterManager {
    private val clientId = "cli-${UUID.randomUUID().toString().take(8)}"
    private val registry = NodeRegistry()
    private val state = ClusterState()
    private val broadcaster = ConfigBroadcaster(registry, state, clientId)
    private val sender = RequestSender(registry, state)
    private val clientOps = ClientOps(registry, state, sender, clientId)
    private val adminOps = LeaderlessAdminOps(registry, state, sender, clientOps::dump, clientId)


    fun addNode(nodeId: String, host: String, port: Int): Result<String> =
        registry.add(nodeId, host, port).map {
            broadcaster.broadcast()
            "Node $nodeId added at ${it.host}:${it.port}"
        }

    fun removeNode(nodeId: String): Result<String> {
        if (!registry.remove(nodeId)) {
            return Result.failure(Exception("Node $nodeId not found"))
        }
        if (state.leaderId == nodeId) state.leaderId = null
        state.leaderNodeIds = state.leaderNodeIds.filter { it != nodeId }
        if (state.starCenterId == nodeId) state.starCenterId = null
        broadcaster.broadcast()
        return Result.success("Node $nodeId removed")
    }

    fun listNodes(): List<NodeInfo> = registry.list()


    fun setLeader(nodeId: String): Result<String> {
        if (!registry.isRegistered(nodeId)) return Result.failure(Exception("Node $nodeId not found"))
        state.leaderId = nodeId
        broadcaster.broadcast()
        return Result.success("Leader set to $nodeId")
    }

    fun getLeader(): String? = state.leaderId

    fun setMode(newMode: ClusterMode): Result<String> {
        state.mode = newMode
        broadcaster.broadcast()
        return Result.success("Mode set to $newMode")
    }

    fun getMode(): ClusterMode = state.mode

    fun setTopology(newTopology: Topology): Result<String> {
        if (state.mode != ClusterMode.MULTI) {
            return Result.failure(Exception("Topology is only applicable in MULTI mode"))
        }
        state.topology = newTopology
        broadcaster.broadcast()
        return Result.success("Topology set to $newTopology")
    }

    fun setStarCenter(nodeId: String): Result<String> {
        if (!registry.isRegistered(nodeId)) return Result.failure(Exception("Node $nodeId not found"))
        if (state.topology != Topology.STAR) {
            return Result.failure(Exception("Star center is only applicable with STAR topology"))
        }
        state.starCenterId = nodeId
        broadcaster.broadcast()
        return Result.success("Star center set to $nodeId")
    }

    fun setLeaders(nodeIds: List<String>): Result<String> {
        if (state.mode != ClusterMode.MULTI) {
            return Result.failure(Exception("setLeaders is only applicable in MULTI mode"))
        }
        for (id in nodeIds) {
            if (!registry.isRegistered(id)) return Result.failure(Exception("Node $id not found"))
        }
        state.leaderNodeIds = nodeIds.toList()
        broadcaster.broadcast()
        return Result.success("Leaders set to ${nodeIds.joinToString(", ")}")
    }

    fun getLeaderNodeIds(): List<String> = state.leaderNodeIds


    fun setHomeReplicas(nodeIds: List<String>): Result<String> {
        if (nodeIds.size != 5) return Result.failure(Exception("Exactly 5 home replicas required, got ${nodeIds.size}"))
        for (id in nodeIds) if (!registry.isRegistered(id)) return Result.failure(Exception("Node $id not found"))
        state.homeReplicaIds = nodeIds.toList()
        broadcaster.broadcast()
        return Result.success("Home replicas set to ${nodeIds.joinToString(", ")}")
    }

    fun setSpareNodes(nodeIds: List<String>): Result<String> {
        if (nodeIds.size != 2) return Result.failure(Exception("Exactly 2 spare nodes required, got ${nodeIds.size}"))
        for (id in nodeIds) if (!registry.isRegistered(id)) return Result.failure(Exception("Node $id not found"))
        state.spareNodeIds = nodeIds.toList()
        broadcaster.broadcast()
        return Result.success("Spare nodes set to ${nodeIds.joinToString(", ")}")
    }

    fun setQuorum(w: Int, r: Int): Result<String> {
        if (w < 1 || w > 5) return Result.failure(Exception("W must be between 1 and 5"))
        if (r < 1 || r > 5) return Result.failure(Exception("R must be between 1 and 5"))
        if (w + r <= 5) return Result.failure(Exception("W + R must be > 5 (got $w + $r = ${w + r}). Error: INVALID_CONFIG"))
        state.writeQuorum = w
        state.readQuorum = r
        broadcaster.broadcast()
        return Result.success("Quorum set to W=$w, R=$r")
    }

    fun setWriteQuorumMode(wqMode: WriteQuorumMode): Result<String> {
        state.writeQuorumMode = wqMode
        broadcaster.broadcast()
        return Result.success("Write quorum mode set to $wqMode")
    }


    fun setReplicationMode(rMode: ReplicationMode): Result<String> {
        state.replicationMode = rMode
        broadcaster.broadcast()
        return Result.success("Replication mode set to $rMode")
    }

    fun setReplicationFactor(rf: Int): Result<String> {
        if (rf < 1) return Result.failure(Exception("RF must be >= 1"))
        if (rf > registry.size()) {
            return Result.failure(Exception("RF ($rf) cannot be greater than cluster size (${registry.size()})"))
        }
        state.replicationFactor = rf
        if (state.replicationMode == ReplicationMode.SEMI_SYNC && state.semiSyncAcks >= rf) {
            state.semiSyncAcks = maxOf(1, rf - 1)
        }
        broadcaster.broadcast()
        return Result.success("Replication factor set to $rf")
    }

    fun setSemiSyncAcks(k: Int): Result<String> {
        if (state.replicationMode != ReplicationMode.SEMI_SYNC) {
            return Result.failure(Exception("semiSyncAcks is only applicable in SEMI_SYNC mode"))
        }
        if (k < 1) return Result.failure(Exception("K must be >= 1"))
        if (k > state.replicationFactor - 1) {
            return Result.failure(Exception("K ($k) must be <= RF-1 (${state.replicationFactor - 1})"))
        }
        state.semiSyncAcks = k
        broadcaster.broadcast()
        return Result.success("Semi-sync ACKs (K) set to $k")
    }

    fun setReplicationDelay(minMs: Long, maxMs: Long): Result<String> {
        if (minMs < 0 || maxMs < 0) return Result.failure(Exception("Delay cannot be negative"))
        if (minMs > maxMs) return Result.failure(Exception("Min delay cannot be greater than max delay"))
        state.replicationDelayMinMs = minMs
        state.replicationDelayMaxMs = maxMs
        broadcaster.broadcast()
        return Result.success("Replication delay set to $minMs-$maxMs ms")
    }


    fun put(key: String, value: String, targetNodeId: String? = null) = clientOps.put(key, value, targetNodeId)
    fun get(key: String, targetNodeId: String? = null) = clientOps.get(key, targetNodeId)
    fun dump(targetNodeId: String? = null) = clientOps.dump(targetNodeId)
    fun getAll(key: String) = clientOps.getAll(key)
    fun clusterDump() = clientOps.clusterDump()


    fun dumpHints(targetNodeId: String? = null) = adminOps.dumpHints(targetNodeId)
    fun runHintedHandoff(targetNodeId: String? = null) = adminOps.runHintedHandoff(targetNodeId)
    fun showMerkleRoot(nodeId: String) = adminOps.showMerkleRoot(nodeId)
    fun wipeNodeData(nodeId: String) = adminOps.wipeNodeData(nodeId)
    fun runAntiEntropyCluster() = adminOps.runAntiEntropyCluster()
    fun getLeaderlessStats() = adminOps.getLeaderlessStats()


    fun getConfig(): ClusterConfig = state.buildConfig(registry.list())

    fun close() = registry.close()
}
