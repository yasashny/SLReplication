package com.yasashny.slreplication.cli

import com.yasashny.slreplication.common.model.*
import com.yasashny.slreplication.common.model.LeaderlessPayload
import com.yasashny.slreplication.common.network.ConnectionPool
import com.yasashny.slreplication.common.network.TcpClient
import kotlinx.coroutines.runBlocking
import kotlin.random.Random
import org.slf4j.LoggerFactory
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

class ClusterManager {
    private val logger = LoggerFactory.getLogger(ClusterManager::class.java)

    private val nodes = ConcurrentHashMap<String, NodeInfo>()
    private val connectionPool = ConnectionPool()

    private var leaderId: String? = null
    private var replicationMode = ReplicationMode.ASYNC
    private var replicationFactor = 1
    private var semiSyncAcks = 1

    private var replicationDelayMinMs = 0L
    private var replicationDelayMaxMs = 0L

    private var mode = ClusterMode.SINGLE
    private var topology = Topology.MESH
    private var starCenterId: String? = null
    private var leaderNodeIds = mutableListOf<String>()

    private var homeReplicaIds = mutableListOf<String>()
    private var spareNodeIds = mutableListOf<String>()
    private var writeQuorum = 3
    private var readQuorum = 3
    private var writeQuorumMode = WriteQuorumMode.STRICT

    private val clientId = "cli-${UUID.randomUUID().toString().take(8)}"
    private val baseRequestTimeoutMs = 10000L


    fun addNode(nodeId: String, host: String, port: Int): Result<String> {
        if (nodes.containsKey(nodeId)) {
            return Result.failure(Exception("Node $nodeId already exists"))
        }
        val nodeInfo = NodeInfo(nodeId, host, port)
        nodes[nodeId] = nodeInfo
        val client = TcpClient(host, port)
        if (client.connect()) {
            connectionPool.add(nodeId, client)
            broadcastClusterUpdate()
            return Result.success("Node $nodeId added at $host:$port")
        } else {
            nodes.remove(nodeId)
            return Result.failure(Exception("Failed to connect to node $nodeId at $host:$port"))
        }
    }

    fun removeNode(nodeId: String): Result<String> {
        if (!nodes.containsKey(nodeId)) {
            return Result.failure(Exception("Node $nodeId not found"))
        }

        nodes.remove(nodeId)
        connectionPool.remove(nodeId)

        if (leaderId == nodeId) leaderId = null
        leaderNodeIds.remove(nodeId)
        if (starCenterId == nodeId) starCenterId = null

        broadcastClusterUpdate()
        return Result.success("Node $nodeId removed")
    }

    fun listNodes(): List<NodeInfo> = nodes.values.toList()

    fun setLeader(nodeId: String): Result<String> {
        if (!nodes.containsKey(nodeId)) {
            return Result.failure(Exception("Node $nodeId not found"))
        }
        leaderId = nodeId
        broadcastClusterUpdate()
        return Result.success("Leader set to $nodeId")
    }

    fun getLeader(): String? = leaderId


    fun setMode(newMode: ClusterMode): Result<String> {
        mode = newMode
        broadcastClusterUpdate()
        return Result.success("Mode set to $newMode")
    }

    fun getMode(): ClusterMode = mode

    fun setTopology(newTopology: Topology): Result<String> {
        if (mode != ClusterMode.MULTI) {
            return Result.failure(Exception("Topology is only applicable in MULTI mode"))
        }
        topology = newTopology
        broadcastClusterUpdate()
        return Result.success("Topology set to $newTopology")
    }

    fun setStarCenter(nodeId: String): Result<String> {
        if (!nodes.containsKey(nodeId)) {
            return Result.failure(Exception("Node $nodeId not found"))
        }
        if (topology != Topology.STAR) {
            return Result.failure(Exception("Star center is only applicable with STAR topology"))
        }
        starCenterId = nodeId
        broadcastClusterUpdate()
        return Result.success("Star center set to $nodeId")
    }

    fun setLeaders(nodeIds: List<String>): Result<String> {
        if (mode != ClusterMode.MULTI) {
            return Result.failure(Exception("setLeaders is only applicable in MULTI mode"))
        }
        for (id in nodeIds) {
            if (!nodes.containsKey(id)) {
                return Result.failure(Exception("Node $id not found"))
            }
        }
        leaderNodeIds = nodeIds.toMutableList()
        broadcastClusterUpdate()
        return Result.success("Leaders set to ${nodeIds.joinToString(", ")}")
    }

    fun getLeaderNodeIds(): List<String> = leaderNodeIds.toList()


    fun setHomeReplicas(nodeIds: List<String>): Result<String> {
        if (nodeIds.size != 5) return Result.failure(Exception("Exactly 5 home replicas required, got ${nodeIds.size}"))
        for (id in nodeIds) {
            if (!nodes.containsKey(id)) return Result.failure(Exception("Node $id not found"))
        }
        homeReplicaIds = nodeIds.toMutableList()
        broadcastClusterUpdate()
        return Result.success("Home replicas set to ${nodeIds.joinToString(", ")}")
    }

    fun setSpareNodes(nodeIds: List<String>): Result<String> {
        if (nodeIds.size != 2) return Result.failure(Exception("Exactly 2 spare nodes required, got ${nodeIds.size}"))
        for (id in nodeIds) {
            if (!nodes.containsKey(id)) return Result.failure(Exception("Node $id not found"))
        }
        spareNodeIds = nodeIds.toMutableList()
        broadcastClusterUpdate()
        return Result.success("Spare nodes set to ${nodeIds.joinToString(", ")}")
    }

    fun setQuorum(w: Int, r: Int): Result<String> {
        if (w < 1 || w > 5) return Result.failure(Exception("W must be between 1 and 5"))
        if (r < 1 || r > 5) return Result.failure(Exception("R must be between 1 and 5"))
        if (w + r <= 5) return Result.failure(Exception("W + R must be > 5 (got $w + $r = ${w + r}). Error: INVALID_CONFIG"))
        writeQuorum = w
        readQuorum = r
        broadcastClusterUpdate()
        return Result.success("Quorum set to W=$w, R=$r")
    }

    fun setWriteQuorumMode(wqMode: WriteQuorumMode): Result<String> {
        writeQuorumMode = wqMode
        broadcastClusterUpdate()
        return Result.success("Write quorum mode set to $wqMode")
    }


    private val antiEntropyOrchestrator by lazy {
        AntiEntropyOrchestrator(
            sendRequest = { target, msg, timeout -> sendRequest(target, msg, timeout) },
            clientId = clientId
        )
    }

    fun dumpHints(targetNodeId: String? = null): Result<Message> {
        val target = targetNodeId ?: spareNodeIds.firstOrNull() ?: nodes.keys.firstOrNull()
            ?: return Result.failure(Exception("No nodes available"))
        return sendRequest(target, Message(
            type = MessageType.CLIENT_DUMP_HINTS,
            requestId = UUID.randomUUID().toString(),
            clientId = clientId
        ))
    }

    fun runHintedHandoff(targetNodeId: String? = null): Map<String, Result<Message>> {
        val targets = if (targetNodeId != null) listOf(targetNodeId) else spareNodeIds.toList()
        return targets.associateWith { nodeId ->
            sendRequest(nodeId, Message(
                type = MessageType.RUN_HINTED_HANDOFF,
                requestId = UUID.randomUUID().toString(),
                clientId = clientId
            ), 30000)
        }
    }

    fun showMerkleRoot(nodeId: String): Result<Message> {
        return sendRequest(nodeId, Message(
            type = MessageType.MERKLE_ROOT_REQUEST,
            requestId = UUID.randomUUID().toString(),
            clientId = clientId
        ))
    }

    fun wipeNodeData(nodeId: String): Result<Message> {
        return sendRequest(nodeId, Message(
            type = MessageType.WIPE_DATA,
            requestId = UUID.randomUUID().toString(),
            clientId = clientId
        ))
    }

    fun runAntiEntropyCluster(): Result<String> {
        val delayMs = if (replicationDelayMinMs > 0 || replicationDelayMaxMs > 0) {
            Random.nextLong(replicationDelayMinMs, replicationDelayMaxMs + 1)
        } else 0L
        return antiEntropyOrchestrator.run(homeReplicaIds.toList(), delayMs)
    }

    fun getLeaderlessStats(): Map<String, Long> {
        val allStats = mutableMapOf<String, Long>()
        for (nodeId in nodes.keys) {
            val result = dump(nodeId)
            result.getOrNull()?.leaderless?.stats?.forEach { (key, value) ->
                allStats[key] = (allStats[key] ?: 0) + value
            }
        }
        return allStats
    }


    fun setReplicationMode(rMode: ReplicationMode): Result<String> {
        replicationMode = rMode
        broadcastClusterUpdate()
        return Result.success("Replication mode set to $rMode")
    }

    fun setReplicationFactor(rf: Int): Result<String> {
        if (rf < 1) return Result.failure(Exception("RF must be >= 1"))
        if (rf > nodes.size) {
            return Result.failure(Exception("RF ($rf) cannot be greater than cluster size (${nodes.size})"))
        }
        replicationFactor = rf
        if (replicationMode == ReplicationMode.SEMI_SYNC && semiSyncAcks >= rf) {
            semiSyncAcks = maxOf(1, rf - 1)
        }
        broadcastClusterUpdate()
        return Result.success("Replication factor set to $rf")
    }

    fun setSemiSyncAcks(k: Int): Result<String> {
        if (replicationMode != ReplicationMode.SEMI_SYNC) {
            return Result.failure(Exception("semiSyncAcks is only applicable in SEMI_SYNC mode"))
        }
        if (k < 1) return Result.failure(Exception("K must be >= 1"))
        if (k > replicationFactor - 1) {
            return Result.failure(Exception("K ($k) must be <= RF-1 (${replicationFactor - 1})"))
        }
        semiSyncAcks = k
        broadcastClusterUpdate()
        return Result.success("Semi-sync ACKs (K) set to $k")
    }

    fun setReplicationDelay(minMs: Long, maxMs: Long): Result<String> {
        if (minMs < 0 || maxMs < 0) return Result.failure(Exception("Delay cannot be negative"))
        if (minMs > maxMs) return Result.failure(Exception("Min delay cannot be greater than max delay"))
        replicationDelayMinMs = minMs
        replicationDelayMaxMs = maxMs
        broadcastClusterUpdate()
        return Result.success("Replication delay set to $minMs-$maxMs ms")
    }


    fun put(key: String, value: String, targetNodeId: String? = null): Result<Message> {
        val target = targetNodeId ?: when (mode) {
            ClusterMode.SINGLE -> leaderId
            ClusterMode.MULTI -> leaderNodeIds.randomOrNull()
            ClusterMode.LEADERLESS -> nodes.keys.randomOrNull()
        } ?: return Result.failure(Exception("No nodes available"))

        return sendRequest(target, Message(
            type = MessageType.CLIENT_PUT,
            requestId = UUID.randomUUID().toString(),
            clientId = clientId,
            key = key,
            value = value
        ))
    }

    fun get(key: String, targetNodeId: String? = null): Result<Message> {
        val target = targetNodeId ?: when (mode) {
            ClusterMode.SINGLE -> leaderId
            ClusterMode.MULTI -> nodes.keys.randomOrNull()
            ClusterMode.LEADERLESS -> nodes.keys.randomOrNull()
        } ?: return Result.failure(Exception("No nodes available"))

        return sendRequest(target, Message(
            type = MessageType.CLIENT_GET,
            requestId = UUID.randomUUID().toString(),
            clientId = clientId,
            key = key
        ))
    }

    fun dump(targetNodeId: String? = null): Result<Message> {
        val target = targetNodeId
            ?: (if (mode == ClusterMode.SINGLE) leaderId else null)
            ?: nodes.keys.firstOrNull()
            ?: return Result.failure(Exception("No nodes in cluster"))

        return sendRequest(target, Message(
            type = MessageType.CLIENT_DUMP,
            requestId = UUID.randomUUID().toString(),
            clientId = clientId
        ))
    }

    fun getAll(key: String): Map<String, Result<Message>> {
        return nodes.keys.sorted().associateWith { nodeId -> get(key, nodeId) }
    }

    fun clusterDump(): Map<String, Result<Message>> {
        return nodes.keys.sorted().associateWith { nodeId -> dump(nodeId) }
    }

    private fun getRequestTimeoutMs(): Long = baseRequestTimeoutMs + replicationDelayMaxMs

    private fun sendRequest(target: String, request: Message, timeoutMs: Long? = null): Result<Message> {
        val client = connectionPool.get(target)
            ?: return Result.failure(Exception("Node $target not connected"))
        return runBlocking {
            val response = client.sendAndWait(request, timeoutMs ?: getRequestTimeoutMs())
            if (response != null) Result.success(response)
            else Result.failure(Exception("Timeout waiting for response from $target"))
        }
    }


    private fun broadcastClusterUpdate() {
        val config = buildConfig()
        val message = Message(
            type = MessageType.CLUSTER_UPDATE,
            requestId = UUID.randomUUID().toString(),
            clientId = clientId,
            clusterConfig = config
        )
        runBlocking {
            for ((nodeId, client) in connectionPool.getAll()) {
                try {
                    val response = client.sendAndWait(message, 5000)
                    if (response?.status == "OK") {
                        logger.debug("Cluster update acknowledged by $nodeId")
                    } else {
                        logger.warn("Node $nodeId did not acknowledge cluster update: ${response?.errorMessage}")
                    }
                } catch (e: Exception) {
                    logger.warn("Failed to send cluster update to $nodeId: ${e.message}")
                }
            }
        }
    }

    private fun buildConfig(): ClusterConfig = ClusterConfig(
        nodes = nodes.values.toList(),
        leaderId = leaderId,
        replicationMode = replicationMode,
        replicationFactor = replicationFactor,
        semiSyncAcks = semiSyncAcks,
        replicationDelayMinMs = replicationDelayMinMs,
        replicationDelayMaxMs = replicationDelayMaxMs,
        mode = mode,
        topology = topology,
        starCenterId = starCenterId,
        leaderNodeIds = leaderNodeIds.toList(),
        homeReplicaIds = homeReplicaIds.toList(),
        spareNodeIds = spareNodeIds.toList(),
        writeQuorum = writeQuorum,
        readQuorum = readQuorum,
        writeQuorumMode = writeQuorumMode
    )

    fun getConfig(): ClusterConfig = buildConfig()

    fun close() {
        connectionPool.closeAll()
    }
}
