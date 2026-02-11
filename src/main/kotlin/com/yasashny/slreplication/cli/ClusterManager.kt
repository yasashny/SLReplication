package com.yasashny.slreplication.cli

import com.yasashny.slreplication.common.model.*
import com.yasashny.slreplication.common.network.ConnectionPool
import com.yasashny.slreplication.common.network.TcpClient
import kotlinx.coroutines.runBlocking
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

    private val clientId = "cli-${UUID.randomUUID().toString().take(8)}"
    private val requestTimeoutMs = 10000L

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

        if (leaderId == nodeId) {
            leaderId = null
        }

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

    fun setReplicationMode(mode: ReplicationMode): Result<String> {
        replicationMode = mode
        broadcastClusterUpdate()
        return Result.success("Replication mode set to $mode")
    }

    fun setReplicationFactor(rf: Int): Result<String> {
        if (rf < 1) {
            return Result.failure(Exception("RF must be >= 1"))
        }
        if (rf > nodes.size) {
            return Result.failure(Exception("RF ($rf) cannot be greater than cluster size (${nodes.size})"))
        }
        replicationFactor = rf
        if (replicationMode == ReplicationMode.SEMI_SYNC) {
            if (semiSyncAcks >= rf) {
                semiSyncAcks = maxOf(1, rf - 1)
            }
        }
        broadcastClusterUpdate()
        return Result.success("Replication factor set to $rf")
    }

    fun setSemiSyncAcks(k: Int): Result<String> {
        if (replicationMode != ReplicationMode.SEMI_SYNC) {
            return Result.failure(Exception("semiSyncAcks is only applicable in SEMI_SYNC mode"))
        }
        if (k < 1) {
            return Result.failure(Exception("K must be >= 1"))
        }
        if (k > replicationFactor - 1) {
            return Result.failure(Exception("K ($k) must be <= RF-1 (${replicationFactor - 1})"))
        }

        semiSyncAcks = k
        broadcastClusterUpdate()
        return Result.success("Semi-sync ACKs (K) set to $k")
    }

    fun setReplicationDelay(minMs: Long, maxMs: Long): Result<String> {
        if (minMs < 0 || maxMs < 0) {
            return Result.failure(Exception("Delay cannot be negative"))
        }
        if (minMs > maxMs) {
            return Result.failure(Exception("Min delay cannot be greater than max delay"))
        }

        replicationDelayMinMs = minMs
        replicationDelayMaxMs = maxMs
        broadcastClusterUpdate()
        return Result.success("Replication delay set to $minMs-$maxMs ms")
    }

    fun put(key: String, value: String, targetNodeId: String? = null): Result<Message> {
        val target = targetNodeId ?: leaderId
            ?: return Result.failure(Exception("No leader set and no target specified"))

        val client = connectionPool.get(target)
            ?: return Result.failure(Exception("Node $target not connected"))

        val request = Message(
            type = MessageType.CLIENT_PUT,
            requestId = UUID.randomUUID().toString(),
            clientId = clientId,
            key = key,
            value = value
        )

        return runBlocking {
            val response = client.sendAndWait(request, requestTimeoutMs)
            if (response != null) {
                Result.success(response)
            } else {
                Result.failure(Exception("Timeout waiting for response from $target"))
            }
        }
    }

    fun get(key: String, targetNodeId: String? = null): Result<Message> {
        val target = targetNodeId ?: leaderId
            ?: return Result.failure(Exception("No leader set and no target specified"))

        val client = connectionPool.get(target)
            ?: return Result.failure(Exception("Node $target not connected"))

        val request = Message(
            type = MessageType.CLIENT_GET,
            requestId = UUID.randomUUID().toString(),
            clientId = clientId,
            key = key
        )

        return runBlocking {
            val response = client.sendAndWait(request, requestTimeoutMs)
            if (response != null) {
                Result.success(response)
            } else {
                Result.failure(Exception("Timeout waiting for response from $target"))
            }
        }
    }

    fun dump(targetNodeId: String? = null): Result<Message> {
        val target = targetNodeId ?: leaderId
            ?: nodes.keys.firstOrNull()
            ?: return Result.failure(Exception("No nodes in cluster"))

        val client = connectionPool.get(target)
            ?: return Result.failure(Exception("Node $target not connected"))

        val request = Message(
            type = MessageType.CLIENT_DUMP,
            requestId = UUID.randomUUID().toString(),
            clientId = clientId
        )

        return runBlocking {
            val response = client.sendAndWait(request, requestTimeoutMs)
            if (response != null) {
                Result.success(response)
            } else {
                Result.failure(Exception("Timeout waiting for response from $target"))
            }
        }
    }

    private fun broadcastClusterUpdate() {
        val config = ClusterConfig(
            nodes = nodes.values.toList(),
            leaderId = leaderId,
            replicationMode = replicationMode,
            replicationFactor = replicationFactor,
            semiSyncAcks = semiSyncAcks,
            replicationDelayMinMs = replicationDelayMinMs,
            replicationDelayMaxMs = replicationDelayMaxMs
        )

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

    fun getConfig(): ClusterConfig = ClusterConfig(
        nodes = nodes.values.toList(),
        leaderId = leaderId,
        replicationMode = replicationMode,
        replicationFactor = replicationFactor,
        semiSyncAcks = semiSyncAcks,
        replicationDelayMinMs = replicationDelayMinMs,
        replicationDelayMaxMs = replicationDelayMaxMs
    )

    fun close() {
        connectionPool.closeAll()
    }
}
