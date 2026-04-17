package com.yasashny.slreplication.cli

import com.yasashny.slreplication.common.model.NodeInfo
import com.yasashny.slreplication.common.network.ConnectionPool
import com.yasashny.slreplication.common.network.TcpClient
import java.util.concurrent.ConcurrentHashMap

class NodeRegistry {
    private val nodes = ConcurrentHashMap<String, NodeInfo>()
    private val connectionPool = ConnectionPool()

    fun add(nodeId: String, host: String, port: Int): Result<NodeInfo> {
        if (nodes.containsKey(nodeId)) {
            return Result.failure(Exception("Node $nodeId already exists"))
        }
        val client = TcpClient(host, port)
        if (!client.connect()) {
            return Result.failure(Exception("Failed to connect to node $nodeId at $host:$port"))
        }
        val info = NodeInfo(nodeId, host, port)
        nodes[nodeId] = info
        connectionPool.add(nodeId, client)
        return Result.success(info)
    }

    fun remove(nodeId: String): Boolean {
        if (!nodes.containsKey(nodeId)) return false
        nodes.remove(nodeId)
        connectionPool.remove(nodeId)
        return true
    }

    fun list(): List<NodeInfo> = nodes.values.toList()
    fun isRegistered(nodeId: String): Boolean = nodes.containsKey(nodeId)
    fun getConnection(nodeId: String): TcpClient? = connectionPool.get(nodeId)
    fun allConnections(): Map<String, TcpClient> = connectionPool.getAll()
    fun nodeIds(): Set<String> = nodes.keys.toSet()
    fun randomId(): String? = nodes.keys.randomOrNull()
    fun firstId(): String? = nodes.keys.firstOrNull()
    fun size(): Int = nodes.size

    fun close() {
        connectionPool.closeAll()
    }
}
