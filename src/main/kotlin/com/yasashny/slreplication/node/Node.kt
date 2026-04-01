package com.yasashny.slreplication.node

import com.yasashny.slreplication.common.model.*
import com.yasashny.slreplication.common.network.ConnectionPool
import com.yasashny.slreplication.common.network.MessageSender
import com.yasashny.slreplication.common.network.TcpClient
import com.yasashny.slreplication.common.network.TcpServer
import com.yasashny.slreplication.node.replication.OperationDeduplicator
import com.yasashny.slreplication.node.replication.ReplicationManager
import com.yasashny.slreplication.node.replication.ReplicationResult
import com.yasashny.slreplication.node.storage.KeyValueStore
import org.slf4j.LoggerFactory
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

class Node(
    val nodeId: String,
    val port: Int
) {
    private val logger = LoggerFactory.getLogger(Node::class.java)
    private val store = KeyValueStore()
    private val deduplicator = OperationDeduplicator()
    private val connectionPool = ConnectionPool()
    private val lamportClock = AtomicLong(0)

    @Volatile
    private var config = ClusterConfig()

    private val replicationManager = ReplicationManager(
        nodeId = nodeId,
        getConfig = { config },
        getNodeConnection = { targetNodeId -> getConnection(targetNodeId) }
    )

    private val server = TcpServer(port) { message, sender ->
        handleMessage(message, sender)
    }

    fun start() {
        logger.info("Starting node $nodeId on port $port")
        server.start()
    }

    fun stop() {
        logger.info("Stopping node $nodeId")
        server.stop()
        replicationManager.stop()
        deduplicator.stop()
        connectionPool.closeAll()
    }

    private fun getConnection(targetNodeId: String): TcpClient? {
        val nodeInfo = config.nodes.find { it.nodeId == targetNodeId } ?: return null
        val client = connectionPool.getOrCreate(targetNodeId, nodeInfo.host, nodeInfo.port) { message ->
            handleIncomingMessage(message)
        }
        if (!client.isConnected()) {
            client.connect()
        }
        return client
    }

    private fun handleIncomingMessage(message: Message) {
        when (message.type) {
            MessageType.REPL_ACK -> {
                val opId = message.operationId ?: message.operationId
                val from = message.fromNodeId ?: message.originNodeId
                if (opId != null && from != null) {
                    replicationManager.handleAck(opId, from)
                }
            }
            else -> {
                logger.debug("Received message via client connection: {}", message.type)
            }
        }
    }

    private suspend fun handleMessage(message: Message, sender: MessageSender) {
        logger.info("Received message: type={}, requestId={}", message.type, message.requestId)

        when (message.type) {
            MessageType.CLIENT_PUT -> handlePut(message, sender)
            MessageType.CLIENT_GET -> handleGet(message, sender)
            MessageType.CLIENT_DUMP -> handleDump(message, sender)
            MessageType.CLUSTER_UPDATE -> handleClusterUpdate(message, sender)
            MessageType.REPL_PUT -> handleReplicationPut(message)
            MessageType.REPL_ACK -> handleReplicationAck(message)
            else -> {
                sender.send(errorResponse(message, ErrorCode.BAD_REQUEST, "Unknown message type: ${message.type}"))
            }
        }
    }

    // ========== PUT ==========

    private suspend fun handlePut(message: Message, sender: MessageSender) {
        val key = message.key
        val value = message.value
        if (key.isNullOrBlank()) {
            sender.send(errorResponse(message, ErrorCode.BAD_REQUEST, "Key is required"))
            return
        }
        if (value == null) {
            sender.send(errorResponse(message, ErrorCode.BAD_REQUEST, "Value is required"))
            return
        }

        if (config.mode == ClusterMode.MULTI) {
            handleMultiPut(key, value, message, sender)
        } else {
            handleSinglePut(key, value, message, sender)
        }
    }

    private suspend fun handleSinglePut(key: String, value: String, message: Message, sender: MessageSender) {
        if (!isLeaderSingle()) {
            sender.send(Message(
                type = MessageType.RESPONSE,
                requestId = message.requestId,
                status = "ERROR",
                errorCode = ErrorCode.NOT_LEADER,
                errorMessage = "This node is not the leader",
                leaderNodeId = config.leaderId
            ))
            return
        }
        store.put(key, value)
        logger.debug("Applied PUT locally: $key = $value")

        when (val result = replicationManager.replicatePut(key, value)) {
            is ReplicationResult.Success -> {
                sender.send(Message(
                    type = MessageType.RESPONSE, requestId = message.requestId, status = "OK"
                ))
            }
            is ReplicationResult.Error -> {
                sender.send(Message(
                    type = MessageType.RESPONSE, requestId = message.requestId,
                    status = "ERROR", errorCode = result.code, errorMessage = result.message
                ))
            }
        }
    }

    private suspend fun handleMultiPut(key: String, value: String, message: Message, sender: MessageSender) {
        if (!isLeaderMulti()) {
            val suggestedLeader = config.leaderNodeIds.randomOrNull()
            sender.send(Message(
                type = MessageType.RESPONSE,
                requestId = message.requestId,
                status = "ERROR",
                errorCode = ErrorCode.NOT_LEADER_FOR_WRITE,
                errorMessage = "This node is not a leader in multi-leader mode",
                leaderNodeId = suggestedLeader
            ))
            return
        }

        val operationId = UUID.randomUUID().toString()
        val newLamport = lamportClock.incrementAndGet()
        val version = Version(newLamport, nodeId)

        // Register in dedup to prevent re-application from ring forwarding
        deduplicator.isDuplicate(operationId)

        store.putVersioned(key, value, version)
        logger.debug("Multi PUT locally: $key=$value, v=($newLamport,$nodeId), opId=$operationId")

        replicationManager.replicateMulti(operationId, key, value, version, nodeId)

        sender.send(Message(
            type = MessageType.RESPONSE, requestId = message.requestId, status = "OK"
        ))
    }

    // ========== GET ==========

    private suspend fun handleGet(message: Message, sender: MessageSender) {
        val key = message.key
        if (key.isNullOrBlank()) {
            sender.send(errorResponse(message, ErrorCode.BAD_REQUEST, "Key is required"))
            return
        }
        val value = store.get(key)
        val version = if (config.mode == ClusterMode.MULTI) store.getVersion(key) else null
        sender.send(Message(
            type = MessageType.RESPONSE,
            requestId = message.requestId,
            status = "OK",
            key = key,
            value = value,
            version = version
        ))
    }

    // ========== DUMP ==========

    private suspend fun handleDump(message: Message, sender: MessageSender) {
        if (config.mode == ClusterMode.MULTI) {
            sender.send(Message(
                type = MessageType.RESPONSE,
                requestId = message.requestId,
                status = "OK",
                data = store.dump(),
                versionedData = store.dumpVersioned()
            ))
        } else {
            sender.send(Message(
                type = MessageType.RESPONSE,
                requestId = message.requestId,
                status = "OK",
                data = store.dump()
            ))
        }
    }

    // ========== CLUSTER UPDATE ==========

    private suspend fun handleClusterUpdate(message: Message, sender: MessageSender) {
        val newConfig = message.clusterConfig
        if (newConfig == null) {
            sender.send(errorResponse(message, ErrorCode.BAD_REQUEST, "Cluster config is required"))
            return
        }
        logger.info("Cluster update: nodes=${newConfig.nodes.map { it.nodeId }}, mode=${newConfig.mode}, " +
                "leader=${newConfig.leaderId}, leaders=${newConfig.leaderNodeIds}, topology=${newConfig.topology}")
        config = newConfig
        sender.send(Message(
            type = MessageType.CLUSTER_UPDATE_ACK,
            requestId = message.requestId,
            status = "OK"
        ))
    }

    // ========== REPLICATION ==========

    private fun handleReplicationPut(message: Message) {
        val operationId = message.operationId
        val key = message.key
        val value = message.value
        val originNodeId = message.originNodeId

        if (operationId == null || key == null || value == null || originNodeId == null) {
            logger.warn("Invalid replication message: missing required fields")
            return
        }

        if (deduplicator.isDuplicate(operationId)) {
            logger.debug("Duplicate operation ignored: $operationId")
            sendAck(operationId, originNodeId)
            return
        }

        if (config.mode == ClusterMode.MULTI) {
            handleMultiReplicationPut(message, operationId, key, value, originNodeId)
        } else {
            store.put(key, value)
            logger.debug("Applied replicated PUT: $key = $value (opId=$operationId)")
            sendAck(operationId, originNodeId)
        }
    }

    private fun handleMultiReplicationPut(
        message: Message, operationId: String, key: String, value: String, originNodeId: String
    ) {
        val version = message.version
        if (version == null) {
            logger.warn("Multi REPL_PUT missing version: opId=$operationId")
            return
        }

        // Update Lamport clock: L = max(L, remoteLamport) + 1
        lamportClock.updateAndGet { current -> maxOf(current, version.lamport) + 1 }

        // Apply using LWW
        val applied = store.putVersioned(key, value, version)
        if (applied) {
            logger.debug("Applied multi replicated PUT: $key=$value, v=(${version.lamport},${version.nodeId})")
        } else {
            logger.debug("Rejected older version for $key: v=(${version.lamport},${version.nodeId})")
        }

        sendAck(operationId, originNodeId)

        // Forward based on topology
        val sourceNodeId = message.sourceNodeId ?: originNodeId
        replicationManager.forwardMulti(operationId, key, value, version, originNodeId, sourceNodeId)
    }

    private fun handleReplicationAck(message: Message) {
        val operationId = message.operationId ?: return
        val fromNodeId = message.fromNodeId ?: message.originNodeId ?: return
        replicationManager.handleAck(operationId, fromNodeId)
    }

    private fun sendAck(operationId: String, targetNodeId: String) {
        val client = getConnection(targetNodeId) ?: return
        val ack = Message(
            type = MessageType.REPL_ACK,
            operationId = operationId,
            fromNodeId = nodeId
        )
        if (client.send(ack)) {
            logger.debug("Sent ACK for opId=$operationId to $targetNodeId")
        } else {
            logger.warn("Failed to send ACK for opId=$operationId to $targetNodeId")
        }
    }

    // ========== Helpers ==========

    private fun isLeaderSingle(): Boolean = config.leaderId == nodeId

    private fun isLeaderMulti(): Boolean = nodeId in config.leaderNodeIds

    private fun errorResponse(request: Message, errorCode: ErrorCode, errorMessage: String) = Message(
        type = MessageType.RESPONSE,
        requestId = request.requestId,
        status = "ERROR",
        errorCode = errorCode,
        errorMessage = errorMessage
    )
}
