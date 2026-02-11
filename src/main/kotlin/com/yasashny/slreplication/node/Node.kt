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

class Node(
    val nodeId: String,
    val port: Int
) {
    private val logger = LoggerFactory.getLogger(Node::class.java)
    private val store = KeyValueStore()
    private val deduplicator = OperationDeduplicator()
    private val connectionPool = ConnectionPool()

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
                message.operationId?.let { opId ->
                    message.originNodeId?.let { fromNode ->
                        replicationManager.handleAck(opId, fromNode)
                    }
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
        if (!isLeader()) {
            logger.warn("NOT_LEADER: nodeId=$nodeId, config.leaderId=${config.leaderId}")
            sender.send(
                Message(
                    type = MessageType.RESPONSE,
                    requestId = message.requestId,
                    status = "ERROR",
                    errorCode = ErrorCode.NOT_LEADER,
                    errorMessage = "This node is not the leader",
                    leaderNodeId = config.leaderId
                )
            )
            return
        }
        store.put(key, value)
        logger.debug("Applied PUT locally: $key = $value")
        when (val result = replicationManager.replicatePut(key, value)) {
            is ReplicationResult.Success -> {
                sender.send(
                    Message(
                        type = MessageType.RESPONSE,
                        requestId = message.requestId,
                        status = "OK"
                    )
                )
            }
            is ReplicationResult.Error -> {
                sender.send(
                    Message(
                        type = MessageType.RESPONSE,
                        requestId = message.requestId,
                        status = "ERROR",
                        errorCode = result.code,
                        errorMessage = result.message
                    )
                )
            }
        }
    }

    private suspend fun handleGet(message: Message, sender: MessageSender) {
        val key = message.key
        if (key.isNullOrBlank()) {
            sender.send(errorResponse(message, ErrorCode.BAD_REQUEST, "Key is required"))
            return
        }
        val value = store.get(key)
        sender.send(
            Message(
                type = MessageType.RESPONSE,
                requestId = message.requestId,
                status = "OK",
                key = key,
                value = value
            )
        )
    }

    private suspend fun handleDump(message: Message, sender: MessageSender) {
        val data = store.dump()

        sender.send(
            Message(
                type = MessageType.RESPONSE,
                requestId = message.requestId,
                status = "OK",
                data = data
            )
        )
    }

    private suspend fun handleClusterUpdate(message: Message, sender: MessageSender) {
        val newConfig = message.clusterConfig
        if (newConfig == null) {
            sender.send(errorResponse(message, ErrorCode.BAD_REQUEST, "Cluster config is required"))
            return
        }
        logger.info("Received cluster update: nodes=${newConfig.nodes.map { it.nodeId }}, leader=${newConfig.leaderId}, " +
                "mode=${newConfig.replicationMode}, RF=${newConfig.replicationFactor}, K=${newConfig.semiSyncAcks}")
        config = newConfig
        sender.send(
            Message(
                type = MessageType.CLUSTER_UPDATE_ACK,
                requestId = message.requestId,
                status = "OK"
            )
        )
    }

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
        store.put(key, value)
        logger.debug("Applied replicated PUT: $key = $value (opId=$operationId)")

        sendAck(operationId, originNodeId)
    }

    private fun sendAck(operationId: String, leaderNodeId: String) {
        val client = getConnection(leaderNodeId) ?: return
        val ack = Message(
            type = MessageType.REPL_ACK,
            operationId = operationId,
            originNodeId = nodeId
        )
        if (client.send(ack)) {
            logger.debug("Sent ACK for opId=$operationId to $leaderNodeId")
        } else {
            logger.warn("Failed to send ACK for opId=$operationId to $leaderNodeId")
        }
    }

    private fun handleReplicationAck(message: Message) {
        val operationId = message.operationId ?: return
        val fromNodeId = message.originNodeId ?: return
        replicationManager.handleAck(operationId, fromNodeId)
    }

    private fun isLeader(): Boolean = config.leaderId == nodeId

    private fun errorResponse(request: Message, errorCode: ErrorCode, errorMessage: String) = Message(
        type = MessageType.RESPONSE,
        requestId = request.requestId,
        status = "ERROR",
        errorCode = errorCode,
        errorMessage = errorMessage
    )
}