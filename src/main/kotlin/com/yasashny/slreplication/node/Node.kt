package com.yasashny.slreplication.node

import com.yasashny.slreplication.common.model.*
import com.yasashny.slreplication.common.network.ConnectionPool
import com.yasashny.slreplication.common.network.MessageSender
import com.yasashny.slreplication.common.network.TcpClient
import com.yasashny.slreplication.common.network.TcpServer
import com.yasashny.slreplication.common.model.LeaderlessPayload
import com.yasashny.slreplication.node.leaderless.LeaderlessCoordinator
import com.yasashny.slreplication.node.replication.OperationDeduplicator
import com.yasashny.slreplication.node.replication.ReplicationManager
import com.yasashny.slreplication.node.replication.ReplicationResult
import com.yasashny.slreplication.node.storage.HintStore
import com.yasashny.slreplication.node.storage.KeyValueStore
import org.slf4j.LoggerFactory
import java.util.UUID

class Node(
    val nodeId: String,
    val port: Int
) {
    private val logger = LoggerFactory.getLogger(Node::class.java)
    private val store = KeyValueStore()
    private val hintStore = HintStore()
    private val deduplicator = OperationDeduplicator()
    private val connectionPool = ConnectionPool()
    private val lamportClock = LamportClock(nodeId)

    @Volatile
    private var config = ClusterConfig()

    private val replicationManager = ReplicationManager(
        nodeId = nodeId,
        getConfig = { config },
        getNodeConnection = { targetNodeId -> getConnection(targetNodeId) }
    )

    private val leaderlessCoordinator = LeaderlessCoordinator(
        nodeId, store, hintStore, deduplicator, lamportClock,
        { config }, { getConnection(it) }
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
        leaderlessCoordinator.stop()
        deduplicator.stop()
        connectionPool.closeAll()
    }

    private fun getConnection(targetNodeId: String): TcpClient? {
        val nodeInfo = config.nodes.find { it.nodeId == targetNodeId } ?: return null
        val client = connectionPool.getOrCreate(targetNodeId, nodeInfo.host, nodeInfo.port) { message ->
            handleIncomingMessage(message)
        }
        if (!client.isConnected()) client.connect()
        return client
    }

    private fun handleIncomingMessage(message: Message) {
        if (message.type == MessageType.REPL_ACK) {
            val opId = message.operationId
            val from = message.fromNodeId ?: message.originNodeId
            if (opId != null && from != null) {
                replicationManager.handleAck(opId, from)
            }
        }
    }


    private suspend fun handleMessage(message: Message, sender: MessageSender) {
        logger.info("Received message: type={}, requestId={}", message.type, message.requestId)

        when (message.type) {
            MessageType.CLIENT_PUT -> handlePut(message, sender)
            MessageType.CLIENT_GET -> handleGet(message, sender)
            MessageType.CLIENT_DUMP -> handleDump(message, sender)
            MessageType.CLIENT_DUMP_HINTS -> handleDumpHints(message, sender)
            MessageType.CLUSTER_UPDATE -> handleClusterUpdate(message, sender)
            MessageType.REPL_PUT -> handleReplicationPut(message)
            MessageType.REPL_ACK -> handleReplicationAck(message)

            MessageType.REPL_WRITE -> leaderlessCoordinator.handleReplicationWrite(message, sender)
            MessageType.READ_QUERY -> leaderlessCoordinator.handleReadQuery(message, sender)
            MessageType.HINTED_HANDOFF_TRANSFER -> leaderlessCoordinator.handleHintedHandoffTransfer(message, sender)
            MessageType.MERKLE_ROOT_REQUEST -> leaderlessCoordinator.handleMerkleRootRequest(message, sender)
            MessageType.MERKLE_DIFF_REQUEST -> leaderlessCoordinator.handleMerkleDiffRequest(message, sender)
            MessageType.MERKLE_RECORDS_TRANSFER -> leaderlessCoordinator.handleMerkleRecordsTransfer(message, sender)
            MessageType.WIPE_DATA -> handleWipeData(message, sender)
            MessageType.RUN_HINTED_HANDOFF -> handleRunHintedHandoff(message, sender)

            MessageType.REPL_WRITE_ACK, MessageType.READ_RESPONSE,
            MessageType.HINTED_HANDOFF_ACK, MessageType.MERKLE_ROOT_RESPONSE,
            MessageType.MERKLE_DIFF_RESPONSE, MessageType.MERKLE_RECORDS_ACK,
            MessageType.WIPE_DATA_ACK, MessageType.RUN_HINTED_HANDOFF_RESPONSE,
            MessageType.CLUSTER_UPDATE_ACK, MessageType.RESPONSE -> {}
        }
    }


    private suspend fun handlePut(message: Message, sender: MessageSender) {
        val key = message.key
        val value = message.value
        if (key.isNullOrBlank()) return sender.send(errorResponse(message, ErrorCode.BAD_REQUEST, "Key is required"))
        if (value == null) return sender.send(errorResponse(message, ErrorCode.BAD_REQUEST, "Value is required"))

        when (config.mode) {
            ClusterMode.LEADERLESS -> handleLeaderlessPut(key, value, message, sender)
            ClusterMode.MULTI -> handleMultiPut(key, value, message, sender)
            ClusterMode.SINGLE -> handleSinglePut(key, value, message, sender)
        }
    }

    private suspend fun handleSinglePut(key: String, value: String, message: Message, sender: MessageSender) {
        if (!isLeaderSingle()) {
            sender.send(Message(
                type = MessageType.RESPONSE, requestId = message.requestId,
                status = "ERROR", errorCode = ErrorCode.NOT_LEADER,
                errorMessage = "This node is not the leader", leaderNodeId = config.leaderId
            ))
            return
        }
        store.put(key, value)

        when (val result = replicationManager.replicatePut(key, value)) {
            is ReplicationResult.Success ->
                sender.send(Message(type = MessageType.RESPONSE, requestId = message.requestId, status = "OK"))
            is ReplicationResult.Error ->
                sender.send(Message(
                    type = MessageType.RESPONSE, requestId = message.requestId,
                    status = "ERROR", errorCode = result.code, errorMessage = result.message
                ))
        }
    }

    private suspend fun handleMultiPut(key: String, value: String, message: Message, sender: MessageSender) {
        if (!isLeaderMulti()) {
            sender.send(Message(
                type = MessageType.RESPONSE, requestId = message.requestId,
                status = "ERROR", errorCode = ErrorCode.NOT_LEADER_FOR_WRITE,
                errorMessage = "This node is not a leader in multi-leader mode",
                leaderNodeId = config.leaderNodeIds.randomOrNull()
            ))
            return
        }

        val operationId = UUID.randomUUID().toString()
        val version = lamportClock.nextVersion()
        deduplicator.isDuplicate(operationId)
        store.putVersioned(key, value, version)

        logger.debug("Multi PUT locally: $key=$value, v=(${version.lamport},$nodeId), opId=$operationId")
        replicationManager.replicateMulti(operationId, key, value, version, nodeId)

        sender.send(Message(type = MessageType.RESPONSE, requestId = message.requestId, status = "OK"))
    }

    private suspend fun handleLeaderlessPut(key: String, value: String, message: Message, sender: MessageSender) {
        val response = leaderlessCoordinator.coordinateWrite(key, value, message.requestId)
        sender.send(response)
    }


    private suspend fun handleGet(message: Message, sender: MessageSender) {
        val key = message.key
        if (key.isNullOrBlank()) return sender.send(errorResponse(message, ErrorCode.BAD_REQUEST, "Key is required"))

        if (config.mode == ClusterMode.LEADERLESS) {
            val response = leaderlessCoordinator.coordinateRead(key, message.requestId)
            sender.send(response)
        } else {
            val value = store.get(key)
            val version = if (config.mode == ClusterMode.MULTI) store.getVersion(key) else null
            sender.send(Message(
                type = MessageType.RESPONSE, requestId = message.requestId,
                status = "OK", key = key, value = value, version = version
            ))
        }
    }


    private suspend fun handleDump(message: Message, sender: MessageSender) {
        when (config.mode) {
            ClusterMode.LEADERLESS -> {
                sender.send(Message(
                    type = MessageType.RESPONSE, requestId = message.requestId,
                    status = "OK", data = store.dump(), versionedData = store.dumpVersioned(),
                    leaderless = LeaderlessPayload(stats = leaderlessCoordinator.getStats())
                ))
            }
            ClusterMode.MULTI -> {
                sender.send(Message(
                    type = MessageType.RESPONSE, requestId = message.requestId,
                    status = "OK", data = store.dump(), versionedData = store.dumpVersioned()
                ))
            }
            ClusterMode.SINGLE -> {
                sender.send(Message(
                    type = MessageType.RESPONSE, requestId = message.requestId,
                    status = "OK", data = store.dump()
                ))
            }
        }
    }

    private suspend fun handleDumpHints(message: Message, sender: MessageSender) {
        sender.send(Message(
            type = MessageType.RESPONSE, requestId = message.requestId,
            status = "OK", leaderless = LeaderlessPayload(hints = hintStore.getAllHints())
        ))
    }

    private suspend fun handleWipeData(message: Message, sender: MessageSender) {
        store.wipe()
        hintStore.wipe()
        deduplicator.reset()
        lamportClock.reset()
        leaderlessCoordinator.resetStats()
        logger.info("Node $nodeId data wiped")
        sender.send(Message(
            type = MessageType.WIPE_DATA_ACK, requestId = message.requestId, status = "OK"
        ))
    }

    private suspend fun handleRunHintedHandoff(message: Message, sender: MessageSender) {
        val result = leaderlessCoordinator.runHintedHandoff()
        sender.send(Message(
            type = MessageType.RUN_HINTED_HANDOFF_RESPONSE, requestId = message.requestId,
            status = "OK", leaderless = LeaderlessPayload(stats = result)
        ))
    }


    private suspend fun handleClusterUpdate(message: Message, sender: MessageSender) {
        val newConfig = message.clusterConfig
        if (newConfig == null) return sender.send(errorResponse(message, ErrorCode.BAD_REQUEST, "Cluster config is required"))

        logger.info("Cluster update: nodes=${newConfig.nodes.map { it.nodeId }}, mode=${newConfig.mode}, " +
                "leader=${newConfig.leaderId}, leaders=${newConfig.leaderNodeIds}, topology=${newConfig.topology}")
        config = newConfig
        sender.send(Message(type = MessageType.CLUSTER_UPDATE_ACK, requestId = message.requestId, status = "OK"))
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

        lamportClock.merge(version.lamport)

        val applied = store.putVersioned(key, value, version)
        if (applied) {
            logger.debug("Applied multi replicated PUT: $key=$value, v=(${version.lamport},${version.nodeId})")
        } else {
            logger.debug("Rejected older version for $key: v=(${version.lamport},${version.nodeId})")
        }

        sendAck(operationId, originNodeId)

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
        val ack = Message(type = MessageType.REPL_ACK, operationId = operationId, fromNodeId = nodeId)
        if (!client.send(ack)) {
            logger.warn("Failed to send ACK for opId=$operationId to $targetNodeId")
        }
    }


    private fun isLeaderSingle(): Boolean = config.leaderId == nodeId
    private fun isLeaderMulti(): Boolean = nodeId in config.leaderNodeIds

    private fun errorResponse(request: Message, errorCode: ErrorCode, errorMessage: String) = Message(
        type = MessageType.RESPONSE, requestId = request.requestId,
        status = "ERROR", errorCode = errorCode, errorMessage = errorMessage
    )
}
