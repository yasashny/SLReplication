package com.yasashny.slreplication.cli

import com.yasashny.slreplication.common.model.ClusterMode
import com.yasashny.slreplication.common.model.Message
import com.yasashny.slreplication.common.model.MessageType
import java.util.UUID

class ClientOps(
    private val registry: NodeRegistry,
    private val state: ClusterState,
    private val sender: RequestSender,
    private val clientId: String
) {
    fun put(key: String, value: String, targetNodeId: String? = null): Result<Message> {
        val target = targetNodeId ?: when (state.mode) {
            ClusterMode.SINGLE -> state.leaderId
            ClusterMode.MULTI -> state.leaderNodeIds.randomOrNull()
            ClusterMode.LEADERLESS -> registry.randomId()
        } ?: return Result.failure(Exception("No nodes available"))

        return sender.send(target, Message(
            type = MessageType.CLIENT_PUT,
            requestId = UUID.randomUUID().toString(),
            clientId = clientId,
            key = key,
            value = value
        ))
    }

    fun get(key: String, targetNodeId: String? = null): Result<Message> {
        val target = targetNodeId ?: when (state.mode) {
            ClusterMode.SINGLE -> state.leaderId
            ClusterMode.MULTI -> registry.randomId()
            ClusterMode.LEADERLESS -> registry.randomId()
        } ?: return Result.failure(Exception("No nodes available"))

        return sender.send(target, Message(
            type = MessageType.CLIENT_GET,
            requestId = UUID.randomUUID().toString(),
            clientId = clientId,
            key = key
        ))
    }

    fun dump(targetNodeId: String? = null): Result<Message> {
        val target = targetNodeId
            ?: (if (state.mode == ClusterMode.SINGLE) state.leaderId else null)
            ?: registry.firstId()
            ?: return Result.failure(Exception("No nodes in cluster"))

        return sender.send(target, Message(
            type = MessageType.CLIENT_DUMP,
            requestId = UUID.randomUUID().toString(),
            clientId = clientId
        ))
    }

    fun getAll(key: String): Map<String, Result<Message>> =
        registry.nodeIds().sorted().associateWith { nodeId -> get(key, nodeId) }

    fun clusterDump(): Map<String, Result<Message>> =
        registry.nodeIds().sorted().associateWith { nodeId -> dump(nodeId) }
}
