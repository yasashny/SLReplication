package com.yasashny.slreplication.cli

import com.yasashny.slreplication.common.model.Message
import com.yasashny.slreplication.common.model.MessageType
import java.util.UUID
import kotlin.random.Random

class LeaderlessAdminOps(
    private val registry: NodeRegistry,
    private val state: ClusterState,
    private val sender: RequestSender,
    private val dump: (String) -> Result<Message>,
    private val clientId: String
) {
    private val antiEntropyOrchestrator = AntiEntropyOrchestrator(
        sendRequest = { target, msg, timeout -> sender.send(target, msg, timeout) },
        clientId = clientId
    )

    fun dumpHints(targetNodeId: String? = null): Result<Message> {
        val target = targetNodeId ?: state.spareNodeIds.firstOrNull() ?: registry.firstId()
            ?: return Result.failure(Exception("No nodes available"))
        return sender.send(target, Message(
            type = MessageType.CLIENT_DUMP_HINTS,
            requestId = UUID.randomUUID().toString(),
            clientId = clientId
        ))
    }

    fun runHintedHandoff(targetNodeId: String? = null): Map<String, Result<Message>> {
        val targets = if (targetNodeId != null) listOf(targetNodeId) else state.spareNodeIds
        return targets.associateWith { nodeId ->
            sender.send(nodeId, Message(
                type = MessageType.RUN_HINTED_HANDOFF,
                requestId = UUID.randomUUID().toString(),
                clientId = clientId
            ), 30000)
        }
    }

    fun showMerkleRoot(nodeId: String): Result<Message> =
        sender.send(nodeId, Message(
            type = MessageType.MERKLE_ROOT_REQUEST,
            requestId = UUID.randomUUID().toString(),
            clientId = clientId
        ))

    fun wipeNodeData(nodeId: String): Result<Message> =
        sender.send(nodeId, Message(
            type = MessageType.WIPE_DATA,
            requestId = UUID.randomUUID().toString(),
            clientId = clientId
        ))

    fun runAntiEntropyCluster(): Result<String> {
        val delayMs = if (state.replicationDelayMinMs > 0 || state.replicationDelayMaxMs > 0) {
            Random.nextLong(state.replicationDelayMinMs, state.replicationDelayMaxMs + 1)
        } else 0L
        return antiEntropyOrchestrator.run(state.homeReplicaIds, delayMs)
    }

    fun getLeaderlessStats(): Map<String, Long> {
        val allStats = mutableMapOf<String, Long>()
        for (nodeId in registry.nodeIds()) {
            val result = dump(nodeId)
            result.getOrNull()?.leaderless?.stats?.forEach { (key, value) ->
                allStats[key] = (allStats[key] ?: 0) + value
            }
        }
        return allStats
    }
}
