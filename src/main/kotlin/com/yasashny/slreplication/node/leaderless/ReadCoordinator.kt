package com.yasashny.slreplication.node.leaderless

import com.yasashny.slreplication.common.model.*
import com.yasashny.slreplication.common.network.TcpClient
import com.yasashny.slreplication.node.storage.KeyValueStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue

class ReadCoordinator(
    private val nodeId: String,
    private val store: KeyValueStore,
    private val metrics: LeaderlessMetrics,
    private val scope: CoroutineScope,
    private val getConfig: () -> ClusterConfig,
    private val getNodeConnection: (String) -> TcpClient?
) {
    private val logger = LoggerFactory.getLogger(ReadCoordinator::class.java)
    private val baseTimeoutMs = 5000L

    private data class ReadEntry(
        val nodeId: String, val value: String?,
        val version: Version?, val operationId: String?
    )

    suspend fun coordinate(key: String, requestId: String?): Message {
        val config = getConfig()
        val homeReplicas = config.homeReplicaIds
        val R = config.readQuorum
        val timeout = baseTimeoutMs + config.replicationDelayMaxMs

        val responses = ConcurrentLinkedQueue<ReadEntry>()
        if (nodeId in homeReplicas) {
            responses.add(ReadEntry(nodeId, store.get(key), store.getVersion(key), store.getOperationId(key)))
        }

        val readJobs = homeReplicas.filter { it != nodeId }.map { homeId ->
            scope.launch {
                val client = getNodeConnection(homeId) ?: return@launch
                val msg = Message(
                    type = MessageType.READ_QUERY,
                    requestId = UUID.randomUUID().toString(),
                    key = key, sourceNodeId = nodeId
                )
                try {
                    val resp = client.sendAndWait(msg, timeout)
                    if (resp?.type == MessageType.READ_RESPONSE) {
                        responses.add(ReadEntry(homeId, resp.value, resp.version, resp.operationId))
                    }
                } catch (_: Exception) {}
            }
        }

        waitForQuorum({ responses.size }, R, readJobs, timeout)

        if (responses.size < R) {
            return Message(
                type = MessageType.RESPONSE, requestId = requestId,
                status = "ERROR", errorCode = ErrorCode.NOT_ENOUGH_REPLICAS,
                errorMessage = "Read quorum not met: got ${responses.size}/$R"
            )
        }

        val allResponses = responses.toList()
        val newestEntry = allResponses
            .filter { it.value != null && it.version != null }
            .maxByOrNull { it.version!! }
            ?: allResponses.first()

        scheduleAsyncReadRepair(allResponses, newestEntry, key)

        return Message(
            type = MessageType.RESPONSE, requestId = requestId,
            status = "OK", key = key, value = newestEntry.value, version = newestEntry.version
        )
    }

    private fun scheduleAsyncReadRepair(allResponses: List<ReadEntry>, newestEntry: ReadEntry, key: String) {
        if (newestEntry.value == null || newestEntry.version == null) return

        val newestVersion = newestEntry.version
        val staleNodes = allResponses.filter { entry ->
            entry.nodeId != newestEntry.nodeId && (
                entry.value == null || entry.version == null || entry.version < newestVersion
            )
        }

        if (staleNodes.isNotEmpty()) {
            metrics.staleReadCount.incrementAndGet()
            scope.launch {
                for (stale in staleNodes) {
                    sendReadRepair(stale.nodeId, key, newestEntry.value, newestVersion, newestEntry.operationId)
                }
            }
        }
    }

    private suspend fun sendReadRepair(
        targetNodeId: String, key: String, value: String,
        version: Version, operationId: String?
    ) {
        val client = getNodeConnection(targetNodeId) ?: return
        val repairOpId = operationId ?: "repair-${UUID.randomUUID()}"
        val msg = Message(
            type = MessageType.REPL_WRITE,
            requestId = UUID.randomUUID().toString(),
            operationId = repairOpId,
            key = key, value = value, version = version,
            sourceNodeId = nodeId
        )
        try {
            client.sendAndWait(msg, 3000)
            metrics.readRepairCount.incrementAndGet()
            logger.debug("Read-repair: key=$key -> $targetNodeId")
        } catch (_: Exception) {
            logger.debug("Read-repair failed: key=$key -> $targetNodeId")
        }
    }
}
