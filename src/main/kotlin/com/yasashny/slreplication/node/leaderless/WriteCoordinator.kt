package com.yasashny.slreplication.node.leaderless

import com.yasashny.slreplication.common.model.*
import com.yasashny.slreplication.common.network.TcpClient
import com.yasashny.slreplication.node.LamportClock
import com.yasashny.slreplication.node.computeReplicationDelay
import com.yasashny.slreplication.node.replication.OperationDeduplicator
import com.yasashny.slreplication.node.storage.HintStore
import com.yasashny.slreplication.node.storage.KeyValueStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

class WriteCoordinator(
    private val nodeId: String,
    private val store: KeyValueStore,
    private val hintStore: HintStore,
    private val deduplicator: OperationDeduplicator,
    private val lamportClock: LamportClock,
    private val metrics: LeaderlessMetrics,
    private val scope: CoroutineScope,
    private val getConfig: () -> ClusterConfig,
    private val getNodeConnection: (String) -> TcpClient?
) {
    private val logger = LoggerFactory.getLogger(WriteCoordinator::class.java)
    private val baseTimeoutMs = 5000L

    suspend fun coordinate(key: String, value: String, requestId: String?): Message {
        val config = getConfig()
        val homeReplicas = config.homeReplicaIds
        val W = config.writeQuorum
        val timeout = baseTimeoutMs + config.replicationDelayMaxMs

        val operationId = UUID.randomUUID().toString()
        val version = lamportClock.nextVersion()
        deduplicator.isDuplicate(operationId)

        val isHome = nodeId in homeReplicas
        val homeAcks = AtomicInteger(if (isHome) 1 else 0)
        val spareAcks = AtomicInteger(0)
        val failedHomes = ConcurrentLinkedQueue<String>()

        if (isHome) {
            store.putVersioned(key, value, version, operationId)
        }

        val homeJobs = sendToHomeReplicas(homeReplicas, operationId, key, value, version, config, timeout, homeAcks, failedHomes)
        waitForQuorum({ homeAcks.get() }, W, homeJobs, timeout)

        if (homeAcks.get() >= W) {
            return successResponse(requestId)
        }

        if (config.writeQuorumMode == WriteQuorumMode.SLOPPY) {
            homeJobs.forEach { it.join() }
            val spareJobs = sendToSpareNodes(config.spareNodeIds, failedHomes.toList(),
                operationId, key, value, version, timeout, spareAcks)
            waitForQuorum({ homeAcks.get() + spareAcks.get() }, W, spareJobs, timeout)

            if (homeAcks.get() + spareAcks.get() >= W) {
                return successResponse(requestId)
            }
        }

        return Message(
            type = MessageType.RESPONSE, requestId = requestId,
            status = "ERROR", errorCode = ErrorCode.NOT_ENOUGH_REPLICAS,
            errorMessage = "Write quorum not met: got ${homeAcks.get() + spareAcks.get()}/$W"
        )
    }

    private fun sendToHomeReplicas(
        homeReplicas: List<String>, operationId: String,
        key: String, value: String, version: Version,
        config: ClusterConfig, timeout: Long,
        homeAcks: AtomicInteger, failedHomes: ConcurrentLinkedQueue<String>
    ): List<Job> {
        return homeReplicas.filter { it != nodeId }.map { homeId ->
            scope.launch {
                val perSendDelay = computeReplicationDelay(config)
                if (perSendDelay > 0) delay(perSendDelay)
                val success = sendReplicationWrite(homeId, operationId, key, value, version, null, timeout)
                if (success) homeAcks.incrementAndGet()
                else failedHomes.add(homeId)
            }
        }
    }

    private fun sendToSpareNodes(
        spareNodeIds: List<String>, failedHomes: List<String>,
        operationId: String, key: String, value: String, version: Version,
        timeout: Long, spareAcks: AtomicInteger
    ): List<Job> {
        return failedHomes.zip(spareNodeIds).map { (failedHome, spareId) ->
            scope.launch {
                if (spareId == nodeId) {
                    hintStore.addHint(HintRecord(key, value, version, operationId, failedHome))
                    spareAcks.incrementAndGet()
                    metrics.hintsCreated.incrementAndGet()
                } else {
                    val success = sendReplicationWrite(spareId, operationId, key, value, version, failedHome, timeout)
                    if (success) {
                        spareAcks.incrementAndGet()
                        metrics.hintsCreated.incrementAndGet()
                    }
                }
            }
        }
    }

    private suspend fun sendReplicationWrite(
        targetNodeId: String, operationId: String,
        key: String, value: String, version: Version,
        intendedHomeNodeId: String?, timeout: Long
    ): Boolean {
        val client = getNodeConnection(targetNodeId) ?: return false
        val msg = Message(
            type = MessageType.REPL_WRITE,
            requestId = UUID.randomUUID().toString(),
            operationId = operationId,
            key = key, value = value, version = version,
            sourceNodeId = nodeId,
            leaderless = if (intendedHomeNodeId != null) LeaderlessPayload(intendedHomeNodeId = intendedHomeNodeId) else null
        )
        return try {
            val response = client.sendAndWait(msg, timeout)
            response?.type == MessageType.REPL_WRITE_ACK
        } catch (e: Exception) {
            logger.debug("Failed to send REPL_WRITE to $targetNodeId: ${e.message}")
            false
        }
    }

    private fun successResponse(requestId: String?) =
        Message(type = MessageType.RESPONSE, requestId = requestId, status = "OK")
}
