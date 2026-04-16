package com.yasashny.slreplication.node.leaderless

import com.yasashny.slreplication.common.model.*
import com.yasashny.slreplication.common.network.TcpClient
import com.yasashny.slreplication.node.LamportClock
import com.yasashny.slreplication.node.replication.OperationDeduplicator
import com.yasashny.slreplication.node.storage.HintStore
import com.yasashny.slreplication.node.storage.KeyValueStore
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.random.Random

class QuorumCoordinator(
    private val nodeId: String,
    private val store: KeyValueStore,
    private val hintStore: HintStore,
    private val deduplicator: OperationDeduplicator,
    private val lamportClock: LamportClock,
    private val getConfig: () -> ClusterConfig,
    private val getNodeConnection: (String) -> TcpClient?
) {
    private val logger = LoggerFactory.getLogger(QuorumCoordinator::class.java)
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    val staleReadCount = AtomicLong(0)
    val readRepairCount = AtomicLong(0)
    val hintsCreated = AtomicLong(0)

    private val baseTimeoutMs = 5000L


    suspend fun coordinateWrite(key: String, value: String, requestId: String?): Message {
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
        waitForQuorum(homeAcks, W, homeJobs, timeout)

        if (homeAcks.get() >= W) {
            return successResponse(requestId)
        }

        if (config.writeQuorumMode == WriteQuorumMode.SLOPPY) {
            homeJobs.forEach { it.join() }
            val spareJobs = sendToSpareNodes(config.spareNodeIds, failedHomes.toList(),
                operationId, key, value, version, timeout, spareAcks)
            waitForQuorum(homeAcks, spareAcks, W, spareJobs, timeout)

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
                val perSendDelay = computeDelay(config)
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
                    hintsCreated.incrementAndGet()
                } else {
                    val success = sendReplicationWrite(spareId, operationId, key, value, version, failedHome, timeout)
                    if (success) {
                        spareAcks.incrementAndGet()
                        hintsCreated.incrementAndGet()
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


    private data class ReadEntry(
        val nodeId: String, val value: String?,
        val version: Version?, val operationId: String?
    )

    suspend fun coordinateRead(key: String, requestId: String?): Message {
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

        waitForQuorum(responses, R, readJobs, timeout)

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
            staleReadCount.incrementAndGet()
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
            readRepairCount.incrementAndGet()
            logger.debug("Read-repair: key=$key -> $targetNodeId")
        } catch (_: Exception) {
            logger.debug("Read-repair failed: key=$key -> $targetNodeId")
        }
    }


    private suspend fun waitForQuorum(counter: AtomicInteger, required: Int, jobs: List<Job>, timeout: Long) {
        try {
            withTimeout(timeout) {
                while (counter.get() < required) {
                    if (jobs.all { it.isCompleted }) break
                    delay(5)
                }
            }
        } catch (_: TimeoutCancellationException) {
            logger.debug("Quorum timeout: got ${counter.get()}/$required")
        }
    }

    private suspend fun waitForQuorum(
        homeAcks: AtomicInteger, spareAcks: AtomicInteger,
        required: Int, jobs: List<Job>, timeout: Long
    ) {
        try {
            withTimeout(timeout) {
                while (homeAcks.get() + spareAcks.get() < required) {
                    if (jobs.all { it.isCompleted }) break
                    delay(5)
                }
            }
        } catch (_: TimeoutCancellationException) {}
    }

    private suspend fun <T> waitForQuorum(
        collection: ConcurrentLinkedQueue<T>, required: Int, jobs: List<Job>, timeout: Long
    ) {
        try {
            withTimeout(timeout) {
                while (collection.size < required) {
                    if (jobs.all { it.isCompleted }) break
                    delay(5)
                }
            }
        } catch (_: TimeoutCancellationException) {}
    }

    private fun successResponse(requestId: String?) =
        Message(type = MessageType.RESPONSE, requestId = requestId, status = "OK")

    fun stop() { scope.cancel() }

    private fun computeDelay(config: ClusterConfig): Long {
        return if (config.replicationDelayMinMs > 0 || config.replicationDelayMaxMs > 0) {
            Random.nextLong(config.replicationDelayMinMs, config.replicationDelayMaxMs + 1)
        } else 0L
    }
}
