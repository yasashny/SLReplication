package com.yasashny.slreplication.node.replication

import com.yasashny.slreplication.common.model.ClusterConfig
import com.yasashny.slreplication.common.model.ClusterMode
import com.yasashny.slreplication.common.model.ErrorCode
import com.yasashny.slreplication.common.model.Message
import com.yasashny.slreplication.common.model.Version
import com.yasashny.slreplication.common.network.TcpClient
import com.yasashny.slreplication.node.LamportClock
import com.yasashny.slreplication.node.storage.KeyValueStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel

sealed class ReplicationResult {
    object Success : ReplicationResult()
    data class Error(val code: ErrorCode, val message: String) : ReplicationResult()
}

class ReplicationManager(
    nodeId: String,
    private val getConfig: () -> ClusterConfig,
    getNodeConnection: (String) -> TcpClient?,
    store: KeyValueStore,
    lamportClock: LamportClock,
    deduplicator: OperationDeduplicator
) {
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private val retryQueue = RetryQueue(nodeId, scope, getNodeConnection)
    private val single = SingleLeaderReplicator(
        nodeId, scope, retryQueue, store, deduplicator, getNodeConnection, getConfig
    )
    private val multi = MultiLeaderReplicator(
        nodeId, scope, retryQueue, store, lamportClock, deduplicator, getNodeConnection, getConfig
    )

    suspend fun replicatePut(key: String, value: String): ReplicationResult =
        single.replicate(key, value)

    fun replicateMulti(
        operationId: String, key: String, value: String,
        version: Version, originNodeId: String
    ) = multi.replicate(operationId, key, value, version, originNodeId)

    fun forwardMulti(
        operationId: String, key: String, value: String,
        version: Version, originNodeId: String, sourceNodeId: String
    ) = multi.forward(operationId, key, value, version, originNodeId, sourceNodeId)

    fun handleIncoming(message: Message) {
        if (getConfig().mode == ClusterMode.MULTI) multi.handleIncoming(message)
        else single.handleIncoming(message)
    }

    fun handleAck(operationId: String, fromNodeId: String) =
        single.handleAck(operationId, fromNodeId)

    fun stop() = scope.cancel()
}
