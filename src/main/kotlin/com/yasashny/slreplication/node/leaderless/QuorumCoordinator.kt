package com.yasashny.slreplication.node.leaderless

import com.yasashny.slreplication.common.model.ClusterConfig
import com.yasashny.slreplication.common.model.Message
import com.yasashny.slreplication.common.network.TcpClient
import com.yasashny.slreplication.node.LamportClock
import com.yasashny.slreplication.node.replication.OperationDeduplicator
import com.yasashny.slreplication.node.storage.HintStore
import com.yasashny.slreplication.node.storage.KeyValueStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel

class QuorumCoordinator(
    nodeId: String,
    store: KeyValueStore,
    hintStore: HintStore,
    deduplicator: OperationDeduplicator,
    lamportClock: LamportClock,
    getConfig: () -> ClusterConfig,
    getNodeConnection: (String) -> TcpClient?,
    val metrics: LeaderlessMetrics = LeaderlessMetrics()
) {
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    private val writer = WriteCoordinator(
        nodeId, store, hintStore, deduplicator, lamportClock, metrics, scope, getConfig, getNodeConnection
    )

    private val reader = ReadCoordinator(
        nodeId, store, metrics, scope, getConfig, getNodeConnection
    )

    suspend fun coordinateWrite(key: String, value: String, requestId: String?): Message =
        writer.coordinate(key, value, requestId)

    suspend fun coordinateRead(key: String, requestId: String?): Message =
        reader.coordinate(key, requestId)

    fun stop() {
        scope.cancel()
    }
}
