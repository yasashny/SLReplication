package com.yasashny.slreplication.common.model

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

enum class ClusterMode {
    SINGLE, MULTI
}

enum class Topology {
    MESH, RING, STAR
}

enum class ReplicationMode {
    ASYNC,
    SEMI_SYNC,
    SYNC
}

enum class MessageType {
    CLIENT_PUT,
    CLIENT_GET,
    CLIENT_DUMP,

    RESPONSE,

    REPL_PUT,
    REPL_ACK,

    CLUSTER_UPDATE,
    CLUSTER_UPDATE_ACK
}

enum class ErrorCode {
    NOT_LEADER,
    NOT_LEADER_FOR_WRITE,
    NOT_ENOUGH_REPLICAS,
    TIMEOUT,
    BAD_REQUEST,
    UNKNOWN_NODE,
    INTERNAL_ERROR,
    INVALID_MODE,
    INVALID_TOPOLOGY,
    INVALID_CONFIG
}

@Serializable
data class Version(
    val lamport: Long,
    val nodeId: String
) : Comparable<Version> {
    override fun compareTo(other: Version): Int {
        val cmp = lamport.compareTo(other.lamport)
        if (cmp != 0) return cmp
        return nodeId.compareTo(other.nodeId)
    }
}

@Serializable
data class VersionedEntry(
    val key: String,
    val value: String,
    val version: Version? = null
)

@Serializable
data class NodeInfo(
    val nodeId: String,
    val host: String,
    val port: Int
)

@Serializable
data class ClusterConfig(
    val nodes: List<NodeInfo> = emptyList(),
    val leaderId: String? = null,
    val replicationMode: ReplicationMode = ReplicationMode.ASYNC,
    val replicationFactor: Int = 1,
    val semiSyncAcks: Int = 1,
    val replicationDelayMinMs: Long = 0,
    val replicationDelayMaxMs: Long = 0,
    val mode: ClusterMode = ClusterMode.SINGLE,
    val topology: Topology = Topology.MESH,
    val starCenterId: String? = null,
    val leaderNodeIds: List<String> = emptyList()
)

@Serializable
data class Message(
    val type: MessageType,
    val requestId: String? = null,
    val clientId: String? = null,

    val key: String? = null,
    val value: String? = null,

    val operationId: String? = null,
    val originNodeId: String? = null,
    val sourceNodeId: String? = null,
    val fromNodeId: String? = null,

    val status: String? = null,
    val errorCode: ErrorCode? = null,
    val errorMessage: String? = null,
    val leaderNodeId: String? = null,

    val data: Map<String, String>? = null,
    val versionedData: List<VersionedEntry>? = null,
    val version: Version? = null,

    val clusterConfig: ClusterConfig? = null
)

val JsonConfig = Json {
    ignoreUnknownKeys = true
    encodeDefaults = true
    isLenient = true
}

fun Message.toJsonLine(): String = JsonConfig.encodeToString(Message.serializer(), this) + "\n"

fun String.toMessage(): Message = JsonConfig.decodeFromString(Message.serializer(), this.trim())
