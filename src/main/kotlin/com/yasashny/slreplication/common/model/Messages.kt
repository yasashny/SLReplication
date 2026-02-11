package com.yasashny.slreplication.common.model

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

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
    NOT_ENOUGH_REPLICAS,
    TIMEOUT,
    BAD_REQUEST,
    UNKNOWN_NODE,
    INTERNAL_ERROR
}

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
    val replicationDelayMaxMs: Long = 0
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

    val status: String? = null,
    val errorCode: ErrorCode? = null,
    val errorMessage: String? = null,
    val leaderNodeId: String? = null,

    val data: Map<String, String>? = null,

    val clusterConfig: ClusterConfig? = null
)

val JsonConfig = Json {
    ignoreUnknownKeys = true
    encodeDefaults = true
    isLenient = true
}

fun Message.toJsonLine(): String = JsonConfig.encodeToString(Message.serializer(), this) + "\n"

fun String.toMessage(): Message = JsonConfig.decodeFromString(Message.serializer(), this.trim())