package com.yasashny.slreplication.common.model

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

enum class ClusterMode {
    SINGLE, MULTI, LEADERLESS
}

enum class WriteQuorumMode {
    STRICT, SLOPPY
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

    REPL_WRITE,
    REPL_WRITE_ACK,
    READ_QUERY,
    READ_RESPONSE,
    CLIENT_DUMP_HINTS,
    HINTED_HANDOFF_TRANSFER,
    HINTED_HANDOFF_ACK,
    MERKLE_ROOT_REQUEST,
    MERKLE_ROOT_RESPONSE,
    MERKLE_DIFF_REQUEST,
    MERKLE_DIFF_RESPONSE,
    MERKLE_RECORDS_TRANSFER,
    MERKLE_RECORDS_ACK,
    WIPE_DATA,
    WIPE_DATA_ACK,
    RUN_HINTED_HANDOFF,
    RUN_HINTED_HANDOFF_RESPONSE,

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
    INVALID_CONFIG,
    NODE_UNAVAILABLE
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
    val version: Version? = null,
    val operationId: String? = null
)

@Serializable
data class HintRecord(
    val key: String,
    val value: String,
    val version: Version,
    val operationId: String,
    val intendedHomeNodeId: String
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
    val leaderNodeIds: List<String> = emptyList(),
    val homeReplicaIds: List<String> = emptyList(),
    val spareNodeIds: List<String> = emptyList(),
    val writeQuorum: Int = 3,
    val readQuorum: Int = 3,
    val writeQuorumMode: WriteQuorumMode = WriteQuorumMode.STRICT
)

@Serializable
data class LeaderlessPayload(
    val intendedHomeNodeId: String? = null,
    val hints: List<HintRecord>? = null,
    val merkleRoot: String? = null,
    val bucketHashes: List<String>? = null,
    val diffBuckets: List<Int>? = null,
    val records: List<VersionedEntry>? = null,
    val stats: Map<String, Long>? = null
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

    val clusterConfig: ClusterConfig? = null,
    val leaderless: LeaderlessPayload? = null
)

val JsonConfig = Json {
    ignoreUnknownKeys = true
    encodeDefaults = true
    isLenient = true
}

fun Message.toJsonLine(): String = JsonConfig.encodeToString(Message.serializer(), this) + "\n"

fun String.toMessage(): Message = JsonConfig.decodeFromString(Message.serializer(), this.trim())
