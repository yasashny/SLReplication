package com.yasashny.slreplication.cli

import com.yasashny.slreplication.common.model.Message
import kotlinx.coroutines.runBlocking

class RequestSender(
    private val registry: NodeRegistry,
    private val state: ClusterState,
    private val baseTimeoutMs: Long = 10000L
) {
    fun send(target: String, request: Message, timeoutMs: Long? = null): Result<Message> {
        val client = registry.getConnection(target)
            ?: return Result.failure(Exception("Node $target not connected"))
        return runBlocking {
            val effectiveTimeout = timeoutMs ?: (baseTimeoutMs + state.replicationDelayMaxMs)
            val response = client.sendAndWait(request, effectiveTimeout)
            if (response != null) Result.success(response)
            else Result.failure(Exception("Timeout waiting for response from $target"))
        }
    }
}
