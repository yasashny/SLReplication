package com.yasashny.slreplication.cli

import com.yasashny.slreplication.common.model.Message
import com.yasashny.slreplication.common.model.MessageType
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import java.util.UUID

class ConfigBroadcaster(
    private val registry: NodeRegistry,
    private val state: ClusterState,
    private val clientId: String
) {
    private val logger = LoggerFactory.getLogger(ConfigBroadcaster::class.java)

    fun broadcast() {
        val config = state.buildConfig(registry.list())
        val message = Message(
            type = MessageType.CLUSTER_UPDATE,
            requestId = UUID.randomUUID().toString(),
            clientId = clientId,
            clusterConfig = config
        )
        runBlocking {
            for ((nodeId, client) in registry.allConnections()) {
                try {
                    val response = client.sendAndWait(message, 5000)
                    if (response?.status == "OK") {
                        logger.debug("Cluster update acknowledged by $nodeId")
                    } else {
                        logger.warn("Node $nodeId did not acknowledge cluster update: ${response?.errorMessage}")
                    }
                } catch (e: Exception) {
                    logger.warn("Failed to send cluster update to $nodeId: ${e.message}")
                }
            }
        }
    }
}
