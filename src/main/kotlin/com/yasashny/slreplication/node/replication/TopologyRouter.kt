package com.yasashny.slreplication.node.replication

import com.yasashny.slreplication.common.model.ClusterConfig
import com.yasashny.slreplication.common.model.Topology

class TopologyRouter(private val nodeId: String) {

    fun getInitialTargets(config: ClusterConfig): List<String> {
        val otherLeaders = sortedLeaders(config).filter { it != nodeId }
        val leaderTargets = when (config.topology) {
            Topology.MESH -> otherLeaders
            Topology.RING -> listOfNotNull(nextLeaderInRing(config))
            Topology.STAR -> {
                if (nodeId == config.starCenterId) otherLeaders
                else listOfNotNull(config.starCenterId?.takeIf { it != nodeId })
            }
        }
        return leaderTargets + myFollowers(config)
    }

    fun getForwardTargets(config: ClusterConfig, originNodeId: String, sourceNodeId: String): List<String> {
        val isLeader = nodeId in config.leaderNodeIds
        val leaderTargets = when (config.topology) {
            Topology.MESH -> emptyList()
            Topology.RING -> {
                if (isLeader) {
                    val next = nextLeaderInRing(config)
                    if (next != null && next != originNodeId) listOf(next) else emptyList()
                } else emptyList()
            }
            Topology.STAR -> {
                if (nodeId == config.starCenterId) {
                    sortedLeaders(config)
                        .filter { it != nodeId && it != sourceNodeId && it != originNodeId }
                } else emptyList()
            }
        }
        val followers = if (isLeader) myFollowers(config) else emptyList()
        return leaderTargets + followers
    }

    private fun nextLeaderInRing(config: ClusterConfig): String? {
        val leaders = sortedLeaders(config)
        val idx = leaders.indexOf(nodeId)
        if (idx == -1) return null
        return leaders[(idx + 1) % leaders.size]
    }

    fun myFollowers(config: ClusterConfig): List<String> {
        val leaders = sortedLeaders(config)
        val leaderSet = leaders.toSet()
        val followers = config.nodes.map { it.nodeId }.filter { it !in leaderSet }.sorted()
        val myIndex = leaders.indexOf(nodeId)
        if (myIndex == -1) return emptyList()
        return followers.filterIndexed { index, _ -> index % leaders.size == myIndex }
    }

    private fun sortedLeaders(config: ClusterConfig): List<String> =
        config.leaderNodeIds.sorted()
}
