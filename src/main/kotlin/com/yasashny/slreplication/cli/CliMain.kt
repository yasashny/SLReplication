package com.yasashny.slreplication.cli

import com.yasashny.slreplication.common.model.ClusterMode
import com.yasashny.slreplication.common.model.ReplicationMode
import com.yasashny.slreplication.common.model.Topology
import org.slf4j.LoggerFactory
import java.util.Scanner

fun main() {
    val logger = LoggerFactory.getLogger("CLI")
    val manager = ClusterManager()
    val scanner = Scanner(System.`in`)

    println("=== KV Store CLI ===")
    println("Type 'help' for available commands")
    println()

    Runtime.getRuntime().addShutdownHook(Thread {
        manager.close()
    })

    while (true) {
        print("> ")
        val line = scanner.nextLine().trim()
        if (line.isBlank()) continue

        val parts = parseCommand(line)
        val command = parts.firstOrNull()?.lowercase() ?: continue
        val cmdArgs = parts.drop(1)

        try {
            when (command) {
                "help" -> printHelp()
                "exit", "quit" -> {
                    manager.close()
                    println("Bye!")
                    return
                }

                // ===== Cluster management =====

                "addnode" -> {
                    if (cmdArgs.size < 3) {
                        println("Usage: addNode <nodeId> <host> <port>")
                    } else {
                        val port = cmdArgs[2].toIntOrNull()
                        if (port == null) println("Invalid port: ${cmdArgs[2]}")
                        else {
                            val result = manager.addNode(cmdArgs[0], cmdArgs[1], port)
                            result.onSuccess { println(it) }
                            result.onFailure { println("Error: ${it.message}") }
                        }
                    }
                }

                "removenode" -> {
                    if (cmdArgs.isEmpty()) println("Usage: removeNode <nodeId>")
                    else {
                        val result = manager.removeNode(cmdArgs[0])
                        result.onSuccess { println(it) }
                        result.onFailure { println("Error: ${it.message}") }
                    }
                }

                "listnodes" -> {
                    val nodes = manager.listNodes()
                    if (nodes.isEmpty()) {
                        println("No nodes in cluster")
                    } else {
                        println("Nodes in cluster:")
                        val config = manager.getConfig()
                        nodes.sortedBy { it.nodeId }.forEach { node ->
                            val role = when {
                                config.mode == ClusterMode.MULTI && node.nodeId in config.leaderNodeIds -> " (LEADER)"
                                config.mode == ClusterMode.MULTI -> " (FOLLOWER)"
                                node.nodeId == config.leaderId -> " (LEADER)"
                                else -> ""
                            }
                            val extra = if (config.topology == Topology.STAR && node.nodeId == config.starCenterId) " [STAR CENTER]" else ""
                            println("  ${node.nodeId}: ${node.host}:${node.port}$role$extra")
                        }
                    }
                }

                "setleader" -> {
                    if (cmdArgs.isEmpty()) println("Usage: setLeader <nodeId>")
                    else {
                        val result = manager.setLeader(cmdArgs[0])
                        result.onSuccess { println(it) }
                        result.onFailure { println("Error: ${it.message}") }
                    }
                }

                "setmode" -> {
                    if (cmdArgs.isEmpty()) {
                        println("Usage: setMode single|multi")
                    } else {
                        val m = when (cmdArgs[0].lowercase()) {
                            "single" -> ClusterMode.SINGLE
                            "multi" -> ClusterMode.MULTI
                            else -> { println("Unknown mode: ${cmdArgs[0]}. Use single or multi"); null }
                        }
                        if (m != null) {
                            val result = manager.setMode(m)
                            result.onSuccess { println(it) }
                            result.onFailure { println("Error: ${it.message}") }
                        }
                    }
                }

                "settopology" -> {
                    if (cmdArgs.isEmpty()) {
                        println("Usage: setTopology mesh|ring|star")
                    } else {
                        val t = when (cmdArgs[0].lowercase()) {
                            "mesh" -> Topology.MESH
                            "ring" -> Topology.RING
                            "star" -> Topology.STAR
                            else -> { println("Unknown topology: ${cmdArgs[0]}. Use mesh, ring, or star"); null }
                        }
                        if (t != null) {
                            val result = manager.setTopology(t)
                            result.onSuccess { println(it) }
                            result.onFailure { println("Error: ${it.message}") }
                        }
                    }
                }

                "setstarcenter" -> {
                    if (cmdArgs.isEmpty()) println("Usage: setStarCenter <nodeId>")
                    else {
                        val result = manager.setStarCenter(cmdArgs[0])
                        result.onSuccess { println(it) }
                        result.onFailure { println("Error: ${it.message}") }
                    }
                }

                "setleaders" -> {
                    if (cmdArgs.isEmpty()) println("Usage: setLeaders <nodeId1> [nodeId2] ...")
                    else {
                        val result = manager.setLeaders(cmdArgs)
                        result.onSuccess { println(it) }
                        result.onFailure { println("Error: ${it.message}") }
                    }
                }

                // ===== Single-leader replication settings =====

                "setreplication" -> {
                    if (cmdArgs.isEmpty()) {
                        println("Usage: setReplication async|semi-sync|sync")
                    } else {
                        val rMode = when (cmdArgs[0].lowercase()) {
                            "async" -> ReplicationMode.ASYNC
                            "semi-sync", "semisync" -> ReplicationMode.SEMI_SYNC
                            "sync" -> ReplicationMode.SYNC
                            else -> { println("Unknown mode: ${cmdArgs[0]}"); null }
                        }
                        if (rMode != null) {
                            val result = manager.setReplicationMode(rMode)
                            result.onSuccess { println(it) }
                            result.onFailure { println("Error: ${it.message}") }
                        }
                    }
                }

                "setrf" -> {
                    if (cmdArgs.isEmpty()) println("Usage: setRF <int>")
                    else {
                        val rf = cmdArgs[0].toIntOrNull()
                        if (rf == null) println("Invalid RF: ${cmdArgs[0]}")
                        else {
                            val result = manager.setReplicationFactor(rf)
                            result.onSuccess { println(it) }
                            result.onFailure { println("Error: ${it.message}") }
                        }
                    }
                }

                "setsemisyncacks" -> {
                    if (cmdArgs.isEmpty()) println("Usage: setSemiSyncAcks <int>")
                    else {
                        val k = cmdArgs[0].toIntOrNull()
                        if (k == null) println("Invalid K: ${cmdArgs[0]}")
                        else {
                            val result = manager.setSemiSyncAcks(k)
                            result.onSuccess { println(it) }
                            result.onFailure { println("Error: ${it.message}") }
                        }
                    }
                }

                "setreplicationdelayms" -> {
                    if (cmdArgs.size < 2) println("Usage: setReplicationDelayMs <min> <max>")
                    else {
                        val min = cmdArgs[0].toLongOrNull()
                        val max = cmdArgs[1].toLongOrNull()
                        if (min == null || max == null) println("Invalid delay values")
                        else {
                            val result = manager.setReplicationDelay(min, max)
                            result.onSuccess { println(it) }
                            result.onFailure { println("Error: ${it.message}") }
                        }
                    }
                }

                // ===== Status =====

                "status" -> {
                    val config = manager.getConfig()
                    println("Cluster status:")
                    println("  Mode: ${config.mode}")
                    println("  Nodes: ${config.nodes.size}")
                    if (config.mode == ClusterMode.SINGLE) {
                        println("  Leader: ${config.leaderId ?: "not set"}")
                        println("  Replication mode: ${config.replicationMode}")
                        println("  Replication factor: ${config.replicationFactor}")
                        if (config.replicationMode == ReplicationMode.SEMI_SYNC) {
                            println("  Semi-sync ACKs (K): ${config.semiSyncAcks}")
                        }
                    } else {
                        println("  Topology: ${config.topology}")
                        println("  Leaders: ${config.leaderNodeIds.joinToString(", ").ifEmpty { "not set" }}")
                        if (config.topology == Topology.STAR) {
                            println("  Star center: ${config.starCenterId ?: "not set"}")
                        }
                    }
                    println("  Replication delay: ${config.replicationDelayMinMs}-${config.replicationDelayMaxMs} ms")
                }

                // ===== User commands =====

                "put" -> {
                    val parsed = parseUserCommand(cmdArgs)
                    if (parsed.args.size < 2) {
                        println("Usage: put <key> <value> [--target <nodeId>] [--client <clientId>]")
                    } else {
                        val key = parsed.args[0]
                        val value = parsed.args.drop(1).joinToString(" ")
                        val result = manager.put(key, value, parsed.target)
                        result.onSuccess { response ->
                            if (response.status == "OK") println("OK")
                            else {
                                println("Error: ${response.errorCode} - ${response.errorMessage}")
                                response.leaderNodeId?.let { println("Leader: $it") }
                            }
                        }
                        result.onFailure { println("Error: ${it.message}") }
                    }
                }

                "get" -> {
                    val parsed = parseUserCommand(cmdArgs)
                    if (parsed.args.isEmpty()) {
                        println("Usage: get <key> [--target <nodeId>] [--client <clientId>]")
                    } else {
                        val key = parsed.args[0]
                        val result = manager.get(key, parsed.target)
                        result.onSuccess { response ->
                            if (response.status == "OK") {
                                if (response.value != null) {
                                    val v = response.version?.let { " [v=(${it.lamport}, ${it.nodeId})]" } ?: ""
                                    println("${response.key} = ${response.value}$v")
                                } else {
                                    println("(nil)")
                                }
                            } else {
                                println("Error: ${response.errorCode} - ${response.errorMessage}")
                            }
                        }
                        result.onFailure { println("Error: ${it.message}") }
                    }
                }

                "dump" -> {
                    val parsed = parseUserCommand(cmdArgs)
                    val result = manager.dump(parsed.target)
                    result.onSuccess { response ->
                        if (response.status == "OK") {
                            val vd = response.versionedData
                            if (!vd.isNullOrEmpty()) {
                                println("Data (${vd.size} entries):")
                                vd.sortedBy { it.key }.forEach { entry ->
                                    val v = entry.version?.let { " [v=(${it.lamport}, ${it.nodeId})]" } ?: ""
                                    println("  ${entry.key} = ${entry.value}$v")
                                }
                            } else {
                                val data = response.data
                                if (data.isNullOrEmpty()) println("(empty)")
                                else {
                                    println("Data (${data.size} entries):")
                                    data.toSortedMap().forEach { (k, v) -> println("  $k = $v") }
                                }
                            }
                        } else {
                            println("Error: ${response.errorCode} - ${response.errorMessage}")
                        }
                    }
                    result.onFailure { println("Error: ${it.message}") }
                }

                // ===== Debug commands =====

                "getall" -> {
                    if (cmdArgs.isEmpty()) {
                        println("Usage: getAll <key>")
                    } else {
                        val key = cmdArgs[0]
                        val results = manager.getAll(key)
                        println("Key '$key' across all nodes:")
                        for ((nid, res) in results) {
                            res.onSuccess { response ->
                                if (response.status == "OK") {
                                    if (response.value != null) {
                                        val v = response.version?.let { " [v=(${it.lamport}, ${it.nodeId})]" } ?: ""
                                        println("  $nid: ${response.value}$v")
                                    } else {
                                        println("  $nid: (nil)")
                                    }
                                } else {
                                    println("  $nid: Error: ${response.errorCode}")
                                }
                            }
                            res.onFailure { println("  $nid: Error: ${it.message}") }
                        }
                    }
                }

                "clusterdump" -> {
                    val results = manager.clusterDump()
                    println("Cluster dump:")
                    for ((nid, res) in results) {
                        res.onSuccess { response ->
                            if (response.status == "OK") {
                                val vd = response.versionedData
                                if (!vd.isNullOrEmpty()) {
                                    println("  Node $nid (${vd.size} entries):")
                                    vd.sortedBy { it.key }.forEach { entry ->
                                        val v = entry.version?.let { " [v=(${it.lamport}, ${it.nodeId})]" } ?: ""
                                        println("    ${entry.key} = ${entry.value}$v")
                                    }
                                } else {
                                    val data = response.data
                                    if (data.isNullOrEmpty()) println("  Node $nid: (empty)")
                                    else {
                                        println("  Node $nid (${data.size} entries):")
                                        data.toSortedMap().forEach { (k, v) -> println("    $k = $v") }
                                    }
                                }
                            } else {
                                println("  Node $nid: Error: ${response.errorCode}")
                            }
                        }
                        res.onFailure { println("  Node $nid: Error: ${it.message}") }
                    }
                }

                // ===== Benchmark =====

                "benchmark" -> runBenchmark(manager, cmdArgs)

                else -> println("Unknown command: $command. Type 'help' for available commands.")
            }
        } catch (e: Exception) {
            println("Error: ${e.message}")
            logger.error("Command error", e)
        }
    }
}

private fun parseCommand(line: String): List<String> {
    val result = mutableListOf<String>()
    var current = StringBuilder()
    var inQuotes = false

    for (char in line) {
        when {
            char == '"' -> inQuotes = !inQuotes
            char == ' ' && !inQuotes -> {
                if (current.isNotEmpty()) {
                    result.add(current.toString())
                    current = StringBuilder()
                }
            }
            else -> current.append(char)
        }
    }
    if (current.isNotEmpty()) result.add(current.toString())
    return result
}

data class ParsedUserCommand(
    val args: List<String>,
    val target: String?,
    val client: String?
)

private fun parseUserCommand(args: List<String>): ParsedUserCommand {
    var target: String? = null
    var client: String? = null
    val positionalArgs = mutableListOf<String>()
    var i = 0
    while (i < args.size) {
        when (args[i]) {
            "--target" -> { if (i + 1 < args.size) target = args[++i] }
            "--client" -> { if (i + 1 < args.size) client = args[++i] }
            else -> positionalArgs.add(args[i])
        }
        i++
    }
    return ParsedUserCommand(positionalArgs, target, client)
}

private fun printHelp() {
    println("""
        === Cluster Management ===
        addNode <nodeId> <host> <port>      - Add a node to the cluster
        removeNode <nodeId>                 - Remove a node from the cluster
        listNodes                           - List all nodes
        setLeader <nodeId>                  - Set the leader node (single mode)
        setMode single|multi                - Set cluster mode
        setTopology mesh|ring|star          - Set replication topology (multi mode)
        setStarCenter <nodeId>              - Set star center node (star topology)
        setLeaders <id1> [id2] ...          - Set leader nodes (multi mode)
        setReplication async|semi-sync|sync - Set replication mode (single mode)
        setRF <int>                         - Set replication factor (single mode)
        setSemiSyncAcks <int>               - Set K for semi-sync mode
        setReplicationDelayMs <min> <max>   - Set replication delay range
        status                              - Show cluster status

        === User Commands ===
        put <key> <value> [--target <nodeId>]  - Put a key-value pair
        get <key> [--target <nodeId>]          - Get a value by key
        dump [--target <nodeId>]               - Dump all data from a node

        === Debug Commands ===
        getAll <key>                        - Get key from all nodes (value + version)
        clusterDump                         - Dump all data from all nodes

        === Benchmark ===
        benchmark [options]                 - Run benchmarks (--help for options)

        === Other ===
        help                                - Show this help
        exit, quit                          - Exit the CLI
    """.trimIndent())
}
