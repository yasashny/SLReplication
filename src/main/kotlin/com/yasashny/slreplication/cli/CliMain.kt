package com.yasashny.slreplication.cli

import com.yasashny.slreplication.common.model.ClusterMode
import com.yasashny.slreplication.common.model.ReplicationMode
import com.yasashny.slreplication.common.model.Topology
import com.yasashny.slreplication.common.model.WriteQuorumMode
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
                                config.mode == ClusterMode.LEADERLESS && node.nodeId in config.homeReplicaIds -> " (HOME)"
                                config.mode == ClusterMode.LEADERLESS && node.nodeId in config.spareNodeIds -> " (SPARE)"
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
                        println("Usage: setMode single|multi|leaderless")
                    } else {
                        val m = when (cmdArgs[0].lowercase()) {
                            "single" -> ClusterMode.SINGLE
                            "multi" -> ClusterMode.MULTI
                            "leaderless" -> ClusterMode.LEADERLESS
                            else -> { println("Unknown mode: ${cmdArgs[0]}. Use single, multi, or leaderless"); null }
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


                "status" -> {
                    val config = manager.getConfig()
                    println("Cluster status:")
                    println("  Mode: ${config.mode}")
                    println("  Nodes: ${config.nodes.size}")
                    when (config.mode) {
                        ClusterMode.SINGLE -> {
                            println("  Leader: ${config.leaderId ?: "not set"}")
                            println("  Replication mode: ${config.replicationMode}")
                            println("  Replication factor: ${config.replicationFactor}")
                            if (config.replicationMode == ReplicationMode.SEMI_SYNC) {
                                println("  Semi-sync ACKs (K): ${config.semiSyncAcks}")
                            }
                        }
                        ClusterMode.MULTI -> {
                            println("  Topology: ${config.topology}")
                            println("  Leaders: ${config.leaderNodeIds.joinToString(", ").ifEmpty { "not set" }}")
                            if (config.topology == Topology.STAR) {
                                println("  Star center: ${config.starCenterId ?: "not set"}")
                            }
                        }
                        ClusterMode.LEADERLESS -> {
                            println("  Home replicas: ${config.homeReplicaIds.joinToString(", ").ifEmpty { "not set" }}")
                            println("  Spare nodes: ${config.spareNodeIds.joinToString(", ").ifEmpty { "not set" }}")
                            println("  Write quorum (W): ${config.writeQuorum}")
                            println("  Read quorum (R): ${config.readQuorum}")
                            println("  Write quorum mode: ${config.writeQuorumMode}")
                        }
                    }
                    println("  Replication delay: ${config.replicationDelayMinMs}-${config.replicationDelayMaxMs} ms")
                }


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


                "sethomereplicas" -> {
                    if (cmdArgs.size < 5) println("Usage: setHomeReplicas <id1> <id2> <id3> <id4> <id5>")
                    else {
                        val result = manager.setHomeReplicas(cmdArgs.take(5))
                        result.onSuccess { println(it) }
                        result.onFailure { println("Error: ${it.message}") }
                    }
                }

                "setsparenodes" -> {
                    if (cmdArgs.size < 2) println("Usage: setSpareNodes <id1> <id2>")
                    else {
                        val result = manager.setSpareNodes(cmdArgs.take(2))
                        result.onSuccess { println(it) }
                        result.onFailure { println("Error: ${it.message}") }
                    }
                }

                "setquorum" -> {
                    if (cmdArgs.size < 2) println("Usage: setQuorum <W> <R>")
                    else {
                        val w = cmdArgs[0].toIntOrNull()
                        val r = cmdArgs[1].toIntOrNull()
                        if (w == null || r == null) println("Invalid quorum values")
                        else {
                            val result = manager.setQuorum(w, r)
                            result.onSuccess { println(it) }
                            result.onFailure { println("Error: ${it.message}") }
                        }
                    }
                }

                "setwritequorummode" -> {
                    if (cmdArgs.isEmpty()) println("Usage: setWriteQuorumMode strict|sloppy")
                    else {
                        val wqm = when (cmdArgs[0].lowercase()) {
                            "strict" -> WriteQuorumMode.STRICT
                            "sloppy" -> WriteQuorumMode.SLOPPY
                            else -> { println("Unknown mode: ${cmdArgs[0]}. Use strict or sloppy"); null }
                        }
                        if (wqm != null) {
                            val result = manager.setWriteQuorumMode(wqm)
                            result.onSuccess { println(it) }
                            result.onFailure { println("Error: ${it.message}") }
                        }
                    }
                }

                "dumphints" -> {
                    val parsed = parseUserCommand(cmdArgs)
                    val result = manager.dumpHints(parsed.target)
                    result.onSuccess { response ->
                        if (response.status == "OK") {
                            val hints = response.leaderless?.hints
                            if (hints.isNullOrEmpty()) println("(no hints)")
                            else {
                                println("Hints (${hints.size} entries):")
                                hints.forEach { h ->
                                    println("  ${h.key}=${h.value} -> ${h.intendedHomeNodeId} [v=(${h.version.lamport},${h.version.nodeId})] opId=${h.operationId}")
                                }
                            }
                        } else println("Error: ${response.errorCode} - ${response.errorMessage}")
                    }
                    result.onFailure { println("Error: ${it.message}") }
                }

                "runhintedhandoff" -> {
                    val parsed = parseUserCommand(cmdArgs)
                    val results = manager.runHintedHandoff(parsed.target)
                    for ((nid, res) in results) {
                        res.onSuccess { response ->
                            val stats = response.leaderless?.stats
                            println("  $nid: delivered=${stats?.get("hintsDelivered") ?: 0}, failed=${stats?.get("hintsFailed") ?: 0}")
                        }
                        res.onFailure { println("  $nid: Error: ${it.message}") }
                    }
                }

                "showmerkleroot" -> {
                    if (cmdArgs.isEmpty()) println("Usage: showMerkleRoot <nodeId>")
                    else {
                        val result = manager.showMerkleRoot(cmdArgs[0])
                        result.onSuccess { response ->
                            println("Merkle root for ${cmdArgs[0]}: ${response.leaderless?.merkleRoot ?: "(empty)"}")
                        }
                        result.onFailure { println("Error: ${it.message}") }
                    }
                }

                "wipenodedata" -> {
                    if (cmdArgs.isEmpty()) println("Usage: wipeNodeData <nodeId>")
                    else {
                        val result = manager.wipeNodeData(cmdArgs[0])
                        result.onSuccess { println("Node ${cmdArgs[0]} data wiped") }
                        result.onFailure { println("Error: ${it.message}") }
                    }
                }

                "runantientropycluster" -> {
                    println("Running cluster-wide anti-entropy...")
                    val result = manager.runAntiEntropyCluster()
                    result.onSuccess { println(it) }
                    result.onFailure { println("Error: ${it.message}") }
                }


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
        setMode single|multi|leaderless     - Set cluster mode
        setTopology mesh|ring|star          - Set replication topology (multi mode)
        setStarCenter <nodeId>              - Set star center node (star topology)
        setLeaders <id1> [id2] ...          - Set leader nodes (multi mode)
        setReplication async|semi-sync|sync - Set replication mode (single mode)
        setRF <int>                         - Set replication factor (single mode)
        setSemiSyncAcks <int>               - Set K for semi-sync mode
        setReplicationDelayMs <min> <max>   - Set replication delay range
        status                              - Show cluster status

        === Leaderless Mode ===
        setHomeReplicas <id1>..<id5>        - Set 5 home replica nodes
        setSpareNodes <id1> <id2>           - Set 2 spare nodes
        setQuorum <W> <R>                   - Set write/read quorum (W+R>5)
        setWriteQuorumMode strict|sloppy    - Set write quorum mode
        dumpHints [--target <nodeId>]       - Show hints on a node
        runHintedHandoff [--target <nodeId>]- Deliver hints to home replicas
        showMerkleRoot <nodeId>             - Show Merkle tree root hash
        wipeNodeData <nodeId>               - Clear node's data (simulate crash)
        runAntiEntropyCluster               - Run cluster-wide anti-entropy

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
