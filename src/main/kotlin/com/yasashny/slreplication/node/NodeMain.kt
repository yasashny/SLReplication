package com.yasashny.slreplication.node

import org.slf4j.LoggerFactory

fun main(args: Array<String>) {
    val logger = LoggerFactory.getLogger("NodeMain")
    if (args.size < 2) {
        println("Usage: kv-node <nodeId> <port>")
        println("Example: kv-node A 5001")
        return
    }
    val nodeId = args[0]
    val port = args[1].toIntOrNull()
    if (port == null || port < 1 || port > 65535) {
        println("Invalid port: ${args[1]}")
        return
    }
    val node = Node(nodeId, port)
    Runtime.getRuntime().addShutdownHook(Thread {
        logger.info("Shutting down node $nodeId...")
        node.stop()
    })
    node.start()
    println("Node $nodeId started on port $port. Press Ctrl+C to stop.")
    Thread.currentThread().join()
}
