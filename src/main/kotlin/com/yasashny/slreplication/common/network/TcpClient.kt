package com.yasashny.slreplication.common.network

import com.yasashny.slreplication.common.model.Message
import com.yasashny.slreplication.common.model.toJsonLine
import com.yasashny.slreplication.common.model.toMessage
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.PrintWriter
import java.net.Socket
import java.net.SocketException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

class TcpClient(
    private val host: String,
    private val port: Int,
    private val onMessage: (suspend (Message) -> Unit)? = null
) {
    private val logger = LoggerFactory.getLogger(TcpClient::class.java)
    private var socket: Socket? = null
    private var reader: BufferedReader? = null
    private var writer: PrintWriter? = null
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private val connected = AtomicBoolean(false)
    private val pendingResponses = ConcurrentHashMap<String, CompletableDeferred<Message>>()

    @Synchronized
    fun connect(): Boolean {
        return try {
            socket = Socket(host, port).apply {
                soTimeout = 0
                keepAlive = true
            }
            reader = BufferedReader(InputStreamReader(socket!!.getInputStream()))
            writer = PrintWriter(socket!!.getOutputStream(), true)
            connected.set(true)
            startReading()
            logger.debug("Connected to $host:$port")
            true
        } catch (e: Exception) {
            logger.debug("Failed to connect to $host:$port: ${e.message}")
            false
        }
    }

    private fun startReading() {
        scope.launch {
            try {
                while (connected.get() && socket?.isClosed == false) {
                    val line = withContext(Dispatchers.IO) { reader?.readLine() } ?: break
                    if (line.isBlank()) continue

                    try {
                        val message = line.toMessage()
                        message.requestId?.let { requestId ->
                            pendingResponses.remove(requestId)?.complete(message)
                        }
                        onMessage?.invoke(message)
                    } catch (e: Exception) {
                        logger.error("Error parsing message: $line", e)
                    }
                }
            } catch (_: SocketException) {
                logger.debug("Connection closed to $host:$port")
            } catch (e: Exception) {
                logger.error("Error reading from $host:$port", e)
            } finally {
                connected.set(false)
            }
        }
    }

    fun isConnected(): Boolean = connected.get() && socket?.isClosed == false

    @Synchronized
    fun send(message: Message): Boolean {
        if (!isConnected()) {
            if (!connect()) return false
        }
        return try {
            writer?.print(message.toJsonLine())
            writer?.flush()
            true
        } catch (e: Exception) {
            logger.error("Failed to send message to $host:$port", e)
            connected.set(false)
            false
        }
    }

    suspend fun sendAndWait(message: Message, timeoutMs: Long = 5000): Message? {
        val requestId = message.requestId ?: return null

        val deferred = CompletableDeferred<Message>()
        pendingResponses[requestId] = deferred

        if (!send(message)) {
            pendingResponses.remove(requestId)
            return null
        }

        return try {
            withTimeout(timeoutMs) {
                deferred.await()
            }
        } catch (e: TimeoutCancellationException) {
            pendingResponses.remove(requestId)
            null
        }
    }

    fun close() {
        connected.set(false)
        scope.cancel()
        pendingResponses.values.forEach { it.cancel() }
        pendingResponses.clear()
        try {
            socket?.close()
        } catch (_: Exception) {}
    }
}

class ConnectionPool {
    private val connections = ConcurrentHashMap<String, TcpClient>()

    fun getOrCreate(
        nodeId: String,
        host: String,
        port: Int,
        onMessage: (suspend (Message) -> Unit)? = null
    ): TcpClient {
        return connections.getOrPut(nodeId) {
            TcpClient(host, port, onMessage)
        }
    }

    fun get(nodeId: String): TcpClient? = connections[nodeId]

    fun add(nodeId: String, client: TcpClient) {
        connections[nodeId] = client
    }

    fun remove(nodeId: String) {
        connections.remove(nodeId)?.close()
    }

    fun getAll(): Map<String, TcpClient> = connections.toMap()

    fun closeAll() {
        connections.values.forEach { it.close() }
        connections.clear()
    }
}
