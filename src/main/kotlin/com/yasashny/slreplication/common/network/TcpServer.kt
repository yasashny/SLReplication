package com.yasashny.slreplication.common.network

import com.yasashny.slreplication.common.model.Message
import com.yasashny.slreplication.common.model.toJsonLine
import com.yasashny.slreplication.common.model.toMessage
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.PrintWriter
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketException
import java.util.concurrent.ConcurrentHashMap

class TcpServer(
    private val port: Int,
    private val messageHandler: suspend (Message, MessageSender) -> Unit
) {
    private val logger = LoggerFactory.getLogger(TcpServer::class.java)
    private var serverSocket: ServerSocket? = null
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private val connections = ConcurrentHashMap<String, ClientConnection>()

    @Volatile
    private var running = false

    fun start() {
        running = true
        serverSocket = ServerSocket(port)
        logger.info("TCP Server started on port $port")

        scope.launch {
            while (running) {
                try {
                    val socket = serverSocket?.accept() ?: break
                    handleConnection(socket)
                } catch (e: SocketException) {
                    if (running) {
                        logger.error("Socket error: ${e.message}")
                    }
                } catch (e: Exception) {
                    if (running) {
                        logger.error("Error accepting connection", e)
                    }
                }
            }
        }
    }

    private fun handleConnection(socket: Socket) {
        val connectionId = "${socket.inetAddress.hostAddress}:${socket.port}"
        logger.debug("New connection from $connectionId")

        scope.launch {
            try {
                val reader = BufferedReader(InputStreamReader(socket.getInputStream()))
                val writer = PrintWriter(socket.getOutputStream(), true)

                val connection = ClientConnection(connectionId, socket, writer)
                connections[connectionId] = connection

                val sender = object : MessageSender {
                    override suspend fun send(message: Message) {
                        withContext(Dispatchers.IO) {
                            synchronized(writer) {
                                writer.print(message.toJsonLine())
                                writer.flush()
                            }
                        }
                    }
                }

                while (running && !socket.isClosed) {
                    val line = withContext(Dispatchers.IO) { reader.readLine() } ?: break
                    if (line.isBlank()) continue

                    scope.launch {
                        try {
                            val message = line.toMessage()
                            messageHandler(message, sender)
                        } catch (e: Exception) {
                            logger.error("Error handling message: $line", e)
                        }
                    }
                }
            } catch (_: SocketException) {
                logger.debug("Connection closed: $connectionId")
            } catch (e: Exception) {
                logger.error("Error handling connection $connectionId", e)
            } finally {
                connections.remove(connectionId)
                try {
                    socket.close()
                } catch (_: Exception) {}
            }
        }
    }

    fun stop() {
        running = false
        connections.values.forEach { it.close() }
        connections.clear()
        serverSocket?.close()
        scope.cancel()
        logger.info("TCP Server stopped")
    }
}

interface MessageSender {
    suspend fun send(message: Message)
}

data class ClientConnection(
    val id: String,
    val socket: Socket,
    val writer: PrintWriter
) {
    fun close() {
        try {
            socket.close()
        } catch (_: Exception) {}
    }
}
