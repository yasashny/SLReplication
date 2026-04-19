package com.yasashny.slreplication.node.replication

import com.yasashny.slreplication.common.model.Message
import com.yasashny.slreplication.common.model.MessageType
import com.yasashny.slreplication.common.network.TcpClient

internal fun sendReplicationAck(
    localNodeId: String,
    targetNodeId: String,
    operationId: String,
    getNodeConnection: (String) -> TcpClient?
): Boolean {
    val client = getNodeConnection(targetNodeId) ?: return false
    val ack = Message(type = MessageType.REPL_ACK, operationId = operationId, fromNodeId = localNodeId)
    return client.send(ack)
}
