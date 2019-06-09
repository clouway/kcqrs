package com.clouway.kcqrs.core

import java.net.NetworkInterface
import java.security.SecureRandom
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset

/**
 * Distributed Sequence Generator Inspired by Twitter snowflake: https://github.com/twitter/snowflake/tree/snowflake-2010
 *
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
internal class SnowflakeIdGenerator : IdGenerator {
    private val nodeId: Int
    private val epoch: Long

    @Volatile
    private var lastTimestamp = -1L
    @Volatile
    private var sequence = 0L

    /**
     * Pass nodeId
     */
    constructor(nodeId: Int, epoch: LocalDateTime) {
        if (nodeId < 0 || nodeId > maxNodeId) {
            throw IllegalArgumentException(String.format("NodeId must be between %d and %d", 0, maxNodeId))
        }
        this.nodeId = nodeId
        this.epoch = epoch.toInstant(ZoneOffset.UTC).toEpochMilli()
    }

    /**
     * The nodeId is generated automatically.
     */
    constructor() {
        this.nodeId = createNodeId()
        this.epoch = LocalDateTime.of(2015, 1, 1, 0, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli()
    }

    override fun nextIds(size: Int): List<Long> {
        val ids = mutableListOf<Long>()
        for (i in 1..size) {
            ids.add(nextId())
        }
        return ids
    }

    @Synchronized
    override fun nextId(): Long {
        var currentTimestamp = timestamp()

        if (currentTimestamp < lastTimestamp) {
            throw IllegalStateException("Invalid System Clock!")
        }

        if (currentTimestamp == lastTimestamp) {
            sequence = sequence + 1 and maxSequence
            if (sequence == 0L) {
                // Sequence Exhausted, wait till next millisecond.
                currentTimestamp = waitNextMillis(currentTimestamp)
            }
        } else {
            // reset sequence to start with zero for the next millisecond
            sequence = 0
        }

        lastTimestamp = currentTimestamp

        var id = currentTimestamp shl TOTAL_BITS - EPOCH_BITS
        id = id or (nodeId shl TOTAL_BITS - EPOCH_BITS - NODE_ID_BITS).toLong()
        id = id or sequence
        return id
    }

    // Block and wait till next millisecond
    private fun waitNextMillis(timestamp: Long): Long {
        var currentTimestamp = timestamp
        while (currentTimestamp == lastTimestamp) {
            currentTimestamp = timestamp()
        }
        return currentTimestamp
    }

    private fun createNodeId(): Int {
        var nodeId: Int
        try {
            val sb = StringBuilder()
            val networkInterfaces = NetworkInterface.getNetworkInterfaces()
            while (networkInterfaces.hasMoreElements()) {
                val networkInterface = networkInterfaces.nextElement()
                val mac = networkInterface.hardwareAddress
                if (mac != null) {
                    for (i in mac.indices) {
                        sb.append(String.format("%02X", mac[i]))
                    }
                }
            }
            nodeId = sb.toString().hashCode()
        } catch (ex: Exception) {
            nodeId = SecureRandom().nextInt()
        }

        nodeId = nodeId and maxNodeId
        return nodeId
    }

    // Get current timestamp in milliseconds, adjust for the custom epoch.
    private fun timestamp(): Long {
        return Instant.now().toEpochMilli() - epoch
    }

    companion object {
        private const val TOTAL_BITS = 64
        private const val EPOCH_BITS = 42
        private const val NODE_ID_BITS = 10
        private const val SEQUENCE_BITS = 12

        private val maxNodeId = (Math.pow(2.0, NODE_ID_BITS.toDouble()) - 1).toInt()
        private val maxSequence = (Math.pow(2.0, SEQUENCE_BITS.toDouble()) - 1).toLong()
    }
}