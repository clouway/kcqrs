package com.clouway.kcqrs.core

import com.clouway.kcqrs.core.messages.MessageFormat

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
/**
 * Simple interface to an aggregate root
 */
interface AggregateRoot {
    /**
     * Gets the ID of the aggregate
     *
     * @return the ID of the aggregate
     */
    fun getId(): String?

    /**
     * Gets all change events since the
     * original hydration. If there are no
     * changes then null is returned
     *
     * @return
     */
    fun getUncommittedChanges(): List<Any>

    /**
     * Mark all changes a committed
     */
    fun markChangesAsCommitted()

    /**
     * load the aggregate root
     *
     * @param history
     * @param version the version of the aggregate
     * @throws HydrationException
     */
    fun loadFromHistory(history: Iterable<Any>, version: Long)

    /**
     * Returns the version of the aggregate when it was hydrated
     * @return
     */
    fun getExpectedVersion(): Long

    /**
     * Returns a SnapshotMapper that will be used in creation
     * of Snapshots for the EventStore
     */
    fun getSnapshotMapper(): SnapshotMapper<AggregateRoot>
    
    /**
     * Gets the snapshot data type which to be used for parsing of the message.
     */
    fun getSnapshotDataType(): Class<*>?

    /**
     * Builds an aggregate from snapshot data and the current version of the snapshot
     */
    fun <T : AggregateRoot> fromSnapshot(snapshotData: ByteArray, snapshotVersion: Long, messageFormat: MessageFormat): T
    
}


interface SnapshotMapper<T : AggregateRoot> {

    /**
     * Serializes the current entity to a string snapshot
     */
    fun toSnapshot(data: T, messageFormat: MessageFormat): Snapshot

    /**
     * Create an aggregate from given snapshot
     */
    fun fromSnapshot(snapshot: ByteArray, snapshotVersion: Long, messageFormat: MessageFormat): T
}