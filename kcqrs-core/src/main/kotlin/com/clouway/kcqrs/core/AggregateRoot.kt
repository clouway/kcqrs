package com.clouway.kcqrs.core

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
    fun getUncommittedChanges(): List<Event>

    /**
     * Mark all changes a committed
     */
    fun markChangesAsCommitted()

    /**
     * load the aggregate root
     *
     * @param history
     * @throws HydrationException
     */
    fun loadFromHistory(history: Iterable<Event>)

    /**
     * Returns the version of the aggregate when it was hydrated
     * @return
     */
    fun getExpectedVersion(): Long

}