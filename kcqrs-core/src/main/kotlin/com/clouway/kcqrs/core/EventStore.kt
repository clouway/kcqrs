package com.clouway.kcqrs.core

import java.util.*

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
interface EventStore {
    
    /**
     * Persist the changes
     *
     * @param aggregateId
     * @param expectedVersion
     * @param events
     * @throws EventCollisionException
     */
    @Throws(EventCollisionException::class)
    fun saveEvents(aggregateId: UUID, expectedVersion: Int, events: Iterable<Event>)

    /**
     * Retrieves the events
     *
     * @param aggregateId
     * @return
     * @throws AggregateNotFoundException
     */
    @Throws(AggregateNotFoundException::class)
    fun <T> getEvents(aggregateId: UUID, aggregateType: Class<T>): Iterable<Event>

    /**
     * Reverts a specific list of events associated with the provided aggregateId.
     *
     * This operation in most cases will be used as rollback-call in case when some post operation fails and need
     * to be retried by the backend.
     * @param aggregateId the aggregateId to which events are attached
     * @param events a list of events
     */
    fun revertEvents(aggregateId: UUID, events: Iterable<Event>)
}