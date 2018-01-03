package com.clouway.kcqrs.core

import java.util.*

class EventRepository(private val eventStore: EventStore,
                      private val eventPublisher: EventPublisher) : Repository {

    @Throws(EventCollisionException::class, PublishErrorException::class)
    override fun <T : AggregateRoot> save(aggregate: T) {
        if (aggregate.getId() == null) {
            throw IllegalArgumentException("aggregate id cannot be null when trying to persist it")
        }

        val events = aggregate.getUncommittedChanges()

        eventStore.saveEvents(aggregate.getId()!!, aggregate.getExpectedVersion(), events)

        try {
            eventPublisher.publish(events)
        } catch (ex: PublishErrorException) {
            eventStore.revertEvents(aggregate.getId()!!, events)
            throw ex
        }

        aggregate.markChangesAsCommitted()
    }

    override fun <T : AggregateRoot> getById(id: UUID, type: Class<T>): T {
        /*
         * get the events from the event store
         */
        val history = eventStore.getEvents(id, type)

        /*
         * Create a new instance of the aggregate
         */
        val aggregate: T
        try {
            aggregate = type.newInstance()
        } catch (e: InstantiationException) {
            throw HydrationException(id, "target type: '${type.name}' cannot be instantiated")
        } catch (e: IllegalAccessException) {
            throw HydrationException(id, "target type: '${type.name} has no default constructor")
        }

        aggregate.loadFromHistory(history)

        return aggregate
    }

}