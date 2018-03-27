package com.clouway.kcqrs.core

import com.clouway.kcqrs.core.messages.MessageFormat
import java.io.ByteArrayInputStream

class SimpleAggregateRepository(private val eventStore: EventStore,
                                private val messageFormat: MessageFormat,
                                private val eventPublisher: EventPublisher,
                                private val configuration: Configuration) : AuditAwareAggregateRepository {

    @Throws(EventCollisionException::class, PublishErrorException::class)
    override fun <T : AggregateRoot> save(aggregate: T, identity: Identity) {
        if (aggregate.getId() == null) {
            throw IllegalArgumentException("aggregate id cannot be null when trying to persist it")
        }

        val uncommittedEvents = aggregate.getUncommittedChanges()

        val eventsWithPayload = uncommittedEvents.map { EventWithPayload(it, messageFormat.format(it)) }

        val events = eventsWithPayload.map {
            EventPayload(it.event::class.java.simpleName, identity.time.toEpochMilli(), identity.id, Binary(it.payload))
        }

        val response = eventStore.saveEvents(
                aggregate::class.java.simpleName,
                events,
                SaveOptions(aggregate.getId()!!, aggregate.getExpectedVersion(), configuration.topicName(aggregate))
        )

        when (response) {
            is SaveEventsResponse.Success -> {
                try {
                    eventPublisher.publish(eventsWithPayload)

                    aggregate.markChangesAsCommitted()
                } catch (ex: PublishErrorException) {
                    eventStore.revertLastEvents(aggregate.getId()!!, events.size)
                    throw ex
                }
            }
            else -> throw IllegalStateException("unable to save events")
        }
    }


    override fun <T : AggregateRoot> getById(id: String, type: Class<T>): T {
        /*
         * Get the events from the event store
         */
        val response = eventStore.getEvents(id)
        when (response) {
            is GetEventsResponse.Success -> {

                val adapter = AggregateAdapter<T>("apply")
                adapter.fetchMetadata(type)

                val history = mutableListOf<Event>()
                response.events.forEach {
                    val eventType = Class.forName(adapter.eventType(it.kind))

                    val event = messageFormat.parse<Event>(ByteArrayInputStream(it.data.payload), eventType)
                    history.add(event)
                }

                /*
                 * Create a new instance of the aggregate
                 */
                val aggregate: T
                try {
                    aggregate = type.newInstance()
                } catch (e: InstantiationException) {
                    throw HydrationException(id, "target type: '${type.name}' cannot be instantiated")
                } catch (e: IllegalAccessException) {
                    throw HydrationException(id, "target type: '${type.name}' has no default constructor")
                }

                aggregate.loadFromHistory(history)

                return aggregate

            }
            else -> throw IllegalStateException("unknown state")
        }
    }
}