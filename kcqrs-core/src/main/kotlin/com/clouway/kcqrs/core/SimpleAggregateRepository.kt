package com.clouway.kcqrs.core

import com.clouway.kcqrs.core.messages.MessageFormat
import java.io.ByteArrayInputStream

class SimpleAggregateRepository(private val eventStore: EventStore,
                                private val messageFormat: MessageFormat,
                                private val eventPublisher: EventPublisher,
                                private val configuration: Configuration
) : AuditAwareAggregateRepository {
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

        if (events.isEmpty()) return

        val aggregateClass = aggregate::class.java
        val response = eventStore.saveEvents(
                aggregateClass.simpleName,
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
            is SaveEventsResponse.EventCollision -> {
                throw EventCollisionException(aggregate.getId()!!, response.expectedVersion)
            }
        //The chosen persistence has reached it's limit for events so a snapshot needs to be created.
            is SaveEventsResponse.SnapshotRequired -> {
                val currentAggregate = buildAggregateFromHistory(aggregateClass, response.currentEvents, aggregate.getId()!!, response.currentSnapshot)

                val newSnapshot = currentAggregate.getSnapshotMapper().toSnapshot(currentAggregate)
                val createSnapshotResponse = eventStore.saveEvents(
                        aggregateClass.simpleName,
                        events,
                        SaveOptions(aggregate.getId()!!, 0, configuration.topicName(aggregate), CreateSnapshot(true, newSnapshot)))

                when (createSnapshotResponse) {
                    is SaveEventsResponse.Success -> {
                        try {
                            eventPublisher.publish(eventsWithPayload)
                            aggregate.markChangesAsCommitted()
                        } catch (ex: PublishErrorException) {
                            eventStore.revertLastEvents(aggregate.getId()!!, events.size)
                            throw ex
                        }
                    }
                    is SaveEventsResponse.EventCollision -> {
                        throw EventCollisionException(aggregate.getId()!!, createSnapshotResponse.expectedVersion)
                    }
                    else -> throw IllegalStateException("unable to save events")
                }
            }
            else -> throw IllegalStateException("unable to save events")
        }
    }

    override fun <T : AggregateRoot> getByIds(ids: List<String>, type: Class<T>): Map<String, T> {
        /*
         * Get the events from the event store
        */
        val response = eventStore.getEvents(ids, type.simpleName)
        when (response) {
            is GetEventsResponse.Success -> {
                val adapter = AggregateAdapter<T>("apply")
                adapter.fetchMetadata(type)
                val result = mutableMapOf<String, T>()
                response.aggregates.forEach { result[it.aggregateId] = buildAggregateFromHistory(type, it.events, it.aggregateId, it.snapshot) }
                return result
            }
            is GetEventsResponse.Error -> {
                throw IllegalStateException(response.message)
            }
            else -> throw IllegalStateException("unknown state")
        }
    }

    override fun <T : AggregateRoot> getById(id: String, type: Class<T>): T {
        /*
         * Get the events from the event store
         */
        val response = eventStore.getEvents(id, type.simpleName)
        when (response) {
            is GetEventsResponse.Success -> {
                //we are sure that only one aggregate will be returned
                return buildAggregateFromHistory(type, response.aggregates[0].events, id, response.aggregates.first().snapshot)

            }
            is GetEventsResponse.AggregateNotFound -> {
                throw AggregateNotFoundException(id)
            }
            is GetEventsResponse.Error -> {
                throw IllegalStateException(response.message)
            }

            else -> throw IllegalStateException("unknown state")
        }
    }

    private fun <T : AggregateRoot> buildAggregateFromHistory(type: Class<T>, events: List<EventPayload>, id: String, snapshot: Snapshot? = null): T {
        val adapter = AggregateAdapter<T>("apply")
        adapter.fetchMetadata(type)
        val history = mutableListOf<Event>()

        events.forEach {
            val eventType = Class.forName(adapter.eventType(it.kind))
            val event = messageFormat.parse<Event>(ByteArrayInputStream(it.data.payload), eventType)
            history.add(event)
        }

        /*
         * Create a new instance of the aggregate
         */
        var aggregate: T
        try {
            aggregate = type.newInstance()
            if (snapshot != null) {
                aggregate = aggregate.fromSnapshot(String(snapshot.data.payload), snapshot.version) as T
            }
        } catch (e: InstantiationException) {
            throw HydrationException(id, "target type: '${type.name}' cannot be instantiated")
        } catch (e: IllegalAccessException) {
            throw HydrationException(id, "target type: '${type.name}' has no default constructor")
        }
        aggregate.loadFromHistory(history)
        return aggregate
    }
}
