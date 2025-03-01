package com.clouway.kcqrs.core

import com.clouway.kcqrs.core.messages.MessageFormat
import java.io.ByteArrayInputStream
import java.util.UUID

class SimpleAggregateRepository(
    private val eventStore: EventStore,
    private val messageFormat: MessageFormat,
    private val eventPublisher: EventPublisher,
    private val configuration: Configuration,
) : AuditAwareAggregateRepository {
    override fun <T : AggregateRoot> save(
        stream: String,
        aggregate: T,
        identity: Identity,
    ) {
        if (aggregate.getId() == null) {
            throw IllegalArgumentException("aggregate id cannot be null when trying to persist it")
        }

        val uncommittedEvents = aggregate.getUncommittedChanges()

        val eventsWithPayload = uncommittedEvents.map { EventWithPayload(it, Binary(messageFormat.formatToBytes(it))) }

        var aggregateId = aggregate.getId()!!
        if (aggregateId == "") {
            aggregateId = UUID.randomUUID().toString()
        }

        val events =
            eventsWithPayload.map {
                EventPayload(aggregateId, it.event::class.java.simpleName, identity.time.toEpochMilli(), identity.id, it.payload)
            }

        if (events.isEmpty()) return

        val aggregateClass = aggregate::class.java
        val aggregateType = aggregateClass.simpleName

        val topicName = configuration.topicName(aggregate)

        val saveEventsRequest = SaveEventsRequest(identity.tenant, stream, aggregateType, events)

        val response =
            eventStore.saveEvents(
                saveEventsRequest,
                SaveOptions(version = aggregate.getExpectedVersion(), topicName = topicName),
            )

        when (response) {
            is SaveEventsResponse.Success -> {
                try {
                    eventPublisher.publish(eventsWithPayload)
                    aggregate.markChangesAsCommitted()
                } catch (ex: PublishErrorException) {
                    eventStore.revertLastEvents(identity.tenant, stream, events.size)
                    throw ex
                }
            }
            is SaveEventsResponse.EventCollision -> {
                throw EventCollisionException(aggregate.getId()!!, response.expectedVersion)
            }
            // The chosen persistence has reached it's limit for events so a snapshot needs to be created.
            is SaveEventsResponse.SnapshotRequired -> {
                val currentAggregate = getById(aggregateId, aggregateClass, identity)

                val newSnapshot = currentAggregate.getSnapshotMapper().toSnapshot(currentAggregate, messageFormat)
                val createSnapshotResponse =
                    eventStore.saveEvents(
                        saveEventsRequest,
                        SaveOptions(
                            version = currentAggregate.getExpectedVersion(),
                            createSnapshot = CreateSnapshot(true, newSnapshot.copy(version = currentAggregate.getExpectedVersion())),
                            topicName = topicName,
                        ),
                    )

                when (createSnapshotResponse) {
                    is SaveEventsResponse.Success -> {
                        try {
                            eventPublisher.publish(eventsWithPayload)
                            aggregate.markChangesAsCommitted()
                        } catch (ex: PublishErrorException) {
                            eventStore.revertLastEvents(aggregateType, aggregate.getId()!!, events.size)
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

    /**
     * Creates a new or updates an existing aggregate in the repository.
     *
     * Calling of this method will ensure a one to one relationship of stream and aggregate and will use
     * AggregateType_AggregaetId pattern for the name of the stream where data will be written.
     *
     * @param aggregate the aggregate to be registered
     * @throws EventCollisionException is thrown in case of
     */
    @Throws(EventCollisionException::class, PublishErrorException::class)
    override fun <T : AggregateRoot> save(
        aggregate: T,
        identity: Identity,
    ) {
        val aggregateClass = aggregate::class.java
        val aggregateType = aggregateClass.simpleName
        var aggregateId = aggregate.getId()!!

        if (aggregateId == "") {
            aggregateId = UUID.randomUUID().toString()
        }

        return this.save(StreamKey.of(aggregateType, aggregateId), aggregate, identity)
    }

    override fun <T : AggregateRoot> getByIds(
        ids: List<String>,
        type: Class<T>,
        identity: Identity,
    ): Map<String, T> {
        val tenant = identity.tenant
        val aggregateType = type.simpleName
        if (ids.isEmpty()) {
            return mapOf()
        }

        val streamIds = ids.map { StreamKey.of(aggregateType, it) }

        /*
         * Get the events from the event store
         */
        when (val response = eventStore.getEventsFromStreams(GetEventsFromStreamsRequest(tenant, streamIds))) {
            is GetEventsResponse.Success -> {
                val result = mutableMapOf<String, T>()
                response.aggregates.forEach {
                    val aggregateId = it.events[0].aggregateId
                    result[aggregateId] = buildAggregateFromHistory(type, it.events, it.version, aggregateId, it.snapshot)
                }
                return result
            }
            is GetEventsResponse.AggregateNotFound -> {
                return mapOf()
            }
            else -> throw IllegalStateException("unknown response: $response")
        }
    }

    override fun <T : AggregateRoot> getById(
        stream: String,
        aggregateId: String,
        type: Class<T>,
        identity: Identity,
    ): T {
        val tenant = identity.tenant

        /*
         * Get the events from the event store
         */
        when (val response = eventStore.getEventsFromStreams(GetEventsFromStreamsRequest(tenant, listOf(stream)))) {
            is GetEventsResponse.Success -> {
                if (response.aggregates.isEmpty()) {
                    throw AggregateNotFoundException(aggregateId)
                }

                val aggregate =
                    response.aggregates.find { it.events[0].aggregateId == aggregateId } ?: throw AggregateNotFoundException(
                        aggregateId,
                    )

                // we are sure that only one aggregate will be returned
                return buildAggregateFromHistory(
                    type,
                    aggregate.events,
                    aggregate.version,
                    aggregateId,
                    response.aggregates.first().snapshot,
                )
            }
            is GetEventsResponse.AggregateNotFound -> throw AggregateNotFoundException(aggregateId)
            else -> throw IllegalStateException("unknown response: $response")
        }
    }

    override fun <T : AggregateRoot> getById(
        id: String,
        type: Class<T>,
        identity: Identity,
    ): T {
        val tenant = identity.tenant

        // A stream is related to a single aggregate and for this reason the existing behaviour is kept
        // to provide backward compatibility.
        val stream = StreamKey.of(type.simpleName, id)

        /*
         * Get the events from the event store
         */
        when (val response = eventStore.getEventsFromStreams(GetEventsFromStreamsRequest(tenant, listOf(stream)))) {
            is GetEventsResponse.Success -> {
                if (response.aggregates.isEmpty()) {
                    throw AggregateNotFoundException(id)
                }
                // we are sure that only one aggregate will be returned
                val aggregate = response.aggregates[0]
                return buildAggregateFromHistory(type, aggregate.events, aggregate.version, id, response.aggregates.first().snapshot)
            }
            // Supported for the legacy eventstore.
            is GetEventsResponse.AggregateNotFound -> {
                throw AggregateNotFoundException(id)
            }
            else -> throw IllegalStateException("unknown state")
        }
    }

    private fun <T : AggregateRoot> buildAggregateFromHistory(
        type: Class<T>,
        events: List<EventPayload>,
        version: Long,
        id: String,
        snapshot: Snapshot? = null,
    ): T {
        val adapter = AggregateAdapter<T>("apply")
        adapter.fetchMetadata(type)
        val history = mutableListOf<Any>()
        events.forEach {
            if (messageFormat.isSupporting(it.kind, adapter)) {
                val event = messageFormat.parse<Any>(ByteArrayInputStream(it.data.payload), it.kind, adapter)
                history.add(event)
            }
        }

        /*
         * Create a new instance of the aggregate
         */
        var aggregate: T
        try {
            aggregate = type.newInstance()
            if (snapshot != null) {
                aggregate = aggregate.fromSnapshot(snapshot.data.payload, snapshot.version, messageFormat) as T
            }
        } catch (e: InstantiationException) {
            throw HydrationException(id, "target type: '${type.name}' cannot be instantiated")
        } catch (e: IllegalAccessException) {
            throw HydrationException(id, "target type: '${type.name}' has no default constructor")
        }
        aggregate.loadFromHistory(history, version)
        return aggregate
    }
}
