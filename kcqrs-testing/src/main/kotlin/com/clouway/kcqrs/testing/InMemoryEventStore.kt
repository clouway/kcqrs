package com.clouway.kcqrs.testing

import com.clouway.kcqrs.core.Aggregate
import com.clouway.kcqrs.core.EventPayload
import com.clouway.kcqrs.core.EventStore
import com.clouway.kcqrs.core.GetAllEventsRequest
import com.clouway.kcqrs.core.GetAllEventsResponse
import com.clouway.kcqrs.core.GetEventsFromStreamsRequest
import com.clouway.kcqrs.core.GetEventsResponse
import com.clouway.kcqrs.core.IndexedEvent
import com.clouway.kcqrs.core.Position
import com.clouway.kcqrs.core.RevertEventsResponse
import com.clouway.kcqrs.core.SaveEventsRequest
import com.clouway.kcqrs.core.SaveEventsResponse
import com.clouway.kcqrs.core.SaveOptions
import com.clouway.kcqrs.core.Snapshot
import java.util.*

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class InMemoryEventStore(private val eventsLimit: Int) : EventStore {
    private val idToAggregate = mutableMapOf<String, MutableList<StoredAggregate>>()
    private val stubbedResponses = LinkedList<SaveEventsResponse>()
    public val saveEventOptions = LinkedList<SaveOptions>()

    override fun saveEvents(request: SaveEventsRequest, saveOptions: SaveOptions): SaveEventsResponse {
        saveEventOptions.add(saveOptions)

        if (stubbedResponses.size > 0) {
            return stubbedResponses.pop()
        }

        val streamKey = streamKey(request.tenant, request.stream)

        if (!idToAggregate.contains(streamKey)) {
            idToAggregate[streamKey] = mutableListOf(StoredAggregate(request.tenant, request.aggregateType, mutableListOf(), null))
        }

        val aggregates = idToAggregate[streamKey]!!

        val aggregate = aggregates.find {
            it.events.find { event -> event.aggregateId == request.events[0].aggregateId} != null
        }!!

        if (saveOptions.createSnapshot.required) {
            val snapshot = saveOptions.createSnapshot.snapshot
            aggregates.add(StoredAggregate(request.tenant, request.aggregateType, mutableListOf(), snapshot))
        } else if (aggregate.events.size + request.events.size > eventsLimit) {
            return SaveEventsResponse.SnapshotRequired(aggregate.events, aggregate.snapshot, aggregate.events.size.toLong())
        }

        aggregate.events.addAll(request.events)
    
        val version = aggregate.events.size.toLong() + (aggregate.snapshot?.version ?: 0L)
        return SaveEventsResponse.Success(
            version,
            (0..request.events.size).map { it.toLong() },
            Aggregate(aggregate.aggregateType, aggregate.snapshot, version, aggregate.events))
    }

    override fun getEventsFromStreams(request: GetEventsFromStreamsRequest): GetEventsResponse {
        val aggregates = mutableListOf<Aggregate>()
        request.streams.forEach { stream ->
            val streamKey = streamKey(request.tenant, stream)

            idToAggregate[streamKey]?.forEach {

                val aggregateIdToEvents = it.events.groupBy { it.aggregateId }
                aggregateIdToEvents.forEach { aggregateId, events ->

                    aggregates.add(
                            Aggregate(
                                    it.aggregateType,
                                    it.snapshot,
                                    it.events.size.toLong() + (it.snapshot?.version ?: 0L),
                                    it.events
                            )
                    )
                }
            }
        }

        return GetEventsResponse.Success(aggregates)
    }

    override fun getAllEvents(request: GetAllEventsRequest): GetAllEventsResponse {
        var positionId = 1L
        val result = mutableListOf<IndexedEvent>()

        idToAggregate.values.forEach { aggregates ->
            aggregates.forEach { aggregate ->
                aggregate.events.forEach { event ->
                    result.add(IndexedEvent(Position(positionId), aggregate.tenant, aggregate.aggregateType, positionId, event))
                    positionId++
                }
            }
        }
        return GetAllEventsResponse.Success(result, request.readDirection, Position(positionId))
    }

    override fun revertLastEvents(tenant: String, stream: String, count: Int): RevertEventsResponse {
        val streamKey = streamKey(tenant, stream)

        if (!idToAggregate.containsKey(streamKey)) {
            return RevertEventsResponse.AggregateNotFound(tenant, stream)
        }

        val aggregates = idToAggregate[streamKey]!!
        val aggregate = aggregates[0]

        val lastEventIndex = aggregate.events.size - count

        val updatedEvents = aggregate.events.filterIndexed { index, _ -> index < lastEventIndex }.toMutableList()

        idToAggregate[streamKey] = mutableListOf(StoredAggregate(aggregate.tenant, aggregate.aggregateType, updatedEvents, aggregate.snapshot))

        return RevertEventsResponse.Success(listOf())
    }

    fun pretendThatNextSaveWillReturn(response: SaveEventsResponse) {
        stubbedResponses.add(response)
    }

    private fun streamKey(tenant: String, stream: String) = "$tenant:$stream"

}

private data class StoredAggregate(val tenant: String, val aggregateType: String, val events: MutableList<EventPayload>, val snapshot: Snapshot?)