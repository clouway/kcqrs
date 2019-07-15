package com.clouway.kcqrs.testing

import com.clouway.kcqrs.core.*
import java.util.LinkedList

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class InMemoryEventStore(private val eventsLimit: Int) : EventStore {

    private val idToAggregate = mutableMapOf<String, StoredAggregate>()
    private val stubbedResponses = LinkedList<SaveEventsResponse>()
    public val saveEventOptions = LinkedList<SaveOptions>()

    override fun saveEvents(aggregateType: String, events: List<EventPayload>, saveOptions: SaveOptions): SaveEventsResponse {
        val aggregateKey = aggregateKey(aggregateType, saveOptions.aggregateId)

        saveEventOptions.add(saveOptions)

        if (stubbedResponses.size > 0) {
            return stubbedResponses.pop()
        }

        if (!idToAggregate.contains(aggregateKey)) {
            idToAggregate[aggregateKey] = StoredAggregate(saveOptions.aggregateId, aggregateType, mutableListOf(), null)
        }

        var aggregate = idToAggregate[aggregateKey]!!

        if (saveOptions.createSnapshot.required) {
            val snapshot = saveOptions.createSnapshot.snapshot
            aggregate = StoredAggregate(aggregate.aggregateId, aggregate.aggregateType, mutableListOf(), snapshot)
        }

        if (aggregate.events.size + events.size > eventsLimit) {
            return SaveEventsResponse.SnapshotRequired(aggregate.events, aggregate.snapshot)
        }

        aggregate.events.addAll(events)
        idToAggregate[aggregateKey] = aggregate

        return SaveEventsResponse.Success(saveOptions.aggregateId, aggregate.events.size.toLong() + (aggregate.snapshot?.version ?: 0L),(0..events.size).map { it.toLong() } )
    }

    override fun getEvents(aggregateId: String, aggregateType: String): GetEventsResponse {
        val key = aggregateKey(aggregateType, aggregateId)
        if (!idToAggregate.containsKey(key)) {
            return GetEventsResponse.AggregateNotFound(listOf(aggregateId), aggregateType)
        }

        val aggregate = idToAggregate[key]!!

        return GetEventsResponse.Success(listOf(Aggregate(
                aggregateId,
                aggregate.aggregateType,
                aggregate.snapshot,
                aggregate.events.size.toLong() + (aggregate.snapshot?.version ?: 0L),
                aggregate.events)
        ))
    }

    override fun getEvents(aggregateIds: List<String>, aggregateType: String): GetEventsResponse {
        val aggregates = aggregateIds.filter { idToAggregate.containsKey(aggregateKey(aggregateType, it)) }.map {
            val aggregate = idToAggregate[aggregateKey(aggregateType, it)]!!
            Aggregate(
                    it,
                    aggregate.aggregateType,
                    aggregate.snapshot,
                    aggregate.events.size.toLong() + (aggregate.snapshot?.version ?: 0L),
                    aggregate.events
            )
        }
        return GetEventsResponse.Success(aggregates)
    }

    override fun getAllEvents(request: GetAllEventsRequest): GetAllEventsResponse {
        var positionId = 1L
        val result = mutableListOf<IndexedEvent>()

        idToAggregate.values.forEach { aggregate ->
            aggregate.events.forEach { event ->
                result.add(IndexedEvent(Position(positionId), aggregate.aggregateId, aggregate.aggregateType, positionId, event))
                positionId++
            }
        }
        return GetAllEventsResponse.Success(result, request.readDirection, Position(positionId))
    }

    override fun revertLastEvents(aggregateType: String, aggregateId: String, count: Int): RevertEventsResponse {
        val aggregate = idToAggregate[aggregateKey(aggregateType, aggregateId)]!!
        val lastEventIndex = aggregate.events.size - count

        val updatedEvents = aggregate.events.filterIndexed { index, _ -> index < lastEventIndex }.toMutableList()

        idToAggregate[aggregateKey(aggregateType, aggregateId)] = StoredAggregate(aggregate.aggregateId, aggregate.aggregateType, updatedEvents, aggregate.snapshot)

        return RevertEventsResponse.Success(listOf())
    }

    fun pretendThatNextSaveWillReturn(response: SaveEventsResponse) {
        stubbedResponses.add(response)
    }

    private fun aggregateKey(aggregateType: String, aggregateId: String) = "${aggregateType}_$aggregateId"

}

private data class StoredAggregate(val aggregateId: String, val aggregateType: String, val events: MutableList<EventPayload>, val snapshot: Snapshot?)