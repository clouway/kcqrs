package com.clouway.kcqrs.testing

import com.clouway.kcqrs.core.Aggregate
import com.clouway.kcqrs.core.EventPayload
import com.clouway.kcqrs.core.EventStore
import com.clouway.kcqrs.core.GetEventsResponse
import com.clouway.kcqrs.core.RevertEventsResponse
import com.clouway.kcqrs.core.SaveEventsResponse
import com.clouway.kcqrs.core.SaveOptions
import com.clouway.kcqrs.core.Snapshot
import java.util.LinkedList

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class InMemoryEventStore(private val eventsLimit: Int) : EventStore {

    private val idToAggregate = mutableMapOf<String, StoredAggregate>()
    private val stubbedResponses = LinkedList<SaveEventsResponse>()

    override fun saveEvents(aggregateType: String, events: List<EventPayload>, saveOptions: SaveOptions): SaveEventsResponse {
        val aggregateId = saveOptions.aggregateId

        if (stubbedResponses.size > 0) {
            return stubbedResponses.pop()
        }

        if (!idToAggregate.contains(aggregateId)) {
            idToAggregate[aggregateId] = StoredAggregate(aggregateId, aggregateType, mutableListOf(), null)
        }

        var aggregate = idToAggregate[aggregateId]!!

        if (saveOptions.createSnapshot.required) {
            val snapshot = saveOptions.createSnapshot.snapshot
            aggregate = StoredAggregate(aggregate.aggregateId, aggregate.aggregateType, mutableListOf(), snapshot)
        }

        if (aggregate.events.size + events.size > eventsLimit) {
            return SaveEventsResponse.SnapshotRequired(aggregate.events, aggregate.snapshot)
        }

        aggregate.events.addAll(events)
        idToAggregate[aggregateId] = aggregate

        return SaveEventsResponse.Success(aggregateId, aggregate.events.size.toLong())
    }

    override fun getEvents(aggregateId: String, aggregateType: String): GetEventsResponse {
        if (!idToAggregate.containsKey(aggregateId)) {
            return GetEventsResponse.AggregateNotFound
        }

        val aggregate = idToAggregate[aggregateId]!!

        if (aggregateType != aggregate.aggregateType) {
            return GetEventsResponse.Error("")
        }

        return GetEventsResponse.Success(listOf(Aggregate(
                aggregateId,
                aggregate.aggregateType,
                aggregate.snapshot,
                aggregate.events.size.toLong(),
                aggregate.events)
        ))
    }

    override fun getEvents(aggregateIds: List<String>, aggregateType: String): GetEventsResponse {
        val aggregates = aggregateIds.filter { idToAggregate.containsKey(it) }.map {
            val aggregate = idToAggregate[it]!!
            if (aggregateType != aggregate.aggregateType) {
                return GetEventsResponse.Error("")
            }
            Aggregate(
                    it,
                    aggregate.aggregateType,
                    aggregate.snapshot,
                    aggregate.events.size.toLong(),
                    aggregate.events
            )
        }
        return GetEventsResponse.Success(aggregates)
    }

    override fun revertLastEvents(aggregateId: String, count: Int): RevertEventsResponse {
        val aggregate = idToAggregate[aggregateId]!!
        val lastEventIndex = aggregate.events.size - count

        val updatedEvents = aggregate.events.filterIndexed { index, _ -> index < lastEventIndex }.toMutableList()

        idToAggregate[aggregateId] = StoredAggregate(aggregate.aggregateId, aggregate.aggregateType, updatedEvents, aggregate.snapshot)

        return RevertEventsResponse.Success
    }

    fun pretendThatNextSaveWillReturn(response: SaveEventsResponse) {
        stubbedResponses.add(response)
    }

}

private data class StoredAggregate(val aggregateId: String, val aggregateType: String, val events: MutableList<EventPayload>, val snapshot: Snapshot?)