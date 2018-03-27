package com.clouway.kcqrs.testing

import com.clouway.kcqrs.core.*

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class InMemoryEventStore : EventStore {
    private val idToAggregate = mutableMapOf<String, StoredAggregate>()

    override fun saveEvents(aggregateType: String, events: List<EventPayload>, saveOptions: SaveOptions): SaveEventsResponse {
        val aggregateId = saveOptions.aggregateId

        if (!idToAggregate.contains(aggregateId)) {
            idToAggregate[aggregateId] = StoredAggregate(aggregateId, aggregateType, mutableListOf())
        }

        val aggregate = idToAggregate[aggregateId]!!
        aggregate.events.addAll(events)

        return SaveEventsResponse.Success(aggregateId, aggregate.events.size.toLong())
    }

    override fun getEvents(aggregateId: String): GetEventsResponse {
        if (!idToAggregate.containsKey(aggregateId)) {
            return GetEventsResponse.AggregateNotFound
        }
        val aggregate = idToAggregate[aggregateId]!!

        return GetEventsResponse.Success(aggregateId, aggregate.aggregateType, null, aggregate.events.size.toLong(), aggregate.events)
    }

    override fun revertLastEvents(aggregateId: String, count: Int): RevertEventsResponse {
        val aggregate = idToAggregate[aggregateId]!!
        val lastEventIndex = aggregate.events.size - count

        val updatedEvents = aggregate.events.filterIndexed { index, _ -> index < lastEventIndex }.toMutableList()

        idToAggregate[aggregateId] = StoredAggregate(aggregate.aggregateId, aggregate.aggregateType, updatedEvents)

        return RevertEventsResponse.Success
    }

}

private data class StoredAggregate(val aggregateId: String, val aggregateType: String, val events: MutableList<EventPayload>)