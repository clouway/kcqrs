package com.clouway.kcqrs.testing

import com.clouway.kcqrs.core.Event
import com.clouway.kcqrs.core.EventStore
import java.util.*

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class InMemoryEventStore : EventStore {
    private val idToEvents = mutableMapOf<UUID, MutableList<Event>>()

    override fun saveEvents(aggregateId: UUID, expectedVersion: Int, events: Iterable<Event>) {
        if (!idToEvents.contains(aggregateId)) {
            idToEvents[aggregateId] = mutableListOf()
        }
        idToEvents[aggregateId]!!.addAll(events)
    }

    override fun <T> getEvents(aggregateId: UUID, aggregateType: Class<T>): Iterable<Event> {
        if (!idToEvents.containsKey(aggregateId)) {
            return listOf()
        }
        return idToEvents[aggregateId]!!
    }

    override fun revertEvents(aggregateId: UUID, events: Iterable<Event>) {
        val newEvents = idToEvents[aggregateId]!!.filter { !events.contains(it) }.toMutableList()
        idToEvents[aggregateId] = newEvents
    }

}