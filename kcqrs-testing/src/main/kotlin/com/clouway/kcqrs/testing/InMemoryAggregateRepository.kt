package com.clouway.kcqrs.testing

import com.clouway.kcqrs.core.AggregateNotFoundException
import com.clouway.kcqrs.core.AggregateRepository
import com.clouway.kcqrs.core.AggregateRoot
import com.clouway.kcqrs.core.Event

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class InMemoryAggregateRepository : AggregateRepository {
    private val aggregateIdToEvents = mutableMapOf<String, MutableList<Event>>()

    override fun <T : AggregateRoot> save(aggregate: T) {
        val changes = aggregate.getUncommittedChanges().toMutableList()

        if (aggregateIdToEvents.containsKey(aggregate.getId())) {
            aggregateIdToEvents[aggregate.getId()]!!.addAll(changes)
        } else {
            aggregateIdToEvents[aggregate.getId()!!] = changes
        }

        aggregate.markChangesAsCommitted()
    }

    override fun <T : AggregateRoot> getById(id: String, type: Class<T>): T {
        if (!aggregateIdToEvents.containsKey(id)) {
            throw AggregateNotFoundException("Cannot find aggregate with ID '$id'")
        }
        val events = aggregateIdToEvents[id]

        val instance = type.newInstance() as T
        instance.loadFromHistory(events!!)
        return instance
    }

    override fun <T : AggregateRoot> getByIds(ids: List<String>, type: Class<T>): Map<String, T> {
        return ids.filter { aggregateIdToEvents.containsKey(it) }.map {
            val instance = type.newInstance() as T
            instance.loadFromHistory(aggregateIdToEvents[it]!!)
            Pair(it, instance)
        }.toMap()
    }
}