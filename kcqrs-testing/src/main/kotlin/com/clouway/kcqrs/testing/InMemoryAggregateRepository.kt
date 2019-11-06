package com.clouway.kcqrs.testing

import com.clouway.kcqrs.core.AggregateNotFoundException
import com.clouway.kcqrs.core.AggregateRepository
import com.clouway.kcqrs.core.AggregateRoot

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class InMemoryAggregateRepository : AggregateRepository {
	private val aggregateIdToEvents = mutableMapOf<String, MutableList<AggregatedEvent>>()
	
	override fun <T : AggregateRoot> save(aggregate: T) {
		val changes = aggregate.getUncommittedChanges().toMutableList()
		val events = changes.map { AggregatedEvent(aggregate.getId()!!, it) }.toMutableList()
		if (aggregateIdToEvents.containsKey(aggregate.getId())) {
			aggregateIdToEvents[aggregate.getId()]!!.addAll(events)
		} else {
			aggregateIdToEvents[aggregate.getId()!!] = events
		}
		
		aggregate.markChangesAsCommitted()
	}
	
	override fun <T : AggregateRoot> save(stream: String, aggregate: T) {
		val changes = aggregate.getUncommittedChanges().toMutableList()
		val events = changes.map { AggregatedEvent(aggregate.getId()!!, it) }.toMutableList()
		if (aggregateIdToEvents.containsKey(stream)) {
			aggregateIdToEvents[stream]!!.addAll(events)
		} else {
			aggregateIdToEvents[stream] = events
		}
		
		aggregate.markChangesAsCommitted()
		
	}
	
	override fun <T : AggregateRoot> getById(id: String, type: Class<T>): T {
		if (!aggregateIdToEvents.containsKey(id)) {
			throw AggregateNotFoundException("Cannot find aggregate with ID '$id'")
		}
		val events = aggregateIdToEvents[id]!!.map { it.event }
		
		val instance = type.newInstance() as T
		instance.loadFromHistory(events, events.size.toLong())
		return instance
	}
	
	override fun <T : AggregateRoot> getById(stream: String, aggregateId: String, type: Class<T>): T {
		if (!aggregateIdToEvents.containsKey(stream)) {
			throw AggregateNotFoundException("Cannot find aggregate with ID '$stream'")
		}
		val events = aggregateIdToEvents[stream]!!.filter { it.aggregateId == aggregateId }.map { it.event }
		
		val instance = type.newInstance() as T
		instance.loadFromHistory(events, events.size.toLong())
		return instance
	}
	
	override fun <T : AggregateRoot> getByIds(ids: List<String>, type: Class<T>): Map<String, T> {
		return ids.filter { aggregateIdToEvents.containsKey(it) }.map {
			val instance = type.newInstance() as T
			val events = aggregateIdToEvents[it]!!.map { it.event }
			instance.loadFromHistory(events, events.size.toLong())
			Pair(it, instance)
		}.toMap()
	}
}

data class AggregatedEvent(val aggregateId: String, val event: Any)