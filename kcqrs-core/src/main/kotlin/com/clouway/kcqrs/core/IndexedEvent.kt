package com.clouway.kcqrs.core

/**
 * IndexedEvent is an Event represented with it's indexing structure in the EventStore.
 *
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
data class IndexedEvent(val position: Position, val aggregateId: String, val aggregateType: String, val version: Long, val payload: EventPayload)