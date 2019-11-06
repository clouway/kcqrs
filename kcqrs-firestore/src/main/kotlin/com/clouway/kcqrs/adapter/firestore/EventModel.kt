package com.clouway.kcqrs.adapter.firestore

/**
 * EventModel is an internal data class which is representing the persistent state of single event in the EventStore.
 * 
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
internal data class EventModel(
        @JvmField val aggregateId: String,
        @JvmField val kind: String,
        @JvmField val version: Long,
        @JvmField val identityId: String,
        @JvmField val timestamp: Long,
        @JvmField val payload: String
)