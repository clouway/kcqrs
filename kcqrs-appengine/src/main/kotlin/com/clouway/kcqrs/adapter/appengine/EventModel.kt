package com.clouway.kcqrs.adapter.appengine

/**
 * EventModel is an internal data class which is representing the persistent state of single event in the EventStore.
 *
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
data class EventModel(
        @JvmField val kind: String,
        @JvmField val version: Long,
        @JvmField val identityId: String,
        @JvmField val timestamp: Long,
        @JvmField val payload: String
)