package com.clouway.kcqrs.adapter.appengine

import com.google.cloud.datastore.EntityQuery
import com.google.cloud.datastore.Key
import com.google.cloud.datastore.StructuredQuery

/**
 * KeyQueryFactory is a generic interface used to create keys and queries for the Aggregate Events stored in the Datastore.
 *
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
interface KeyQueryFactory {
    
    /**
     * Creates a new snapshot key.
     */
    fun createSnapshotKey(aggregateType: String, aggregateId: String): Key

    /**
     * Creates a new aggregate key.
     */
    fun createAggregateKey(aggregateType: String, aggregateId: String?, aggregateIndex: Long): Key

    /**
     * Creates the Index key.
     */
    fun createIndexKey(aggregateKey: Key, event: EventModel): Key

    /**
     * Creates an IndexLookupQuery that
     */
    fun createIndexLookupQuery(filter: StructuredQuery.Filter, maxCount: Int): EntityQuery
}