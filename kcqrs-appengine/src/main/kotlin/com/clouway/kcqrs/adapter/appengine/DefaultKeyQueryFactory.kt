package com.clouway.kcqrs.adapter.appengine

import com.google.cloud.datastore.Datastore
import com.google.cloud.datastore.EntityQuery
import com.google.cloud.datastore.Key
import com.google.cloud.datastore.PathElement
import com.google.cloud.datastore.Query
import com.google.cloud.datastore.StructuredQuery

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class DefaultKeyQueryFactory(
        private val datastore: Datastore,
        private val kind: String
) : KeyQueryFactory {
    
    /**
     * Event key factory used to create the event keys.
     */
    private val eventKeyFactory = datastore.newKeyFactory().setKind(kind)

    /**
     * Event key factory used to create the event keys.
     */
    private val snapshotEventFactory = datastore.newKeyFactory().setKind(kind + "Snapshot")

    /**
     * Entity Kind used for storing of event index: [aggregateId, aggregateType, sequence number, version]
     */
    private val indexKind = kind + "Index"

    /**
     * Creates a new aggregate key.
     */
    override fun createAggregateKey(aggregateType: String, aggregateId: String?, aggregateIndex: Long): Key {
        if (aggregateId.isNullOrEmpty()) {
            return eventKeyFactory.newKey("${aggregateType}_$aggregateIndex")
        }
        return eventKeyFactory.newKey("${aggregateType}_${aggregateId}_$aggregateIndex")
    }

    /**
     * Creates a new snapshot key.
     */
    override fun createSnapshotKey(aggregateType: String, aggregateId: String): Key {
        return snapshotEventFactory.newKey("${aggregateType}_$aggregateId")
    }

    /**
     * Creates an index key that will be used for storing of Index entities in the datastore.
     */
    override fun createIndexKey(aggregateKey: Key, event: EventModel): Key {
        return datastore.newKeyFactory()
                .addAncestor(PathElement.of(aggregateKey.kind, aggregateKey.name))
                .setKind(indexKind)
                .newKey(event.version)
    }

    /**
     * Creates a new index lookup query using the provided filter and max number of elements.
     * @return the newly created EntityQuery. 
     */
    override fun createIndexLookupQuery(filter: StructuredQuery.Filter, maxCount: Int): EntityQuery {
        return Query.newEntityQueryBuilder()
                .setKind(indexKind)
                .setFilter(filter)
                .setLimit(maxCount)
                .build()
    }

}