package com.clouway.kcqrs.adapter.appengine

import com.clouway.kcqrs.core.Aggregate
import com.clouway.kcqrs.core.Binary
import com.clouway.kcqrs.core.EventPayload
import com.clouway.kcqrs.core.EventStore
import com.clouway.kcqrs.core.GetAllEventsRequest
import com.clouway.kcqrs.core.GetAllEventsResponse
import com.clouway.kcqrs.core.GetEventsResponse
import com.clouway.kcqrs.core.IdGenerator
import com.clouway.kcqrs.core.IndexedEvent
import com.clouway.kcqrs.core.Position
import com.clouway.kcqrs.core.ReadDirection
import com.clouway.kcqrs.core.RevertEventsResponse
import com.clouway.kcqrs.core.SaveEventsResponse
import com.clouway.kcqrs.core.SaveOptions
import com.clouway.kcqrs.core.Snapshot
import com.clouway.kcqrs.core.messages.MessageFormat
import com.google.cloud.datastore.Blob
import com.google.cloud.datastore.Datastore
import com.google.cloud.datastore.Entity
import com.google.cloud.datastore.Key
import com.google.cloud.datastore.ListValue
import com.google.cloud.datastore.LongValue
import com.google.cloud.datastore.StringValue
import com.google.cloud.datastore.StructuredQuery
import com.google.cloud.datastore.Transaction
import com.google.cloud.datastore.Value
import java.io.ByteArrayInputStream

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class AppEngineEventStore(
        private val keyQueryFactory: KeyQueryFactory,
        private val datastore: Datastore,
        private val messageFormat: MessageFormat,
        private val idGenerator: IdGenerator
) : EventStore {

    /**
     * Property name for the aggregate id.
     */
    private val aggregateIdProperty = "i"

    /**
     * Property name for the aggregate type.
     */
    private val aggregateTypeProperty = "a"

    /**
     * Property name for the aggregate index.
     */
    private val aggregateIndexProperty = "ai"

    /**
     * Property name for the list of event data in the entity.
     */
    private val eventsProperty = "e"

    /**
     * Property name for the sequence property.
     */
    private val sequenceProperty = "s"

    /**
     * Property name of the version which is used for concurrency control.
     */
    private val versionProperty = "v"

    /**
     * Indexed Event Transform
     */
    private val indexedEventTransform: (Entity) -> IndexedEventKey = {
        val aggregateType = it.getString(aggregateTypeProperty)
        val aggregateId = it.getString(aggregateIdProperty)
        val aggregateIndex = it.getLong(aggregateIndexProperty)
        val version = it.getLong(versionProperty)
        val sequenceId = it.getLong(sequenceProperty)

        IndexedEventKey(sequenceId, aggregateId, aggregateType, aggregateIndex, version)
    }

    override fun saveEvents(aggregateType: String, events: List<EventPayload>, saveOptions: SaveOptions): SaveEventsResponse {
        val aggregateId = saveOptions.aggregateId
        var transaction: Transaction? = null

        try {
            transaction = datastore.newTransaction()

            val snapshotKey = keyQueryFactory.createSnapshotKey(aggregateType, aggregateId)

            var snapshotEntity = transaction.get(snapshotKey)

            var aggregateIndex = snapshotEntity?.getLong("aggregateIndex") ?: 0

            if (saveOptions.createSnapshot.required && saveOptions.createSnapshot.snapshot != null) {
                aggregateIndex += 1
                val snapshotData = saveOptions.createSnapshot.snapshot!!.data

                snapshotEntity = Entity.newBuilder(keyQueryFactory.createSnapshotKey(aggregateType, aggregateId))
                        .set("version", saveOptions.createSnapshot.snapshot!!.version)
                        .set("data", Blob.copyFrom(snapshotData.payload))
                        .set(aggregateIdProperty, StringValue.newBuilder(aggregateId).setExcludeFromIndexes(true).build())
                        .set("aggregateIndex", LongValue
                                .newBuilder(aggregateIndex)
                                .setExcludeFromIndexes(true)
                                .build()
                        )
                        .build()
            }

            /*
             * Keys of aggregates are dispatched on different levels by using an index for scalling purposes.
             * Each aggregate in the datastore has single or multiple levels.
             */
            val aggregateKey = keyQueryFactory.createAggregateKey(aggregateType, aggregateId, aggregateIndex)

            val aggregateEntity =
                    transaction.get(aggregateKey) ?: Entity.newBuilder(aggregateKey)
                            .set(eventsProperty, ListValue.newBuilder().setExcludeFromIndexes(true).build())
                            .set(versionProperty, LongValue
                                    .newBuilder(saveOptions.createSnapshot.snapshot?.version ?: 0L)
                                    .setExcludeFromIndexes(true)
                                    .build()
                            )
                            .set(aggregateTypeProperty, aggregateType)
                            .set(aggregateIdProperty, aggregateId)
                            .build()

            @Suppress("UNCHECKED_CAST")
            val aggregateEvents = getTextList(aggregateEntity.getList(eventsProperty))
            val currentVersion = aggregateEntity.getLong(versionProperty)

            /**
             *  If the current version is different than what was hydrated during the state change then we know we
             *  have an event collision. This is a very simple approach and more "business knowledge" can be added
             *  here to handle scenarios where the versions may be different but the state change can still occur.
             */
            if (currentVersion != saveOptions.version) {
                return SaveEventsResponse.EventCollision(aggregateId, currentVersion)
            }

            val eventsModel = events.mapIndexed { index, it -> EventModel(it.kind, currentVersion + index + 1, it.identityId, it.timestamp, it.data.payload.toString(Charsets.UTF_8)) }

            val sequenceIds = (1..eventsModel.size).map { idGenerator.nextId() }

            val eventIndexes = eventsModel.mapIndexed { index, eventModel ->
                val sequenceId = sequenceIds[index]

                val indexKey = keyQueryFactory.createIndexKey(aggregateKey, eventModel)

                val eventIndex = Entity.newBuilder(indexKey)
                eventIndex.set(aggregateIdProperty, StringValue.newBuilder(aggregateId).setExcludeFromIndexes(true).build())
                eventIndex.set(aggregateTypeProperty, aggregateType)
                eventIndex.set(aggregateIndexProperty, LongValue.newBuilder(aggregateIndex).setExcludeFromIndexes(true).build())
                eventIndex.set(versionProperty, LongValue.newBuilder(eventModel.version).setExcludeFromIndexes(true).build())
                eventIndex.set(sequenceProperty, sequenceId)
                eventIndex.build()
            }.toTypedArray()

            var sizeInBytes = 0
            val eventsAsText = eventsModel.map {
                val eventPayload = messageFormat.formatToString(it)
                sizeInBytes += eventPayload.toByteArray().size
                eventPayload
            }

            aggregateEvents.addAll(eventsAsText)

            val newAggregateEntity = Entity.newBuilder(aggregateEntity)
                    .set(eventsProperty, ListValue.of(aggregateEvents.map { StringValue.newBuilder(it).setExcludeFromIndexes(true).build() }))
                    .set(versionProperty, LongValue.of(currentVersion + events.size))
                    .build()

            // Current datastore entity size is 1,048,572 bytes and we add buffer of 100kb for the metadata to be sure
            // that entity could be stored in the datastore.
            if (sizeInBytes >= 948572) {
                var snapshot: Snapshot? = null
                //if a build snapshot does not exist it would not have the field data filled in.
                if (snapshotEntity != null) {
                    val blobData = snapshotEntity.getBlob("data")
                    snapshot = Snapshot(
                            snapshotEntity.getLong("version") as Long? ?: 0,
                            Binary(blobData.toByteArray())
                    )
                }
                aggregateEvents.removeAll(eventsAsText)
                return SaveEventsResponse.SnapshotRequired(adaptEvents(aggregateEvents), snapshot)
            }

            if (snapshotEntity != null) {
                transaction.put(snapshotEntity, newAggregateEntity, *eventIndexes)
            } else {
                transaction.put(newAggregateEntity, *eventIndexes)
            }

            transaction.commit()
            return SaveEventsResponse.Success(aggregateId, currentVersion, sequenceIds)
        } catch (ex: Exception) {
            ex.printStackTrace()
            return SaveEventsResponse.Error("could not save events due: ${ex.message}")
        } finally {
            if (transaction != null && transaction.isActive) {
                transaction.rollback()
            }
        }
    }


    override fun getEvents(aggregateIds: List<String>, aggregateType: String): GetEventsResponse {
        val snapshotKeyToAggregateId = mutableMapOf<Key, String>()
        val snapshotKeys = aggregateIds.map {
            val key = keyQueryFactory.createSnapshotKey(aggregateType, it)
            snapshotKeyToAggregateId[key] = it
            key
        }

        val snapshotEntities = datastore.get(snapshotKeys).asSequence()
                .map { it.key to it }
                .toMap()
                .toMutableMap()

        if (snapshotEntities.size < aggregateIds.size) {
            aggregateIds.forEach {
                val key = keyQueryFactory.createSnapshotKey(it, aggregateType)
                if (!snapshotEntities.containsKey(key)) {
                    snapshotEntities[key] = Entity.newBuilder(key)
                            .set("aggregateIndex", LongValue.newBuilder(0L).setExcludeFromIndexes(true).build())
                            .set(aggregateIdProperty, it)
                            .build()
                }
            }
        }

        val keyToAggregateId = mutableMapOf<Key, String>()
        val aggregateKeys = snapshotEntities.values.map {
            val key = keyQueryFactory.createAggregateKey(aggregateType, it.getString(aggregateIdProperty), it.getLong("aggregateIndex"))
            keyToAggregateId[key] = it.key.name
            key
        }

        val aggregateEntities = datastore.get(aggregateKeys).asSequence().map { it.key to it }.toMap()

        val aggregates = mutableListOf<Aggregate>()

        aggregateEntities.keys.forEach {
            val aggregateEntity = aggregateEntities[it]!!
            var snapshot: Snapshot? = null

            val snapshotKey = keyQueryFactory.createSnapshotKey(aggregateType, aggregateEntity.getString(aggregateIdProperty))
            if (snapshotEntities.containsKey(snapshotKey)) {
                val thisSnapshot = snapshotEntities[snapshotKey]!!
                val version = thisSnapshot.getLong("aggregateIndex") as Long? ?: 0
                if (thisSnapshot.contains("data")) {
                    val data = thisSnapshot.getBlob("data")
                    snapshot = Snapshot(
                            version,
                            Binary(data.toByteArray())
                    )
                }
            }
            val aggregateId = snapshotKeyToAggregateId[snapshotKey]!!
            val currentVersion = aggregateEntity.getLong(versionProperty)
            val events = adaptEvents(getTextList(aggregateEntity.getList(eventsProperty)))

            aggregates.add(Aggregate(aggregateId, aggregateType, snapshot, currentVersion, events))
        }

        return GetEventsResponse.Success(aggregates)
    }

    override fun getEvents(aggregateId: String, aggregateType: String, index: Long?): GetEventsResponse {
        var snapshot: Snapshot? = null

        val aggregateKey = if (index == null) {
            val snapshotKey = keyQueryFactory.createSnapshotKey(aggregateType, aggregateId)
            val snapshotEntity: Entity?

            snapshotEntity = datastore.get(snapshotKey)

            val aggregateIndex = snapshotEntity?.getLong("aggregateIndex") ?: 0
            val version = snapshotEntity?.getLong("version") ?: 0

            val snapshotData = snapshotEntity?.getBlob("data")
            snapshot = if (snapshotData != null) {
                Snapshot(version, Binary(snapshotData.toByteArray()))
            } else null

            keyQueryFactory.createAggregateKey(aggregateType, aggregateId, aggregateIndex)
        } else {
            keyQueryFactory.createAggregateKey(aggregateType, aggregateId, index)
        }

        val aggregate =
                datastore.get(aggregateKey)
                        ?: return GetEventsResponse.AggregateNotFound(listOf(aggregateId), aggregateType)

        @Suppress("UNCHECKED_CAST")
        val aggregateEvents = getTextList(aggregate.getList(eventsProperty))
        val currentVersion = aggregate.getLong(versionProperty)

        val events = aggregateEvents.map {
            messageFormat.parse<EventModel>(ByteArrayInputStream(
                    it.toByteArray(Charsets.UTF_8)), EventModel::class.java)

        }.map { EventPayload(it.kind, it.timestamp, it.identityId, Binary(it.payload.toByteArray(Charsets.UTF_8))) }

        return GetEventsResponse.Success(listOf(Aggregate(aggregateId, aggregateType, snapshot, currentVersion, events)))
    }

    override fun getAllEvents(request: GetAllEventsRequest): GetAllEventsResponse {

        val startSequenceId = request.position?.value ?: 0

        // Proper query needs to be send to the backend depending on the
        // read direction.
        val eventsFilterPredicate = if (request.readDirection == ReadDirection.FORWARD) {
            StructuredQuery.PropertyFilter.gt(sequenceProperty, startSequenceId)
        } else {
            StructuredQuery.PropertyFilter.lt(sequenceProperty, startSequenceId)
        }

        var eventKeys = mutableListOf<IndexedEventKey>()
        val filters = mutableListOf<StructuredQuery.Filter>()

        if (request.aggregateTypes.isEmpty()) {
            filters.add(eventsFilterPredicate)
        }

        filters += request.aggregateTypes.map {
            StructuredQuery.CompositeFilter.and(
                    eventsFilterPredicate,
                    StructuredQuery.PropertyFilter.eq(aggregateTypeProperty, it)
            )
        }

        //TODO(mgenov): use coroutines to make it async but it will break the GAE compatibility.
        filters.forEach {
            val query = keyQueryFactory.createIndexLookupQuery(it, request.maxCount)
            val indexes = datastore.run(query).asSequence().map(indexedEventTransform).toList()
            
            eventKeys.addAll(indexes)
        }

        // Backward direction requires events to be retrieved in reversed order.
        if (request.readDirection == ReadDirection.BACKWARD) {
            eventKeys = eventKeys.asReversed()
        }

        val indexByAggregate = eventKeys.groupBy {
            AggregateLookupKey(it.aggregateType, it.aggregateId, it.aggregateIndex)
        }

        val aggregateKeys = indexByAggregate.keys.map { keyQueryFactory.createAggregateKey(it.aggregateType, it.aggregateId, it.aggregateIndex) }

        val aggregateEntities = datastore.get(aggregateKeys).asSequence().map { it.key to it }.toMap()

        val indexedEvents = mutableListOf<IndexedEvent>()
        var lastSequenceId = 0L

        indexByAggregate.keys.forEach {
            val key = keyQueryFactory.createAggregateKey(it.aggregateType, it.aggregateId, it.aggregateIndex)

            val aggregate = aggregateEntities[key]!!
            val eventVersions = indexByAggregate[it]!!
            val aggregateEvents = getTextList(aggregate.getList(eventsProperty))

            eventVersions.forEach {
                val datastorePayload = aggregateEvents[it.version.toInt() - 1]
                val eventModel = messageFormat.parse<EventModel>(ByteArrayInputStream(
                        datastorePayload.toByteArray(Charsets.UTF_8)), EventModel::class.java)

                val eventPayload = EventPayload(
                        eventModel.kind,
                        eventModel.timestamp,
                        eventModel.identityId,
                        Binary(eventModel.payload.toByteArray(Charsets.UTF_8))
                )

                val indexedEvent = IndexedEvent(Position(it.sequenceId), it.aggregateId, it.aggregateType, it.version, eventPayload)
                indexedEvents.add(indexedEvent)

                lastSequenceId = it.sequenceId
            }

        }

        return GetAllEventsResponse.Success(indexedEvents, ReadDirection.FORWARD, Position(lastSequenceId))
    }

    override fun revertLastEvents(aggregateType: String, aggregateId: String, count: Int): RevertEventsResponse {
        if (count == 0) {
            throw IllegalArgumentException("trying to revert zero events")
        }

        var transaction: Transaction? = null
        try {
            transaction = datastore.newTransaction()

            val snapshotKey = keyQueryFactory.createSnapshotKey(aggregateId, aggregateType)

            val snapshot = transaction.get(snapshotKey)

            val aggregateIndex: Long

            aggregateIndex = snapshot?.getLong("aggregateIndex") ?: 0

            val aggregateKey = keyQueryFactory.createAggregateKey(aggregateType, aggregateId, aggregateIndex)

            val aggregateEntity = transaction.get(aggregateKey)
                    ?: return RevertEventsResponse.AggregateNotFound(aggregateId, aggregateType)

            @Suppress("UNCHECKED_CAST")
            val aggregateEvents = getTextList(aggregateEntity.getList(eventsProperty))
            val currentVersion = aggregateEntity.getLong(versionProperty)

            if (count > aggregateEvents.size) {
                return RevertEventsResponse.ErrorNotEnoughEventsToRevert(aggregateEvents.size, count)
            }

            val lastEventIndex = aggregateEvents.size - count
            val updatedEvents = aggregateEvents.filterIndexed { index, _ -> index < lastEventIndex }

            val updatedAggregate = Entity.newBuilder(aggregateEntity)
                    .set(eventsProperty, ListValue.of(updatedEvents.map { StringValue.newBuilder(it).setExcludeFromIndexes(true).build() }))
                    .set(versionProperty, LongValue.newBuilder(currentVersion - count).setExcludeFromIndexes(true).build())
                    .build()

            if (snapshot != null) {
                transaction.put(snapshot, updatedAggregate)
            } else {
                transaction.put(updatedAggregate)
            }

            transaction.commit()

        } catch (ex: Exception) {
            return RevertEventsResponse.Error("could not save events due: ${ex.message}")
        } finally {
            if (transaction != null && transaction.isActive) {
                transaction.rollback()
            }
        }

        return RevertEventsResponse.Success(listOf())
    }

    private fun adaptEvents(aggregateEvents: List<String>): List<EventPayload> {
        return aggregateEvents.map {
            messageFormat.parse<EventModel>(ByteArrayInputStream(it.toByteArray(Charsets.UTF_8)), EventModel::class.java)
        }.map { EventPayload(it.kind, it.timestamp, it.identityId, Binary(it.payload.toByteArray(Charsets.UTF_8))) }
    }

    private fun getTextList(value: List<Value<String>>): MutableList<String> {
        return value.map { it.get()!! }.toMutableList()
    }

}