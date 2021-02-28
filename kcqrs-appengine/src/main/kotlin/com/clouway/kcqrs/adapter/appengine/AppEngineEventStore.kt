package com.clouway.kcqrs.adapter.appengine

import com.clouway.kcqrs.core.Aggregate
import com.clouway.kcqrs.core.Binary
import com.clouway.kcqrs.core.EventPayload
import com.clouway.kcqrs.core.EventStore
import com.clouway.kcqrs.core.GetAllEventsRequest
import com.clouway.kcqrs.core.GetAllEventsResponse
import com.clouway.kcqrs.core.GetEventsFromStreamsRequest
import com.clouway.kcqrs.core.GetEventsResponse
import com.clouway.kcqrs.core.IdGenerator
import com.clouway.kcqrs.core.IndexedEvent
import com.clouway.kcqrs.core.Position
import com.clouway.kcqrs.core.ReadDirection
import com.clouway.kcqrs.core.RevertEventsResponse
import com.clouway.kcqrs.core.SaveEventsRequest
import com.clouway.kcqrs.core.SaveEventsResponse
import com.clouway.kcqrs.core.SaveOptions
import com.clouway.kcqrs.core.Snapshot
import com.clouway.kcqrs.core.messages.DataModelFormat
import com.google.appengine.api.datastore.Blob
import com.google.appengine.api.datastore.DatastoreServiceFactory
import com.google.appengine.api.datastore.Entity
import com.google.appengine.api.datastore.EntityNotFoundException
import com.google.appengine.api.datastore.EntityTranslator
import com.google.appengine.api.datastore.FetchOptions
import com.google.appengine.api.datastore.Key
import com.google.appengine.api.datastore.KeyFactory
import com.google.appengine.api.datastore.Query
import com.google.appengine.api.datastore.Text
import com.google.appengine.api.datastore.Transaction
import com.google.appengine.api.datastore.TransactionOptions
import java.io.ByteArrayInputStream

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class AppEngineEventStore(private val kind: String = "Event", private val messageFormat: DataModelFormat, private val idGenerator: IdGenerator) : EventStore {

    /**
     * Entity Kind used for storing of snapshots.
     */
    private val snapshotKind = kind + "Snapshot"

    /**
     * Property name for the aggregate id.
     */
    private val aggregateIdProperty = "i"

    /**
     * Property name for the aggregate type.
     */
    private val aggregateTypeProperty = "a"
    
    /**
     * Property name for the tenant type.
     */
    private val tenantProperty = "t"

    /**
     * Property name for the aggregate type.
     */
    private val streamProperty = "st"

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
     * Entity Kind used for storing of event index: [aggregateId, aggregateType, sequence number, version]
     */
    private val indexKind = kind + "Index"

    override fun saveEvents(request: SaveEventsRequest, saveOptions: SaveOptions): SaveEventsResponse {
        var transaction: Transaction? = null

        try {
            val dataStore = DatastoreServiceFactory.getDatastoreService()
            transaction = dataStore.beginTransaction(TransactionOptions.Builder.withXG(true))

            val snapshotKey = snapshotKey(request.tenant, request.stream)
            var snapshotEntity: Entity?

            snapshotEntity = try {
                dataStore.get(transaction, snapshotKey)
            } catch (ex: EntityNotFoundException) {
                null
            }

            var aggregateIndex = if (snapshotEntity != null) snapshotEntity.getProperty("aggregateIndex") as Long else 0
            var snapshot: Snapshot? = null
            if (saveOptions.createSnapshot.required && saveOptions.createSnapshot.snapshot != null) {

                if (snapshotEntity == null) {
                    snapshotEntity = Entity(snapshotKind, snapshotKey.name)
                }

                aggregateIndex += 1
                val snapshotData = saveOptions.createSnapshot.snapshot!!.data
                val version = saveOptions.createSnapshot.snapshot!!.version
                snapshotEntity.setUnindexedProperty("version", version)
                snapshotEntity.setUnindexedProperty("data", Blob(snapshotData.payload))
                snapshotEntity.setUnindexedProperty("aggregateIndex", aggregateIndex)
                snapshotEntity.setUnindexedProperty(tenantProperty, request.tenant)
                snapshotEntity.setUnindexedProperty(streamProperty, request.stream)
                
                snapshot = Snapshot(version, snapshotData)
            }
            
            if (snapshotEntity != null && !saveOptions.createSnapshot.required) {
                val snapshotVersion = snapshotEntity.getProperty("version") as Long
                val snapshotData = snapshotEntity.getProperty("data") as Blob
                snapshot = Snapshot(snapshotVersion, Binary(snapshotData.bytes))
            }

            /*
             * Keys of aggregates are dispatched on different levels by using an index for scalling purposes.
             * Each aggregate in the datastore has single or multiple levels.
             */
            val aggregateKey = aggregateKey(request.tenant, request.stream, aggregateIndex)

            val aggregateEntity = try {
                dataStore.get(transaction, aggregateKey)
            } catch (ex: EntityNotFoundException) {
                val entity = Entity(aggregateKey)
                entity.setUnindexedProperty(eventsProperty, mutableListOf<String>())
                entity.setUnindexedProperty(versionProperty, saveOptions.createSnapshot.snapshot?.version ?: 0L)
                entity.setProperty(aggregateTypeProperty, request.aggregateType)

                entity
            }

            @Suppress("UNCHECKED_CAST")
            val aggregateEvents = getTextList(aggregateEntity.getProperty(eventsProperty))
            val currentVersion = aggregateEntity.getProperty(versionProperty) as Long

            /**
             *  If the current version is different than what was hydrated during the state change then we know we
             *  have an event collision. This is a very simple approach and more "business knowledge" can be added
             *  here to handle scenarios where the versions may be different but the state change can still occur.
             */
            if (currentVersion != saveOptions.version) {
                return SaveEventsResponse.EventCollision(currentVersion)
            }
            val newVersion = currentVersion + request.events.size
            val eventsModel = request.events.mapIndexed { index, it -> EventModel(it.aggregateId, it.kind, currentVersion + index + 1, it.identityId, it.timestamp, it.data.payload.toString(Charsets.UTF_8)) }
            val sequenceIds = (1..eventsModel.size).map { idGenerator.nextId() }
            val eventIndexes = eventsModel.mapIndexed { index, eventModel ->
                val sequenceId = sequenceIds[index]
                val eventIndex = Entity(indexKind, eventModel.version, aggregateKey)
                eventIndex.setUnindexedProperty(aggregateIdProperty, eventModel.aggregateId)
                eventIndex.setIndexedProperty(tenantProperty, request.tenant)
                eventIndex.setIndexedProperty(streamProperty, request.stream)
                eventIndex.setUnindexedProperty(aggregateTypeProperty, request.aggregateType)
                eventIndex.setUnindexedProperty(aggregateIndexProperty, aggregateIndex)
                eventIndex.setUnindexedProperty(versionProperty, eventModel.version)
                eventIndex.setIndexedProperty(sequenceProperty, sequenceId)
                eventIndex
            }

            val eventsAsText = eventsModel.map { Text(messageFormat.formatToString(it)) }

            aggregateEvents.addAll(eventsAsText)

            aggregateEntity.setUnindexedProperty(eventsProperty, aggregateEvents)
            
            aggregateEntity.setUnindexedProperty(versionProperty, newVersion)

            val protoEntity = EntityTranslator.convertToPb(aggregateEntity)

            //Current datastore entity size is 1,048,572 bytes
            if (protoEntity.serializedSize >= 1048572) {
                //if a build snapshot does not exist it would not have the field data filled in.
                if (snapshotEntity != null) {
                    val blobData = snapshotEntity.getProperty("data") as Blob
                    snapshot = Snapshot(
                            snapshotEntity.getProperty("version") as Long? ?: 0,
                            Binary(blobData.bytes))
                }
                aggregateEvents.removeAll(eventsAsText)
                return SaveEventsResponse.SnapshotRequired(adaptEvents(aggregateEvents), snapshot, currentVersion)
            }

            if (snapshotEntity != null) {
                dataStore.put(transaction, listOf(snapshotEntity, aggregateEntity) + eventIndexes)
            } else {
                dataStore.put(transaction, listOf(aggregateEntity) + eventIndexes)
            }

            transaction.commit()
            return SaveEventsResponse.Success(currentVersion, sequenceIds, Aggregate(request.aggregateType, snapshot, newVersion, adaptEvents(aggregateEvents)))
        } catch (ex: Exception) {
            return SaveEventsResponse.Error("could not save events due: ${ex.message}")
        } finally {
            if (transaction != null && transaction.isActive) {
                transaction.rollback()
            }
        }
    }

    override fun getEventsFromStreams(request: GetEventsFromStreamsRequest): GetEventsResponse {
        val dataStore = DatastoreServiceFactory.getDatastoreService()
        val snapshotKeyToAggregateId = mutableMapOf<Key, String>()
        val snapshotKeys = request.streams.map {
            val key = snapshotKey(request.tenant, it)
            snapshotKeyToAggregateId[key] = it
            key
        }

        val snapshotEntities = try {
            dataStore.get(snapshotKeys)
        } catch (ex: EntityNotFoundException) {
            mapOf<Key, Entity>()
        }.toMutableMap()

        if (snapshotEntities.size < request.streams.size) {
            request.streams.forEach {
                val key = snapshotKey(request.tenant, it)
                if (!snapshotEntities.containsKey(key)) {
                    val entity = Entity(snapshotKind, key.name)
                    entity.setProperty(streamProperty,  it)
                    snapshotEntities[key] = entity
                }
            }
        }

        val keyToAggregateId = mutableMapOf<Key, String>()
        val aggregateKeys = snapshotEntities.values.map {
            val index = it.getProperty("aggregateIndex") as Long? ?: 0
            val stream = it.getProperty(streamProperty) as String? ?: ""
            val key = aggregateKey(request.tenant, stream, index)
            keyToAggregateId[key] = it.key.name
            key
        }

        val aggregateEntities = dataStore.get(aggregateKeys)

        val aggregates = mutableListOf<Aggregate>()
        aggregateEntities.keys.forEach {
            val aggregateEntity = aggregateEntities[it]!!
            var snapshot: Snapshot? = null
            val snapshotKey = KeyFactory.createKey(snapshotKind, keyToAggregateId[it]!!)
            if (snapshotEntities.containsKey(snapshotKey)) {
                val thisSnapshot = snapshotEntities[snapshotKey]!!
                val version = thisSnapshot.getProperty("version") as Long? ?: 0
                if (thisSnapshot.hasProperty("data")) {
                    val data = thisSnapshot.getProperty("data") as Blob
                    snapshot = Snapshot(
                            version,
                            Binary(data.bytes)
                    )
                }
            }

            val currentVersion = aggregateEntity.getProperty(versionProperty) as Long
            val aggregateType = aggregateEntity.getProperty(aggregateTypeProperty) as String
            val events = adaptEvents(getTextList(aggregateEntity.getProperty(eventsProperty)))

            val eventsByAggregate = events.groupBy { it.aggregateId }
            eventsByAggregate.forEach { (_, v) ->
                aggregates.add(Aggregate(aggregateType, snapshot, currentVersion, v))
            }
            
        }

        return GetEventsResponse.Success(aggregates)
    }

    
    override fun getAllEvents(request: GetAllEventsRequest): GetAllEventsResponse {
        val dataStore = DatastoreServiceFactory.getDatastoreService()
        val startSequenceId = request.position?.value ?: 0

        // Proper query needs to be send to the backend depending on the
        // read direction.
        val eventsFilterPredicate = if (request.readDirection == ReadDirection.FORWARD) {
            Query.FilterPredicate(sequenceProperty, Query.FilterOperator.GREATER_THAN, startSequenceId)
        } else {
            Query.FilterPredicate(sequenceProperty, Query.FilterOperator.LESS_THAN, startSequenceId)
        }

        var filter: Query.Filter = eventsFilterPredicate


        if (!request.streams.isEmpty()) {
            filter = Query.CompositeFilter(Query.CompositeFilterOperator.AND, listOf(
                    eventsFilterPredicate, Query.FilterPredicate(streamProperty, Query.FilterOperator.IN, request.streams)
            ))
        }

        var eventKeys = dataStore.prepare(Query(indexKind).setFilter(filter))
                .asIterable(FetchOptions.Builder.withLimit(request.maxCount))
                .map {
                    val tenant = it.getProperty(tenantProperty) as String
                    val stream = it.getProperty(streamProperty) as String
                    val aggregateType = it.getProperty(aggregateTypeProperty) as String
                    val aggregateId = it.getProperty(aggregateIdProperty) as String
                    val aggregateIndex = it.getProperty(aggregateIndexProperty) as Long
                    val version = it.getProperty(versionProperty) as Long
                    val sequenceId = it.getProperty(sequenceProperty) as Long
                    
                    IndexedEventKey(tenant, stream, sequenceId, aggregateId, aggregateType, aggregateIndex, version)
                }

        // Backward direction requires events to be retrieved in reversed order.
        if (request.readDirection == ReadDirection.BACKWARD) {
            eventKeys = eventKeys.asReversed()
        }

        val indexByAggregate = eventKeys.groupBy {
            AggregateLookupKey(it.tenant, it.stream, it.aggregateIndex)
        }

        val aggregateKeys = indexByAggregate.keys.map { aggregateKey(it.tenant, it.stream, it.aggregateIndex) }
        val aggregateEntities = dataStore.get(aggregateKeys)

        val indexedEvents = mutableListOf<IndexedEvent>()
        var lastSequenceId = 0L

        indexByAggregate.keys.forEach {
            val key = aggregateKey(it.tenant, it.stream, it.aggregateIndex)
            val aggregate = aggregateEntities[key]!!
            val eventVersions = indexByAggregate[it]!!
            val aggregateEvents = getTextList(aggregate.getProperty(eventsProperty))

            eventVersions.forEach {
                val datastorePayload = aggregateEvents[it.version.toInt() - 1]
                val eventModel = messageFormat.parse<EventModel>(ByteArrayInputStream(
                        datastorePayload.value.toByteArray(Charsets.UTF_8)), EventModel::class.java)

                val eventPayload = EventPayload(
                        eventModel.aggregateId,
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

    override fun revertLastEvents(tenant: String, stream: String, count: Int): RevertEventsResponse {
        if (count == 0) {
            throw IllegalArgumentException("trying to revert zero events")
        }

        var transaction: Transaction? = null
        try {
            val dataStore = DatastoreServiceFactory.getDatastoreService()
            transaction = dataStore.beginTransaction(TransactionOptions.Builder.withXG(true))

            val snapshotKey = snapshotKey(stream, tenant)

            try {
                val snapshot: Entity? = try {
                    dataStore.get(transaction, snapshotKey)
                } catch (e: EntityNotFoundException) {
                    null
                }
                val aggregateIndex: Long

                aggregateIndex = if (snapshot != null) snapshot.getProperty("aggregateIndex") as Long else 0

                val aggregateKey = aggregateKey(tenant, stream, aggregateIndex)

                val aggregateEntity = dataStore.get(transaction, aggregateKey)

                @Suppress("UNCHECKED_CAST")
                val aggregateEvents = getTextList(aggregateEntity.getProperty(eventsProperty))
                val currentVersion = aggregateEntity.getProperty(versionProperty) as Long

                if (count > aggregateEvents.size) {
                    return RevertEventsResponse.ErrorNotEnoughEventsToRevert(aggregateEvents.size, count)
                }

                val lastEventIndex = aggregateEvents.size - count
                val updatedEvents = aggregateEvents.filterIndexed { index, _ -> index < lastEventIndex }

                aggregateEntity.setUnindexedProperty(eventsProperty, updatedEvents)
                aggregateEntity.setUnindexedProperty(versionProperty, currentVersion - count)
                if (snapshot != null) {
                    dataStore.put(transaction, listOf(snapshot, aggregateEntity))
                } else {
                    dataStore.put(transaction, listOf(aggregateEntity))
                }


                transaction.commit()

            } catch (ex: EntityNotFoundException) {
                return RevertEventsResponse.AggregateNotFound(stream, tenant)
            }

        } catch (ex: Exception) {
            return RevertEventsResponse.Error("could not save events due: ${ex.message}")
        } finally {
            if (transaction != null && transaction.isActive) {
                transaction.rollback()
            }
        }

        return RevertEventsResponse.Success(listOf())
    }
    
    private fun aggregateKey(aggregateType: String, aggregateId: String?, aggregateIndex: Long): Key {
        if (aggregateId.isNullOrEmpty()) {
            return KeyFactory.createKey(kind, "${aggregateType}_$aggregateIndex")
        }
        return KeyFactory.createKey(kind, "${aggregateType}_${aggregateId}_$aggregateIndex")
    }

    
    private fun snapshotKey(tenant: String, stream: String) =
            KeyFactory.createKey(snapshotKind, "${tenant}_$stream")

    
    private fun adaptEvents(aggregateEvents: List<Text>): List<EventPayload> {
        return aggregateEvents.map {
            messageFormat.parse<EventModel>(ByteArrayInputStream(it.value.toByteArray(Charsets.UTF_8)), EventModel::class.java)
        }.map { EventPayload(it.aggregateId, it.kind, it.timestamp, it.identityId, Binary(it.payload.toByteArray(Charsets.UTF_8))) }
    }

    private fun getTextList(value: Any): MutableList<Text> {
        return if (value is List<*>) {
            value.map {
                if (it is String) {
                    Text(it)
                } else {
                    it as Text
                }
            }.toMutableList()
        } else {
            mutableListOf()
        }
    }

}