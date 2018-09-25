package com.clouway.kcqrs.adapter.appengine

import com.clouway.kcqrs.core.Aggregate
import com.clouway.kcqrs.core.Binary
import com.clouway.kcqrs.core.EventPayload
import com.clouway.kcqrs.core.EventStore
import com.clouway.kcqrs.core.GetEventsResponse
import com.clouway.kcqrs.core.RevertEventsResponse
import com.clouway.kcqrs.core.SaveEventsResponse
import com.clouway.kcqrs.core.SaveOptions
import com.clouway.kcqrs.core.Snapshot
import com.clouway.kcqrs.core.messages.MessageFormat
import com.google.appengine.api.datastore.Blob
import com.google.appengine.api.datastore.DatastoreServiceFactory
import com.google.appengine.api.datastore.Entity
import com.google.appengine.api.datastore.EntityNotFoundException
import com.google.appengine.api.datastore.EntityTranslator
import com.google.appengine.api.datastore.Key
import com.google.appengine.api.datastore.KeyFactory
import com.google.appengine.api.datastore.Text
import com.google.appengine.api.datastore.Transaction
import com.google.appengine.api.datastore.TransactionOptions
import java.io.ByteArrayInputStream

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class AppEngineEventStore(private val kind: String = "Event", private val messageFormat: MessageFormat) : EventStore {

    /**
     * Entity Kind used for storing of snapshots.
     */
    private val snapshotKind = kind + "Snapshot"

    /**
     * Property name for the aggregate type.
     */
    private val aggregateTypeProperty = "a"

    /**
     * Property name for the list of event data in the entity.
     */
    private val eventsProperty = "e"

    /**
     * Property name of the version which is used for concurrency control.
     */
    private val versionProperty = "v"

    override fun saveEvents(aggregateType: String, events: List<EventPayload>, saveOptions: SaveOptions): SaveEventsResponse {
        val aggregateId = saveOptions.aggregateId
        var transaction: Transaction? = null

        try {
            val dataStore = DatastoreServiceFactory.getDatastoreService()
            transaction = dataStore.beginTransaction(TransactionOptions.Builder.withXG(true))

            val snapshotKey = snapshotKey(aggregateId, aggregateType)
            var snapshotEntity: Entity?

            snapshotEntity = try {
                dataStore.get(transaction, snapshotKey)
            } catch (ex: EntityNotFoundException) {
                null
            }

            var aggregateIndex = if (snapshotEntity != null) snapshotEntity.getProperty("aggregateIndex") as Long else 0

            if (saveOptions.createSnapshot.required && saveOptions.createSnapshot.snapshot != null) {

                if (snapshotEntity == null) {
                    snapshotEntity = Entity(snapshotKind, snapshotKey.name)
                }

                aggregateIndex += 1
                val snapshotData = saveOptions.createSnapshot.snapshot!!.data
                snapshotEntity.setUnindexedProperty("version", saveOptions.createSnapshot.snapshot!!.version)
                snapshotEntity.setUnindexedProperty("data", Blob(snapshotData.payload))
                snapshotEntity.setUnindexedProperty("aggregateIndex", aggregateIndex)
            }

            /*
             * Keys of aggregates are dispatched on different levels by using an index for scalling purposes.
             * Each aggregate in the datastore has single or multiple levels.
             */
            val aggregateKey = aggregateKey(aggregateType, aggregateId, aggregateIndex)

            val aggregateEntity = try {
                dataStore.get(transaction, aggregateKey)
            } catch (ex: EntityNotFoundException) {
                val entity = Entity(aggregateKey)
                entity.setUnindexedProperty(eventsProperty, mutableListOf<String>())
                entity.setUnindexedProperty(versionProperty, 0L)
                entity.setProperty(aggregateTypeProperty, aggregateType)

                entity
            }

            @Suppress("UNCHECKED_CAST")
            val aggregateEvents = aggregateEntity.getProperty(eventsProperty) as MutableList<Text>
            val currentVersion = aggregateEntity.getProperty(versionProperty) as Long

            /**
             *  If the current version is different than what was hydrated during the state change then we know we
             *  have an event collision. This is a very simple approach and more "business knowledge" can be added
             *  here to handle scenarios where the versions may be different but the state change can still occur.
             */
            if (currentVersion != saveOptions.version) {
                return SaveEventsResponse.EventCollision(aggregateId, currentVersion)
            }

            val eventsModel = events.mapIndexed { index, it -> EventModel(it.kind, currentVersion + index + 1, it.identityId, it.timestamp, it.data.payload.toString(Charsets.UTF_8)) }
            val eventsAsText = eventsModel.map { Text(messageFormat.formatToString(it)) }

            aggregateEvents.addAll(eventsAsText)

            aggregateEntity.setUnindexedProperty(eventsProperty, aggregateEvents)
            aggregateEntity.setUnindexedProperty(versionProperty, currentVersion + events.size)

            val protoEntity = EntityTranslator.convertToPb(aggregateEntity)

            //Current datastore entity size is 1,048,572 bytes
            if (protoEntity.serializedSize >= 1048572) {
                var snapshot: Snapshot? = null
                //if a build snapshot does not exist it would not have the field data filled in.
                if (snapshotEntity != null) {
                    val blobData = snapshotEntity.getProperty("data") as Blob
                    snapshot = Snapshot(
                            snapshotEntity.getProperty("aggregateIndex") as Long? ?: 0,
                            Binary(blobData.bytes))
                }
                aggregateEvents.removeAll(eventsAsText)
                return SaveEventsResponse.SnapshotRequired(adaptEvents(aggregateEvents), snapshot)
            }
            if (snapshotEntity != null) {
                dataStore.put(transaction, listOf(snapshotEntity, aggregateEntity))
            } else {
                dataStore.put(transaction, listOf(aggregateEntity))
            }

            transaction.commit()
            return SaveEventsResponse.Success(aggregateId, currentVersion)
        } catch (ex: Exception) {
            return SaveEventsResponse.Error("could not save events due: ${ex.message}")
        } finally {
            if (transaction != null && transaction.isActive) {
                transaction.rollback()
            }
        }
    }


    override fun getEvents(aggregateIds: List<String>, aggregateType: String): GetEventsResponse {
        val dataStore = DatastoreServiceFactory.getDatastoreService()
        val snapshotKeyToAggregateId = mutableMapOf<Key, String>()
        val snapshotKeys = aggregateIds.map {
            val key = snapshotKey(it, aggregateType)
            snapshotKeyToAggregateId[key] = it
            key
        }

        val snapshotEntities = try {
            dataStore.get(snapshotKeys)
        } catch (ex: EntityNotFoundException) {
            mapOf<Key, Entity>()
        }.toMutableMap()

        if (snapshotEntities.size < aggregateIds.size) {
            aggregateIds.forEach {
                val key = snapshotKey(it, aggregateType)
                if (!snapshotEntities.containsKey(key)) {
                    snapshotEntities[key] = Entity(snapshotKind, key.name)
                }
            }
        }

        val keyToAggregateId = mutableMapOf<Key, String>()
        val aggregateKeys = snapshotEntities.values.map {
            val key = aggregateKey(it.key.name, it.getProperty("aggregateIndex") as Long? ?: 0)
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
                val version = thisSnapshot.getProperty("aggregateIndex") as Long? ?: 0
                if (thisSnapshot.hasProperty("data")) {
                    val data = thisSnapshot.getProperty("data") as Blob
                    snapshot = Snapshot(
                            version,
                            Binary(data.bytes))
                }
            }
            val aggregateId = snapshotKeyToAggregateId[snapshotKey]!!
            val aggregateEvents = aggregateEntity.getProperty(eventsProperty) as List<*>
            val currentVersion = aggregateEntity.getProperty(versionProperty) as Long
            val events = adaptEvents(aggregateEvents.filterIsInstance(Text::class.java))

            aggregates.add(Aggregate(aggregateId, aggregateType, snapshot, currentVersion, events))
        }

        return GetEventsResponse.Success(aggregates)
    }

    override fun getEvents(aggregateId: String, aggregateType: String): GetEventsResponse {
        val dataStore = DatastoreServiceFactory.getDatastoreService()

        val snapshotKey = snapshotKey(aggregateId, aggregateType)
        val snapshotEntity: Entity?

        snapshotEntity = try {
            dataStore.get(snapshotKey)
        } catch (ex: EntityNotFoundException) {
            null
        }

        val aggregateIndex = snapshotEntity?.getProperty("aggregateIndex") as Long? ?: 0
        val version = snapshotEntity?.getProperty("version") as Long? ?: 0

        val snapshotData = snapshotEntity?.getProperty("data") as Blob?
        val snapshot: Snapshot? = if (snapshotData != null) {
            Snapshot(version, Binary(snapshotData.bytes))

        } else null

        val aggregateKey = aggregateKey(aggregateType, aggregateId, aggregateIndex)

        val aggregate = try {
            dataStore.get(aggregateKey)
        } catch (ex: EntityNotFoundException) {
            return GetEventsResponse.AggregateNotFound
        }

        @Suppress("UNCHECKED_CAST")
        val aggregateEvents = aggregate.getProperty(eventsProperty) as List<Text>
        val currentVersion = aggregate.getProperty(versionProperty) as Long

        val events = aggregateEvents.map {
            messageFormat.parse<EventModel>(ByteArrayInputStream(
                    it.value.toByteArray(Charsets.UTF_8)), EventModel::class.java)

        }.map { EventPayload(it.kind, it.timestamp, it.identityId, Binary(it.payload.toByteArray(Charsets.UTF_8))) }

        return GetEventsResponse.Success(listOf(Aggregate(aggregateId, aggregateType, snapshot, currentVersion, events)))
    }

    override fun revertLastEvents(aggregateType: String, aggregateId: String, count: Int): RevertEventsResponse {
        if (count == 0) {
            throw IllegalArgumentException("trying to revert zero events")
        }

        var transaction: Transaction? = null
        try {
            val dataStore = DatastoreServiceFactory.getDatastoreService()
            transaction = dataStore.beginTransaction(TransactionOptions.Builder.withXG(true))

            val snapshotKey = snapshotKey(aggregateId, aggregateType)

            try {
                val snapshot: Entity? = try {
                    dataStore.get(transaction, snapshotKey)
                } catch (e: EntityNotFoundException) {
                    null
                }
                val aggregateIndex: Long

                aggregateIndex = if (snapshot != null) snapshot.getProperty("aggregateIndex") as Long else 0

                val aggregateKey = aggregateKey(aggregateType, aggregateId, aggregateIndex)

                val aggregateEntity = dataStore.get(transaction, aggregateKey)

                @Suppress("UNCHECKED_CAST")
                val aggregateEvents = aggregateEntity.getProperty(eventsProperty) as MutableList<Text>
                val currentVersion = aggregateEntity.getProperty(versionProperty) as Long

                if (count > aggregateEvents.size) {
                    return RevertEventsResponse.ErrorNotEnoughEventsToRevert
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
                return RevertEventsResponse.AggregateNotFound
            }

        } catch (ex: Exception) {
            return RevertEventsResponse.Error("could not save events due: ${ex.message}")
        } finally {
            if (transaction != null && transaction.isActive) {
                transaction.rollback()
            }
        }

        return RevertEventsResponse.Success
    }

    private fun aggregateKey(aggregateType: String, aggregateId: String, aggregateIndex: Long) =
            KeyFactory.createKey(kind, "${aggregateType}_${aggregateId}_$aggregateIndex")

    private fun aggregateKey(snapshotKey: String, aggregateIndex: Long) =
            KeyFactory.createKey(kind, "${snapshotKey}_$aggregateIndex")

    private fun snapshotKey(aggregateId: String, aggregateType: String) =
            KeyFactory.createKey(snapshotKind, "${aggregateType}_$aggregateId")

    private fun adaptEvents(aggregateEvents: List<Text>): List<EventPayload> {
        return aggregateEvents.map {
            messageFormat.parse<EventModel>(ByteArrayInputStream(it.value.toByteArray(Charsets.UTF_8)), EventModel::class.java)
        }.map { EventPayload(it.kind, it.timestamp, it.identityId, Binary(it.payload.toByteArray(Charsets.UTF_8))) }
    }

}