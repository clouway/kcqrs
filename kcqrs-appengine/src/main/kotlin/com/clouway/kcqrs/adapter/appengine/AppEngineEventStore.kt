package com.clouway.kcqrs.adapter.appengine

import com.clouway.kcqrs.core.*
import com.clouway.kcqrs.core.messages.MessageFormat
import com.google.appengine.api.datastore.*
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

            val snapshotKey = KeyFactory.createKey(snapshotKind, aggregateId)

            val snapshot = try {
                dataStore.get(transaction, snapshotKey)
            } catch (ex: EntityNotFoundException) {
                Entity(snapshotKind, aggregateId)
            }

            val aggregateIndex = snapshot.getProperty("aggregateIndex") as Long? ?: 0

            /*
             * Keys of aggregates are dispatched on different levels by using an index for scalling purposes.
             * Each aggregate in the datastore has single or multiple levels.
             */
            val aggregateKey = aggregateKey(aggregateId, aggregateIndex)
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
            val aggregateEvents = aggregateEntity.getProperty(eventsProperty) as MutableList<String>
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
            val eventsAsString = eventsModel.map { messageFormat.format(it) }

            aggregateEvents.addAll(eventsAsString)

            aggregateEntity.setUnindexedProperty(eventsProperty, aggregateEvents)
            aggregateEntity.setUnindexedProperty(versionProperty, currentVersion + events.size)

            dataStore.put(transaction, listOf(snapshot, aggregateEntity))

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

    override fun getEvents(aggregateId: String): GetEventsResponse {
        val dataStore = DatastoreServiceFactory.getDatastoreService()

        val snapshotKey = KeyFactory.createKey(snapshotKind, aggregateId)
        val snapshotEntity = try {
            dataStore.get(snapshotKey)
        } catch (ex: EntityNotFoundException) {
            return GetEventsResponse.SnapshotNotFound
        }

        val aggregateIndex = snapshotEntity.getProperty("aggregateIndex") as Long? ?: 0
        val snapshotVersion = snapshotEntity.getProperty("version") as Long? ?: 0

        val snapshotData = snapshotEntity.getProperty("data") as Blob?
        val snapshot: Snapshot? = if (snapshotData != null) Snapshot(snapshotVersion, Binary(snapshotData.bytes)) else null

        val aggregateKey = aggregateKey(aggregateId, aggregateIndex)

        val aggregate = try {
            dataStore.get(aggregateKey)
        } catch (ex: EntityNotFoundException) {
            return GetEventsResponse.AggregateNotFound
        }

        @Suppress("UNCHECKED_CAST")
        val aggregateEvents = aggregate.getProperty(eventsProperty) as List<String>
        val currentVersion = aggregate.getProperty(versionProperty) as Long
        val aggregateType = aggregate.getProperty(aggregateTypeProperty) as String

        val events = aggregateEvents.map {
            messageFormat.parse<EventModel>(ByteArrayInputStream(it.toByteArray(Charsets.UTF_8)), EventModel::class.java)
        }.map { EventPayload(it.kind, it.timestamp, it.identityId, Binary(it.payload.toByteArray(Charsets.UTF_8))) }

        return GetEventsResponse.Success(aggregateId, aggregateType, snapshot, currentVersion, events)
    }

    override fun revertLastEvents(aggregateId: String, count: Int): RevertEventsResponse {
        if (count == 0) {
            throw IllegalArgumentException("trying to revert zero events")
        }

        var transaction: Transaction? = null
        try {
            val dataStore = DatastoreServiceFactory.getDatastoreService()
            transaction = dataStore.beginTransaction(TransactionOptions.Builder.withXG(true))

            val snapshotKey = KeyFactory.createKey(snapshotKind, aggregateId)

            try {
                val snapshot = dataStore.get(transaction, snapshotKey)
                val aggregateIndex = snapshot.getProperty("aggregateIndex") as Long? ?: 0

                val aggregateKey = aggregateKey(aggregateId, aggregateIndex)

                val aggregateEntity = dataStore.get(transaction, aggregateKey)

                @Suppress("UNCHECKED_CAST")
                val aggregateEvents = aggregateEntity.getProperty(eventsProperty) as MutableList<String>
                val currentVersion = aggregateEntity.getProperty(versionProperty) as Long

                if (count > aggregateEvents.size) {
                    return RevertEventsResponse.ErrorNotEnoughEventsToRevert
                }

                val lastEventIndex = aggregateEvents.size - count
                val updatedEvents = aggregateEvents.filterIndexed { index, _ -> index < lastEventIndex }

                aggregateEntity.setUnindexedProperty(eventsProperty, updatedEvents)
                aggregateEntity.setUnindexedProperty(versionProperty, currentVersion - count)

                dataStore.put(transaction, listOf(snapshot, aggregateEntity))
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

    private fun aggregateKey(aggregateId: String, aggregateIndex: Long) =
            KeyFactory.createKey(kind, "${aggregateId}_$aggregateIndex")
}