package com.clouway.kcqrs.adapter.firestore

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
import com.clouway.kcqrs.core.messages.MessageFormat
import com.google.cloud.firestore.Blob
import com.google.cloud.firestore.DocumentReference
import com.google.cloud.firestore.Firestore
import com.google.cloud.firestore.SetOptions
import com.google.protobuf.ByteString
import java.io.ByteArrayInputStream

/**
 * FirestoreEventStore is a firestore implementation that uses
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class FirestoreEventStore(private val firestore: Firestore,
                          private val messageFormat: DataModelFormat,
                          private val idGenerator: IdGenerator
) : EventStore {

    override fun saveEvents(request: SaveEventsRequest, saveOptions: SaveOptions): SaveEventsResponse {
        val version = saveOptions.version
        val aggregateType = request.aggregateType
        val stream = request.stream

        val call = firestore.runTransaction { transaction ->
            val snapshotRef = firestore.document(
                    snapshotPath(request.tenant, stream)
            )
            val snapshotDoc = transaction.get(snapshotRef).get()
            val aggregateIndex = if (snapshotDoc.exists()) snapshotDoc.getLong("aggregateIndex")!! else 0L

            val aggregateDocPath = aggregatePath(request.tenant, stream, aggregateIndex)
            var eventsDocRef = firestore.document(aggregateDocPath)

            val eventsDoc = transaction.get(eventsDocRef).get()

            val docVersion = if (eventsDoc.exists())
                eventsDoc.getLong("version")
            else
                saveOptions.createSnapshot.snapshot?.version ?: 0L

            /**
             *  If the current version is different than what was hydrated during the state change then we know we
             *  have an event collision. This is a very simple approach and more "business knowledge" can be added
             *  here to handle scenarios where the versions may be different but the state change can still occur.
             */
            if (docVersion != saveOptions.version) {
                return@runTransaction SaveEventsResponse.EventCollision(docVersion!!)
            }

            val events = request.events.mapIndexed { index, it -> EventModel(it.aggregateId, it.kind, docVersion + index + 1, it.identityId, it.timestamp, it.data.payload.toString(Charsets.UTF_8)) }

            val sequenceIds = (1..events.size).map { idGenerator.nextId() }
            val eventIndexes = events.mapIndexed { index, eventModel ->
                val sequenceId = sequenceIds[index]
                mapOf(
                    "s" to sequenceId,
                    "t" to request.tenant,
                    "ai" to eventModel.aggregateId,
                    "at" to aggregateType,
                    "st" to stream,
                    "v" to eventModel.version,
                    "r" to eventsDocRef
                )
            }

            val existingModel = if (!eventsDoc.exists()) {
                EventsModel(listOf())
            } else {
                messageFormat.parse(
                        ByteArrayInputStream(eventsDoc.getString("payload")?.toByteArray(Charsets.UTF_8)),
                        EventsModel::class.java
                )
            }

            var snapshot: Snapshot? = null
            val currentVersion = if (!eventsDoc.exists()) docVersion else eventsDoc.getLong("version")!!
            val eventsModel = if (saveOptions.createSnapshot.required && saveOptions.createSnapshot.snapshot != null) {
                val snapshotData = saveOptions.createSnapshot.snapshot!!.data
                val aggregateVersion = saveOptions.createSnapshot.snapshot!!.version

                transaction.set(snapshotRef, mapOf(
                        "version" to aggregateVersion,
                        "aggregateIndex" to aggregateIndex + 1,
                        "data" to snapshotData.payload.toString(Charsets.UTF_8)
                ))

                snapshot = Snapshot(aggregateIndex + 1, snapshotData)
                
                // Make sure that eventsDocRef is will point the new snapshot as the events
                // needs to be appended to it.
                eventsDocRef = firestore.document(aggregatePath(request.tenant, request.stream, aggregateIndex + 1))
                EventsModel(events)
            } else {
                EventsModel(existingModel.events + events)
            }

            val payloadAsText = messageFormat.formatToString(eventsModel)
            val sizeInBytes = payloadAsText.toByteArray(Charsets.UTF_8).size

            // Current firestore entity size is 1,048,572 bytes and we add buffer of 100kb for the metadata to be sure
            // that document could be stored
            if (sizeInBytes >= 948572) {

                var snapshot: Snapshot? = null
                //if a build snapshot does not exist it would not have the field data filled in.
                if (snapshotDoc.exists()) {
                    val blobData = snapshotDoc.getString("data")!!
                    snapshot = Snapshot(
                            snapshotDoc.getLong("version") ?: 0,
                            Binary(blobData.toByteArray(Charsets.UTF_8))
                    )
                }
                return@runTransaction SaveEventsResponse.SnapshotRequired(adaptEvents(existingModel.events), snapshot, version)
            }
    
           
            if (snapshotDoc.exists() && !saveOptions.createSnapshot.required) {
                val snapshotVersion = snapshotDoc.getLong("version")!!
                val snapshotData = snapshotDoc.getString("data")!!
                snapshot = Snapshot(snapshotVersion, Binary(snapshotData))
            }
    
            val newVersion = currentVersion + events.size
            transaction.set(eventsDocRef, mapOf(
                    "version" to newVersion,
                    "payload" to payloadAsText,
                    "stream" to stream,
                    "aggregateType" to aggregateType
            ))

            eventIndexes.forEach {
                val indexedVersion = it["v"]
                val indexRef = firestore.document("stream_indexes/${request.tenant}_${stream}_${aggregateIndex}_$indexedVersion")
                transaction.set(indexRef, it)
            }

            SaveEventsResponse.Success(version, sequenceIds, Aggregate(aggregateType, snapshot, newVersion, adaptEvents(eventsModel.events)))
        }

        return call.get()
    }

    override fun getEventsFromStreams(request: GetEventsFromStreamsRequest): GetEventsResponse {
        val snapshotKeyToStream = mutableMapOf<DocumentReference, String>()
        val snapshotKeys = request.streams.map {
            val key = firestore.document(snapshotPath(request.tenant, it))
            snapshotKeyToStream[key] = it
            key
        }.toTypedArray()

        val snapshotEntities = firestore.getAll(*snapshotKeys).get()
        val streamToSnapshot = snapshotEntities.map { it.id to it }.toMap()

        val keyToAggregateId = mutableMapOf<String, String>()
        val aggregateRefs = snapshotEntities.map {
            val aggregateId = snapshotKeyToStream[it.reference]!!
            val index = it.getLong("aggregateIndex") ?: 0
            val key = firestore.document(aggregatePath(request.tenant, it.id, index))
            keyToAggregateId[key.path] = aggregateId
            key
        }.toTypedArray()

        val aggregateDocs = firestore.getAll(*aggregateRefs).get()

        val aggregates = mutableListOf<Aggregate>()
        aggregateDocs
                .filter { it.exists() }
                .forEach { aggregate ->
                    var snapshot: Snapshot? = null

                    val stream = aggregate.getString("stream")!!

                    if (streamToSnapshot.containsKey(stream)) {
                        val thisSnapshot = streamToSnapshot[stream]!!

                        if (thisSnapshot.contains("data")) {
                            val version = thisSnapshot.getLong("version") ?: 0
                            val data = thisSnapshot.getString("data")!!
                            snapshot = Snapshot(
                                    version,
                                    Binary(data.toByteArray(Charsets.UTF_8))
                            )
                        }

                    }

                    val currentVersion = aggregate.getLong("version") ?: 0
                    val aggregateType = aggregate.getString("aggregateType")!!
                    val payload = aggregate.getString("payload")

                    val eventsModel = messageFormat.parse<EventsModel>(
                            ByteArrayInputStream(payload?.toByteArray(Charsets.UTF_8)),
                            EventsModel::class.java
                    )

                    val eventsToAggregate = eventsModel.events.groupBy { it.aggregateId }

                    eventsToAggregate.keys.forEach { aggregateId ->
                        val aggregateEvents = eventsToAggregate.getValue(aggregateId)
                        val events = aggregateEvents.sortedBy { it.timestamp }
                        val items = adaptEvents(events)
                        aggregates += Aggregate(aggregateType, snapshot, currentVersion, items)
                    }
                }

        return GetEventsResponse.Success(aggregates)
    }

    override fun getAllEvents(request: GetAllEventsRequest): GetAllEventsResponse {
        val indexQuery = if (request.streams.isEmpty() && request.readDirection == ReadDirection.FORWARD) {
            firestore.collection("stream_indexes")
                    .whereGreaterThan("s", request.position?.value ?: 0)
                    .limit(request.maxCount)
        } else if (request.streams.isEmpty() && request.readDirection == ReadDirection.BACKWARD) {
            firestore.collection("stream_indexes")
                    .whereLessThan("s", request.position?.value ?: 0)
                    .limit(request.maxCount)
        } else {
            firestore.collection("stream_indexes")
                    .whereGreaterThan("s", request.position?.value ?: 0)
                    .whereIn("st", request.streams)
                    .limit(request.maxCount)
        }

        val docIdToIndex = indexQuery.get().get().documents.groupBy { (it.get("r") as DocumentReference) }
        val eventDocs = firestore.getAll(*docIdToIndex.keys.toTypedArray()).get()

        val indexedEvents = mutableListOf<IndexedEvent>()
        var lastSequenceId = 0L

        eventDocs.forEach { eventDoc ->
            val docIndexes = docIdToIndex.getValue(eventDoc.reference)

            val eventsModel = messageFormat.parse<EventsModel>(
                    ByteArrayInputStream(eventDoc.getString("payload")?.toByteArray(Charsets.UTF_8)),
                    EventsModel::class.java
            )
            val items = adaptEvents(eventsModel.events)
            
            docIndexes.forEach { docIndex ->
                val version = docIndex.getLong("v")!!

                val tenant = docIndex.getString("t")!!
                val aggregateType = docIndex.getString("at")!!
                val sequenceId = docIndex.getLong("s")!!

                val versionIndex = version.toInt() - 1
                val eventModel = items[versionIndex]

                val indexedEvent = IndexedEvent(Position(sequenceId), tenant, aggregateType, version, eventModel)
                indexedEvents += indexedEvent

                lastSequenceId = sequenceId
            }
        }

        // Backward direction requires events to be retrieved in reversed order.
        val sortedEvents = if (request.readDirection == ReadDirection.FORWARD) {
            indexedEvents.sortedBy { it.position.value }
        } else {
            indexedEvents.sortedBy { it.position.value }.asReversed()
        }

        return GetAllEventsResponse.Success(sortedEvents, ReadDirection.FORWARD, Position(lastSequenceId))

    }

    override fun revertLastEvents(tenant: String, stream: String, count: Int): RevertEventsResponse {
        if (count == 0) {
            throw IllegalArgumentException("trying to revert zero events")
        }

        val revertCall = firestore.runTransaction { transaction ->
            val snapshotRef = firestore.document(
                    snapshotPath(tenant, stream)
            )

            val snapshotDoc = transaction.get(snapshotRef).get()
            val aggregateIndex = if (snapshotDoc.exists()) snapshotDoc.getLong("aggregateIndex")!! else 0L

            val eventsDocRef = firestore.document(aggregatePath(tenant, stream, aggregateIndex))

            val eventsDoc = transaction.get(eventsDocRef).get()

            if (!eventsDoc.exists()) {
                return@runTransaction RevertEventsResponse.AggregateNotFound(tenant, stream)
            }

            val payload = eventsDoc.getString("payload")

            val eventsModel = messageFormat.parse<EventsModel>(
                    ByteArrayInputStream(payload?.toByteArray(Charsets.UTF_8)),
                    EventsModel::class.java
            )

            if (count > eventsModel.events.size) {
                return@runTransaction RevertEventsResponse.ErrorNotEnoughEventsToRevert(eventsModel.events.size, count)
            }

            val updatedModel = eventsModel.removeLastN(count)
            val updatedPayload = messageFormat.formatToString(updatedModel)

            transaction.set(eventsDocRef, mapOf(
                    "payload" to updatedPayload,
                    "version" to updatedModel.events.size
            ), SetOptions.merge())

            RevertEventsResponse.Success(listOf())
        }

        return revertCall.get()
    }

    private fun snapshotPath(tenant: String, stream: String): String {
        return "tenants/$tenant/snapshots/$stream"
    }

    private fun aggregatePath(tenant: String, stream: String, index: Long): String {
        return "tenants/$tenant/streams/${stream}_${index}"
    }

    private fun adaptEvents(aggregateEvents: List<EventModel>): List<EventPayload> {
        return aggregateEvents.map {
            EventPayload(it.aggregateId, it.kind, it.timestamp, it.identityId, Binary(it.payload.toByteArray(Charsets.UTF_8)))
        }

    }

}