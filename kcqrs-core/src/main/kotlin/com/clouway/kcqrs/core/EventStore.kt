package com.clouway.kcqrs.core

import java.util.*

/**
 * EventStore is an abstraction of the persistence layer of the EventStore.
 *
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
interface EventStore {

    /**
     * Saves the provided list of events in to the aggregate.
     *
     * @param request the request that holds the events that needs to be persisted
     * @param saveOptions saving options
     * @throws EventCollisionException is thrown in case
     */
    fun saveEvents(request: SaveEventsRequest, saveOptions: SaveOptions = SaveOptions(version = 0L)): SaveEventsResponse

    /**
     * Retrieves the events that are saved for the stored aggregate state.
     *
     * @param request the request containing domain reference
     * @return a response of events
     */
    fun getEventsFromStreams(request: GetEventsFromStreamsRequest): GetEventsResponse
    
    /**
     * Gets all events starting from given position of the aggregate.
     */
    fun getAllEvents(request: GetAllEventsRequest): GetAllEventsResponse


    /**
     * Reverts last events that are stored for the aggregate.
     */
    fun revertLastEvents(tenant: String, stream: String, count: Int): RevertEventsResponse

}

data class SaveOptions(var aggregateId: String = "", val version: Long = 0L, val topicName: String = "", val createSnapshot: CreateSnapshot = CreateSnapshot()) {
    init {
        if (aggregateId == "") {
            aggregateId = UUID.randomUUID().toString()
        }
    }
}

data class CreateSnapshot(val required: Boolean = false, val snapshot: Snapshot? = null)

/**
 * SaveEventsRequest is a request that will be used to persist a list of events in their native form.
 */
data class SaveEventsRequest(val tenant: String,
                             val stream: String,
                             val aggregateType: String,
                             val events: List<EventPayload>
)

/**
 * SaveEventsDataResponse is representing the returned result of saving of the events.
 */
sealed class SaveEventsResponse {
    /**
     * Returned when save operation was successfully executed.
     */
    data class Success(val version: Long, val sequenceIds: List<Long>, val aggregate: Aggregate) : SaveEventsResponse()

    /**
     * Returned when concurrent modification of the aggregate was executed and update was failed due collision.
     */
    data class EventCollision(val expectedVersion: Long) : SaveEventsResponse()

    /**
     * Returned when error was encountered.
     */
    data class Error(val message: String) : SaveEventsResponse()

    /**
     * Returned when there is an communication error
     */
    data class ErrorInCommunication(val message: String) : SaveEventsResponse()

    /**
     * Returned when aggregate's current Events persistence limit has been reached.
     */

    data class SnapshotRequired(val currentEvents: List<EventPayload>, val currentSnapshot: Snapshot? = null, val version: Long) : SaveEventsResponse()
}

data class GetEventsFromStreamsRequest(val tenant: String, val streams: List<String>) {
    constructor(tenant: String, stream: String) : this(tenant, listOf(stream))
}


data class GetAllEventsRequest(
        val position: Position? = Position(0),
        val maxCount: Int = 100,
        val readDirection: ReadDirection = ReadDirection.FORWARD,
        val streams: List<String> = listOf()
)

sealed class GetAllEventsResponse {
    data class Success(val events:List<IndexedEvent>, val readDirection: ReadDirection, val nextPosition: Position?) : GetAllEventsResponse()

    data class Error(val message: String) : GetAllEventsResponse()

    data class ErrorInCommunication(val message: String) : GetAllEventsResponse()
}

sealed class GetEventsResponse {

    data class Success(val aggregates: List<Aggregate>) : GetEventsResponse()

    data class SnapshotNotFound(val aggregateId: String, val aggregateType: String) : GetEventsResponse()

    data class AggregateNotFound(val aggregateIds: List<String>, val aggregateType: String) : GetEventsResponse()

    data class Error(val message: String) : GetEventsResponse()
    /**
     * Returned when there is an communication error
     */
    data class ErrorInCommunication(val message: String) : GetEventsResponse()

}

sealed class RevertEventsResponse {

    data class Success(val eventIds: List<String>) : RevertEventsResponse()

    data class AggregateNotFound(val aggregateId: String, val aggregateType: String) : RevertEventsResponse()

    data class ErrorNotEnoughEventsToRevert(val available: Int, val requested: Int) : RevertEventsResponse()

    data class Error(val message: String) : RevertEventsResponse()
}

data class Aggregate(val aggregateType: String, val snapshot: Snapshot?, val version: Long, val events: List<EventPayload>)

data class Snapshot(val version: Long, val data: Binary)

data class EventPayload(val aggregateId: String, val kind: String, val timestamp: Long, val identityId: String, val data: Binary, val author: Author? = null) {
    constructor(kind: String, payload: String) : this("", kind, 0, "", Binary(payload.toByteArray(Charsets.UTF_8)), author = null)
}

data class Author(
    val id: String,
    val email: String,
    val givenName: String,
    val middleName: String,
    val familyName: String,
    val position: String,
    val customerId: String,
    val customerName: String
)

data class Binary(val payload: ByteArray) {

    constructor(payload: String) : this(payload.toByteArray(Charsets.UTF_8))

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Binary

        if (!Arrays.equals(payload, other.payload)) return false

        return true
    }

    override fun hashCode(): Int {
        return Arrays.hashCode(payload)
    }

    override fun toString(): String {
        return "Binary(payload=${payload.toString(Charsets.UTF_8)})"
    }
}

