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
     * @param events a list of events to be saved
     * @param saveOptions saving options
     * @throws EventCollisionException is thrown in case
     */
    fun saveEvents(aggregateType: String, events: List<EventPayload>, saveOptions: SaveOptions = SaveOptions(version = 0L)): SaveEventsResponse

    /**
     * Retrieves the events that are saved for the stored aggregate state.
     *
     * @param aggregateId the ID of the aggregate which events should be retrieved
     * @return a response of events
     */
    fun getEvents(aggregateId: String): GetEventsResponse

    /**
     * Reverts last events that are stored for the aggregate.
     */
    fun revertLastEvents(aggregateId: String, count: Int): RevertEventsResponse

}

data class SaveOptions(var aggregateId: String = "", val version: Long = 0L, val topicName: String = "") {
    init {
        if (aggregateId == "") {
            aggregateId = UUID.randomUUID().toString()
        }
    }
}

/**
 * SaveEventsDataResponse is representing the returned result of saving of the events.
 */
sealed class SaveEventsResponse {
    /**
     * Returned when save operation was successfully executed.
     */
    data class Success(val aggregateId: String, val version: Long) : SaveEventsResponse()

    /**
     * Returned when concurrent modification of the aggregate was executed and update was failed due collision.
     */
    data class EventCollision(val aggregateId: String, val expectedVersion: Long) : SaveEventsResponse()

    /**
     * Returned when error was encountered.
     */
    data class Error(val message: String) : SaveEventsResponse()

    /**
     * Returned when there is an communication error
     */
    object ErrorInCommunication : SaveEventsResponse()
}

sealed class GetEventsResponse {

    data class Success(val aggregateId: String, val aggregateType: String, val snapshot: Snapshot?, val version: Long, val events: List<EventPayload>) : GetEventsResponse()

    object SnapshotNotFound : GetEventsResponse()

    object AggregateNotFound : GetEventsResponse()

    data class Error(val message: String) : GetEventsResponse()

    /**
     * Returned when there is an communication error
     */
    object ErrorInCommunication : GetEventsResponse()

}

sealed class RevertEventsResponse {

    object Success : RevertEventsResponse()

    object AggregateNotFound : RevertEventsResponse()

    object ErrorNotEnoughEventsToRevert : RevertEventsResponse()

    data class Error(val message: String) : RevertEventsResponse()
}


data class Snapshot(private val version: Long, private val data: Binary)

data class EventPayload(val kind: String, val timestamp: Long, val identityId: String, val data: Binary) {
    constructor(kind: String, payload: String) : this(kind, 0, "", Binary(payload.toByteArray(Charsets.UTF_8)))
}

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