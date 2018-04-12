package com.clouway.kcqrs.client

import com.clouway.kcqrs.core.*
import com.google.api.client.http.GenericUrl
import com.google.api.client.http.HttpRequestFactory
import com.google.api.client.http.HttpStatusCodes
import com.google.api.client.http.json.JsonHttpContent
import com.google.api.client.json.GenericJson
import com.google.api.client.json.gson.GsonFactory
import com.google.api.client.util.Key
import java.io.IOException
import java.net.URL

/**
 * HttpEventStore is an implementation of EventStore which uses REST api for storing and retrieving of events.
 *
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class HttpEventStore(private val endpoint: URL,
                     private val requestFactory: HttpRequestFactory) : EventStore {

    override fun saveEvents(aggregateType: String, events: List<EventPayload>, saveOptions: SaveOptions): SaveEventsResponse {
        val aggregateId = saveOptions.aggregateId

        val requestEvents = events.map { EventPayloadDto(it.kind, it.timestamp, it.identityId, it.data.payload.toString(Charsets.UTF_8)) }

        try {
            val request = requestFactory.buildPostRequest(
                    GenericUrl(endpoint.toString() + "/v1/aggregates"),
                    JsonHttpContent(GsonFactory.getDefaultInstance(), SaveEventsRequestDto(aggregateId, aggregateType, saveOptions.version, saveOptions.topicName, requestEvents))
            )
            request.throwExceptionOnExecuteError = false

            val response = request.execute()

            if (response.isSuccessStatusCode) {
                val resp = response.parseAs(SaveEventsResponseDto::class.java)
                return SaveEventsResponse.Success(aggregateId, resp.version)
            }

            if (response.statusCode == HttpStatusCodes.STATUS_CODE_CONFLICT) {
                val resp = response.parseAs(SaveEventsResponseDto::class.java)
                return SaveEventsResponse.EventCollision(aggregateId, resp.version)
            }

        } catch (ex: IOException) {
            return SaveEventsResponse.ErrorInCommunication
        }

        return SaveEventsResponse.Error("Generic Error")
    }

    override fun getEvents(aggregateId: String): GetEventsResponse {
        val request = requestFactory.buildGetRequest(GenericUrl(endpoint.toString() + "/v1/aggregates/$aggregateId"))
        request.throwExceptionOnExecuteError = false
        try {
            val response = request.execute()

            // Aggregate was not found and no events cannot be returned
            if (response.statusCode == HttpStatusCodes.STATUS_CODE_NOT_FOUND) {
                return GetEventsResponse.AggregateNotFound
            }

            if (response.isSuccessStatusCode) {
                val resp = response.parseAs(GetEventsResponseDto::class.java)

                val events = resp.events.map { EventPayload(it.kind, it.timestamp, it.identityId, Binary(it.payload)) }

                return GetEventsResponse.Success(aggregateId, resp.aggregateType, null, resp.version, events)
            }

            return GetEventsResponse.Error("got unknown error")

        } catch (ex: IOException) {
            return GetEventsResponse.ErrorInCommunication
        }
    }

    override fun revertLastEvents(aggregateId: String, count: Int): RevertEventsResponse {
        val request = requestFactory.buildPatchRequest(
                GenericUrl(endpoint.toString() + "/v1/aggregates/$aggregateId"),
                JsonHttpContent(GsonFactory.getDefaultInstance(), RevertEventsRequestDto(aggregateId, count))
        )
        request.throwExceptionOnExecuteError = false
        try {
            val response = request.execute()

            // Aggregate was not found, so no events cannot be returned
            if (response.statusCode == HttpStatusCodes.STATUS_CODE_NOT_FOUND) {
                return RevertEventsResponse.AggregateNotFound
            }

            if (response.isSuccessStatusCode) {
                return RevertEventsResponse.Success
            }

            return RevertEventsResponse.Error("Generic Error")

        } catch (ex: IOException) {
            return RevertEventsResponse.Error("Communication Error")
        }
    }

}

internal data class GetEventsResponseDto(@Key @JvmField var aggregateId: String, @Key @JvmField var aggregateType: String, @Key @JvmField var version: Long, @Key @JvmField var topic: String, @Key @JvmField var events: List<EventPayloadDto>) : GenericJson() {
    @Suppress("UNUSED")
    constructor() : this("", "", 0L, "", mutableListOf())
}

internal data class EventPayloadDto(@Key @JvmField var kind: String, @Key @JvmField var timestamp: Long, @Key @JvmField var identityId: String, @Key @JvmField var payload: String) : GenericJson() {
    @Suppress("UNUSED")
    constructor() : this("", 0L, "", "")
}

internal data class SaveEventsRequestDto(@Key @JvmField var aggregateId: String, @Key @JvmField var aggregateType: String, @Key @JvmField var version: Long, @Key @JvmField var topicName: String, @Key @JvmField var events: List<EventPayloadDto>) : GenericJson() {
    @Suppress("UNUSED")
    constructor() : this("", "", 0L, "", mutableListOf())
}

internal data class SaveEventsResponseDto(@Key @JvmField var aggregateId: String, @Key @JvmField var version: Long) : GenericJson() {
    @Suppress("UNUSED")
    constructor() : this("", 0L)
}

internal data class RevertEventsRequestDto(@Key @JvmField var aggregateId: String, @Key @JvmField var count: Int) : GenericJson() {
    @Suppress("UNUSED")
    constructor() : this("", 0)
}