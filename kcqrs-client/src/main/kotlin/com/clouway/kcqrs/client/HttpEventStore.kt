package com.clouway.kcqrs.client

import com.clouway.kcqrs.core.Aggregate
import com.clouway.kcqrs.core.Binary
import com.clouway.kcqrs.core.EventPayload
import com.clouway.kcqrs.core.EventStore
import com.clouway.kcqrs.core.GetAllEventsRequest
import com.clouway.kcqrs.core.GetAllEventsResponse
import com.clouway.kcqrs.core.GetEventsFromStreamsRequest
import com.clouway.kcqrs.core.GetEventsResponse
import com.clouway.kcqrs.core.IndexedEvent
import com.clouway.kcqrs.core.Position
import com.clouway.kcqrs.core.RevertEventsResponse
import com.clouway.kcqrs.core.SaveEventsRequest
import com.clouway.kcqrs.core.SaveEventsResponse
import com.clouway.kcqrs.core.SaveOptions
import com.clouway.kcqrs.core.Snapshot
import com.clouway.kcqrs.core.StreamKey
import com.google.api.client.http.GenericUrl
import com.google.api.client.http.HttpRequestFactory
import com.google.api.client.http.HttpStatusCodes
import com.google.api.client.http.json.JsonHttpContent
import com.google.api.client.json.GenericJson
import com.google.api.client.json.gson.GsonFactory
import com.google.api.client.util.Key
import java.io.IOException
import java.net.URL
import java.net.URLEncoder
import java.util.Arrays

/**
 * HttpEventStore is an implementation of EventStore which uses REST api for storing and retrieving of events.
 *
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class HttpEventStore(
	private val endpoint: URL,
	private val requestFactory: HttpRequestFactory,
	private val timeout: Int = 60000
) : EventStore {
	
	override fun saveEvents(request: SaveEventsRequest, saveOptions: SaveOptions): SaveEventsResponse {
		val key = StreamKey.from(request.stream)
		val aggregateId = key.aggregateId
		val aggregateType = request.aggregateType
		
		val requestEvents =
			request.events.map {
				EventPayloadDto(it.kind, it.timestamp, it.identityId, it.data.payload.toString(Charsets.UTF_8))
			}
		
		try {
			var snapshotDto: SnapshotDto? = null
			if (saveOptions.createSnapshot.required && saveOptions.createSnapshot.snapshot != null) {
				snapshotDto = SnapshotDto(
					saveOptions.createSnapshot.snapshot!!.version,
					BinaryDto(saveOptions.createSnapshot.snapshot!!.data.payload)
				)
			}
			val req = requestFactory.buildPostRequest(
				GenericUrl("$endpoint/v1/aggregates"),
				JsonHttpContent(
					GsonFactory.getDefaultInstance(),
					SaveEventsRequestDto(
						aggregateId,
						aggregateType,
						saveOptions.version,
						saveOptions.topicName,
						requestEvents,
						saveOptions.createSnapshot.required,
						snapshotDto
					)
				)
			).setConnectTimeout(timeout).setReadTimeout(timeout)
			
			req.throwExceptionOnExecuteError = false
			
			val response = req.execute()
			
			if (response.isSuccessStatusCode) {
				val resp = response.parseAs(SaveEventsResponseDto::class.java)
				return SaveEventsResponse.Success(resp.version, resp.sequenceIds, Aggregate(aggregateType, null, resp.version, resp.aggregate.events.map { EventPayload(aggregateId, it.kind, it.timestamp, it.identityId, Binary(it.payload)) }))
			}
			
			if (response.statusCode == HttpStatusCodes.STATUS_CODE_CONFLICT) {
				val resp = response.parseAs(SaveEventsResponseDto::class.java)
				return SaveEventsResponse.EventCollision(resp.version)
			}
			
			if (response.statusCode == HttpStatusCodes.STATUS_CODE_UNPROCESSABLE_ENTITY) {
				val resp = response.parseAs(SnapshotRequiredDto::class.java)
				var snapshot: Snapshot? = null
				if (resp.currentSnapshot != null) {
					snapshot = Snapshot(resp.currentSnapshot!!.version, Binary(resp.currentSnapshot!!.data!!.payload))
				}
				return SaveEventsResponse.SnapshotRequired(resp.currentEvents.map {
					EventPayload(
						aggregateId,
						it.kind,
						it.timestamp,
						it.identityId,
						Binary(it.payload),
					)
				}, snapshot, resp.version)
			}
			
			if (response.statusCode == HttpStatusCodes.STATUS_CODE_BAD_GATEWAY) {
				return SaveEventsResponse.Error("Unable to publish event")
			}
			
		} catch (ex: IOException) {
			return SaveEventsResponse.ErrorInCommunication(ex.message!!)
		}
		return SaveEventsResponse.Error("Generic Error")
	}
	
	override fun getEventsFromStreams(request: GetEventsFromStreamsRequest): GetEventsResponse {
		val streams = request.streams
		val keys = streams.map { StreamKey.from(it) }
		val aggregateIds = keys.map { it.aggregateId }
		
		val httpRequest =
			requestFactory.buildGetRequest(GenericUrl("$endpoint/v2/aggregates?ids=${aggregateIds.joinToString(",")}&aggregateType=${keys[0].aggregateType}"))
				.setConnectTimeout(timeout).setReadTimeout(timeout)
		httpRequest.headers.set("Accept-version", "v2")
		httpRequest.throwExceptionOnExecuteError = false
		try {
			val response = httpRequest.execute()
			
			// Aggregate was not found and no events cannot be returned
			if (response.statusCode == HttpStatusCodes.STATUS_CODE_NOT_FOUND) {
				return GetEventsResponse.AggregateNotFound(aggregateIds = aggregateIds, aggregateType = keys[0].aggregateType)
			}
			
			if (response.isSuccessStatusCode) {
				val resp = response.parseAs(GetEventsResponseDto::class.java)
				
				val aggregates = resp.aggregates.map {
					adapt(it.aggregateId, it)
				}
				
				return GetEventsResponse.Success(aggregates)
			}
			
			return GetEventsResponse.Error("got unknown error")
			
		} catch (ex: IOException) {
			return GetEventsResponse.ErrorInCommunication(ex.message!!)
		}
	}
	
	
	override fun getAllEvents(request: GetAllEventsRequest): GetAllEventsResponse {
		val all = URLEncoder.encode("\$all", "UTF-8")
		
		val streams = request.streams.joinToString(separator = ",")
		
		val streamsParam = if (streams.isNotEmpty()) "&streams=$streams" else ""
		
		val url = endpoint.toString() + "/v2/aggregates/$all?fromPosition=${
			request.position?.value
				?: 0
		}&maxCount=${request.maxCount}&readDirection=${request.readDirection.name}$streamsParam"
		val req = requestFactory.buildGetRequest(GenericUrl(url))
			.setConnectTimeout(timeout).setReadTimeout(timeout)
		req.headers.set("Accept-version", "v2")
		req.throwExceptionOnExecuteError = false
		try {
			val response = req.execute()
			
			// Aggregate was not found and no events cannot be returned
			if (response.statusCode == HttpStatusCodes.STATUS_CODE_NOT_FOUND) {
				return GetAllEventsResponse.Success(listOf(), request.readDirection, null)
			}
			
			if (response.isSuccessStatusCode) {
				val resp = response.parseAs(GetAllEventsResponseDto::class.java)
				val events = resp.events.map {
					IndexedEvent(
						Position(it.position),
						it.tenant,
						it.aggregateType,
						it.version,
						EventPayload(
							"",
							it.payload.kind,
							it.payload.timestamp,
							it.payload.identityId,
							Binary(it.payload.payload)
						)
					)
				}
				
				return GetAllEventsResponse.Success(events, request.readDirection, Position(resp.nextPosition ?: 0))
			}
			
			return GetAllEventsResponse.Error("got unknown error")
			
		} catch (ex: IOException) {
			return GetAllEventsResponse.ErrorInCommunication(ex.message!!)
		}
	}
	
	override fun revertLastEvents(tenant: String, stream: String, count: Int): RevertEventsResponse {
		val request = requestFactory.buildPatchRequest(
			GenericUrl("$endpoint/v1/aggregates/$stream?&aggregateType=$tenant"),
			JsonHttpContent(GsonFactory.getDefaultInstance(), RevertEventsRequestDto(stream, count))
		).setConnectTimeout(timeout).setReadTimeout(timeout)
		request.headers.set("Accept-version", "v2")
		request.throwExceptionOnExecuteError = false
		try {
			val response = request.execute()
			
			// Aggregate was not found, so no events cannot be returned
			if (response.statusCode == HttpStatusCodes.STATUS_CODE_NOT_FOUND) {
				return RevertEventsResponse.AggregateNotFound(stream, tenant)
			}
			
			if (response.isSuccessStatusCode) {
				return RevertEventsResponse.Success(listOf())
			}
			
			return RevertEventsResponse.Error("Generic Error")
			
		} catch (ex: IOException) {
			return RevertEventsResponse.Error("Communication Error")
		}
	}
	
}

private fun adapt(aggregateId: String, it: AggregateDto): Aggregate {
	var snapshot: Snapshot? = null
	if (it.snapshot != null) {
		snapshot = Snapshot(it.snapshot!!.version, Binary(it.snapshot!!.data!!.payload))
	}
	return Aggregate(
		it.aggregateType,
		snapshot,
		it.version,
		it.events.map { event ->
			EventPayload(
				aggregateId,
				event.kind,
				event.timestamp,
				event.identityId,
				Binary(event.payload)
			)
		}
	)
}

internal data class GetAllEventsResponseDto(
	@Key @JvmField var events: List<IndexedEventDto>,
	@Key @JvmField var readDirection: String?,
	@Key @JvmField var nextPosition: Long?
) : GenericJson() {
	@Suppress("UNUSED")
	constructor() : this(mutableListOf(), null, 0L)
}

internal data class IndexedEventDto(
	@Key @JvmField var position: Long,
	@Key @JvmField var tenant: String,
	@Key @JvmField var aggregateType: String,
	@Key @JvmField var version: Long,
	@Key @JvmField var payload: EventPayloadDto
) : GenericJson() {
	@Suppress("UNUSED")
	constructor() : this(0L, "", "", 0L, EventPayloadDto("", 0L, "", ""))
}


internal data class GetEventsResponseDto(@Key @JvmField var aggregates: List<AggregateDto>) : GenericJson() {
	@Suppress("UNUSED")
	constructor() : this(mutableListOf())
}

internal data class AggregateDto(
	@Key @JvmField var aggregateId: String,
	@Key @JvmField var aggregateType: String,
	@Key @JvmField var snapshot: SnapshotDto?,
	@Key @JvmField var version: Long,
	@Key @JvmField var topic: String,
	@Key @JvmField var events: List<EventPayloadDto>
) {
	constructor() : this("", "", null, 0L, "", listOf())
}

internal data class EventPayloadDto(
	@Key @JvmField var kind: String,
	@Key @JvmField var timestamp: Long,
	@Key @JvmField var identityId: String,
	@Key @JvmField var payload: String
) : GenericJson() {
	@Suppress("UNUSED")
	constructor() : this("", 0L, "", "")
}

internal data class SaveEventsRequestDto(
	@Key @JvmField var aggregateId: String,
	@Key @JvmField var aggregateType: String,
	@Key @JvmField var version: Long,
	@Key @JvmField var topicName: String,
	@Key @JvmField var events: List<EventPayloadDto>,
	@Key @JvmField var snapshotRequired: Boolean,
	@Key @JvmField var snapshot: SnapshotDto?
) : GenericJson() {
	@Suppress("UNUSED")
	constructor() : this("", "", 0L, "", mutableListOf(), false, null)
}

internal data class SaveEventsResponseDto(
	@Key @JvmField var aggregateId: String,
	@Key @JvmField var version: Long,
	@Key @JvmField var sequenceIds: List<Long>,
	@Key @JvmField var aggregate: AggregateDto
) : GenericJson() {
	@Suppress("UNUSED")
	constructor() : this("", 0L, listOf(), AggregateDto())
}

internal data class RevertEventsRequestDto(@Key @JvmField var aggregateId: String, @Key @JvmField var count: Int) :
	GenericJson() {
	@Suppress("UNUSED")
	constructor() : this("", 0)
}

internal data class SnapshotRequiredDto(
	@Key @JvmField var currentEvents: List<EventPayloadDto>,
	@Key @JvmField var currentSnapshot: SnapshotDto? = null,
	@Key @JvmField var version: Long
) : GenericJson() {
	@Suppress("UNUSED")
	constructor() : this(listOf(), null, 0L)
}

internal data class SnapshotDto(@Key @JvmField var version: Long, @Key @JvmField var data: BinaryDto?) : GenericJson() {
	@Suppress("UNUSED")
	constructor() : this(0, null)
}

internal data class BinaryDto(@Key @JvmField var payload: ByteArray) : GenericJson() {
	override fun equals(other: Any?): Boolean {
		if (this === other) return true
		if (javaClass != other?.javaClass) return false
		if (!super.equals(other)) return false
		
		other as BinaryDto
		
		if (!Arrays.equals(payload, other.payload)) return false
		
		return true
	}
	
	override fun hashCode(): Int {
		var result = super.hashCode()
		result = 31 * result + Arrays.hashCode(payload)
		return result
	}
	
	@Suppress("UNUSED")
	constructor() : this(ByteArray(0))
}

