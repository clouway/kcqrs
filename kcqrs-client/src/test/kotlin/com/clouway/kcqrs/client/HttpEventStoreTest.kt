package com.clouway.kcqrs.client

import com.clouway.kcqrs.core.Aggregate
import com.clouway.kcqrs.core.Binary
import com.clouway.kcqrs.core.CreateSnapshot
import com.clouway.kcqrs.core.EventPayload
import com.clouway.kcqrs.core.GetAllEventsRequest
import com.clouway.kcqrs.core.GetAllEventsResponse
import com.clouway.kcqrs.core.GetEventsFromStreamsRequest
import com.clouway.kcqrs.core.GetEventsResponse
import com.clouway.kcqrs.core.IndexedEvent
import com.clouway.kcqrs.core.Position
import com.clouway.kcqrs.core.ReadDirection
import com.clouway.kcqrs.core.RevertEventsResponse
import com.clouway.kcqrs.core.SaveEventsRequest
import com.clouway.kcqrs.core.SaveEventsResponse
import com.clouway.kcqrs.core.SaveOptions
import com.clouway.kcqrs.core.Snapshot
import com.google.api.client.http.HttpStatusCodes
import com.google.api.client.http.LowLevelHttpRequest
import com.google.api.client.http.LowLevelHttpResponse
import com.google.api.client.json.gson.GsonFactory
import com.google.api.client.testing.http.MockHttpTransport
import com.google.api.client.testing.http.MockLowLevelHttpRequest
import com.google.api.client.testing.http.MockLowLevelHttpResponse
import org.hamcrest.Matchers.`is`
import org.hamcrest.Matchers.equalTo
import org.hamcrest.Matchers.hasItems
import org.jmock.integration.junit4.JUnitRuleMockery
import org.junit.Assert.assertThat
import org.junit.Rule
import org.junit.Test
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.net.URL
import java.util.UUID


/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class HttpEventStoreTest {
	
	@Rule
	@JvmField
	val context = JUnitRuleMockery()
	
	private val anyBackendEndpoint = URL("http://localhost:8080")
	
	@Test
	fun saveEventsSucceds() {
		val aggregateId = randomAggregateId()
		
		val transport = MockHttpTransport.Builder()
				.setLowLevelHttpResponse(MockLowLevelHttpResponse()
						.setStatusCode(HttpStatusCodes.STATUS_CODE_CREATED)
						.setContent("""
             {
               "version": 4,
               "sequenceIds": [1,2,3],
               "aggregate": {
                        "aggregateType": "Invoice",
                        "version": 4,
                        "events": [
                          {"aggregateId": "123", "kind": "SampleEvent", "timestamp": 123, "identityId": "userid1", "payload": "::payload::"}
                        ]
               }
             }
               """.trimMargin()))
				.build()
		
		val store = HttpEventStore(anyBackendEndpoint, transport.createRequestFactory {
			it.parser = GsonFactory.getDefaultInstance().createJsonObjectParser()
		})
		val response = store.saveEvents(SaveEventsRequest("tenant1", "", "Invoice", listOf(EventPayload(aggregateId, "::kind::", 1L, "::user::", Binary("::event data::"))))) as SaveEventsResponse.Success
		
		assertThat(response, `is`(equalTo(SaveEventsResponse.Success(
				version = 4,
				sequenceIds = listOf(1L, 2L, 3L),
				aggregate = Aggregate(
						"Invoice",
						null,
						4L,
						listOf(
								EventPayload(aggregateId = "123", kind = "SampleEvent", timestamp = 123, identityId = "userid1", data = Binary("::payload::"))
						)
				)
		))))
	}
	
	@Test
	fun saveEventsPayloadIsSendToTheServer() {
		val aggregateId = randomAggregateId()
		
		val transport = MockHttpTransport.Builder()
				.setLowLevelHttpRequest(
						MockLowLevelHttpRequest()
								.setResponse(MockLowLevelHttpResponse()
										.setStatusCode(HttpStatusCodes.STATUS_CODE_NOT_FOUND)
								)
				).build()
		
		val store = HttpEventStore(anyBackendEndpoint, transport.createRequestFactory {
			it.parser = GsonFactory.getDefaultInstance().createJsonObjectParser()
		})
		
		store.saveEvents(SaveEventsRequest("tenant1", "Invoice_$aggregateId", "Invoice", listOf(EventPayload(aggregateId, "::kind::", 1L, "::user::", Binary("::event data::")))), SaveOptions(version = 1L, topicName = "crm"))
		
		val outputStream = ByteArrayOutputStream()
		transport.lowLevelHttpRequest.streamingContent.writeTo(outputStream)
		
		assertThat(outputStream.toString(), `is`(equalTo(
				"""{"aggregateType":"Invoice","events":[{"aggregateId":"$aggregateId","identityId":"::user::","kind":"::kind::","payload":"::event data::","timestamp":1}],"snapshotRequired":false,"stream":"Invoice_$aggregateId","tenant":"tenant1","topicName":"crm","version":1}""".trimIndent()
		)))
	}
	
	@Test
	fun saveEventsWithSnapshot() {
		val aggregateId = randomAggregateId()
		
		val transport = MockHttpTransport.Builder()
				.setLowLevelHttpRequest(
						MockLowLevelHttpRequest()
								.setResponse(MockLowLevelHttpResponse()
										.setStatusCode(HttpStatusCodes.STATUS_CODE_NOT_FOUND)
								)
				).build()
		
		val store = HttpEventStore(anyBackendEndpoint, transport.createRequestFactory {
			it.parser = GsonFactory.getDefaultInstance().createJsonObjectParser()
		})
		
		store.saveEvents(SaveEventsRequest("tenant1", "Invoice_$aggregateId", "Invoice", listOf(EventPayload(aggregateId, "::kind::", 1L, "::user::", Binary("::event data::")))), SaveOptions(version = 1L, topicName = "crm", createSnapshot = CreateSnapshot(true, Snapshot(0, Binary("data")))))
		
		val outputStream = ByteArrayOutputStream()
		transport.lowLevelHttpRequest.streamingContent.writeTo(outputStream)
		
		assertThat(outputStream.toString(), `is`(equalTo(
				"""{"aggregateType":"Invoice","events":[{"aggregateId":"$aggregateId","identityId":"::user::","kind":"::kind::","payload":"::event data::","timestamp":1}],"snapshot":{"data":{"payload":[100,97,116,97]},"version":0},"snapshotRequired":true,"stream":"Invoice_$aggregateId","tenant":"tenant1","topicName":"crm","version":1}""".trimIndent()
		)))
	}
	
	@Test
	fun unableToCommunicateWithTheBackend() {
		val aggregateId = randomAggregateId()
		
		val transport = MockHttpTransport.Builder()
				.setLowLevelHttpRequest(object : MockLowLevelHttpRequest() {
					override fun execute(): LowLevelHttpResponse {
						throw IOException("unable to connect")
					}
				})
				.build()
		
		val store = HttpEventStore(anyBackendEndpoint, transport.createRequestFactory {
			it.parser = GsonFactory.getDefaultInstance().createJsonObjectParser()
		})
		
		val response = store.saveEvents(
				SaveEventsRequest(
						"tenant1",
						"",
						"Order",
						listOf(EventPayload(aggregateId, "::kind::", 1L, "::user id::", Binary("::event data::")))
				),
				SaveOptions(aggregateId)) as SaveEventsResponse.ErrorInCommunication
		
		assertThat(response, `is`(equalTo(SaveEventsResponse.ErrorInCommunication("unable to connect"))))
	}
	
	@Test
	fun unableToPublishEvent() {
		val aggregateId = randomAggregateId()
		
		val transport = MockHttpTransport.Builder()
				.setLowLevelHttpRequest(MockLowLevelHttpRequest()
						.setResponse(MockLowLevelHttpResponse()
								.setStatusCode(HttpStatusCodes.STATUS_CODE_BAD_GATEWAY)
						))
				.build()
		
		val store = HttpEventStore(anyBackendEndpoint, transport.createRequestFactory {
			it.parser = GsonFactory.getDefaultInstance().createJsonObjectParser()
		})
		
		val response = store.saveEvents(
				SaveEventsRequest(
						"::tenant 1",
						"",
						"Order",
						listOf(EventPayload(aggregateId, "::kind::", 1L, "::user id::", Binary("::event data::")))
				)
		) as SaveEventsResponse.Error
		
		assertThat(response, `is`(equalTo(SaveEventsResponse.Error("Unable to publish event"))))
	}
	
	@Test
	fun savingOfEventsFails() {
		val transport = MockHttpTransport.Builder()
				.setLowLevelHttpResponse(MockLowLevelHttpResponse()
						.setStatusCode(HttpStatusCodes.STATUS_CODE_SERVER_ERROR)
				).build()
		
		val store = HttpEventStore(anyBackendEndpoint, transport.createRequestFactory {
			it.parser = GsonFactory.getDefaultInstance().createJsonObjectParser()
		})
		
		val response = store.saveEvents(
				SaveEventsRequest(
						"tenant1",
						"",
						"InventoryItem",
						listOf(EventPayload("::kind::", "::event data::"))
				)
		) as SaveEventsResponse.Error
		
		assertThat(response, `is`(equalTo(SaveEventsResponse.Error("Generic Error"))))
	}
	
	@Test
	fun concurrentSavingOfEvents() {
		val aggregateId = randomAggregateId()
		
		val transport = MockHttpTransport.Builder()
				.setLowLevelHttpResponse(MockLowLevelHttpResponse()
						.setStatusCode(HttpStatusCodes.STATUS_CODE_CONFLICT)
						.setContent("""{"aggregateId": "$aggregateId","version": 5}""")
				).build()
		
		val store = HttpEventStore(anyBackendEndpoint, transport.createRequestFactory {
			it.parser = GsonFactory.getDefaultInstance().createJsonObjectParser()
		})
		
		val response = store.saveEvents(SaveEventsRequest("::teanant::", "", "Order", listOf(EventPayload(aggregateId, "::event kind::", 1L, "::user id::", Binary("::event data::")))), SaveOptions(version = 4)) as SaveEventsResponse.EventCollision
		assertThat(response, `is`(equalTo(SaveEventsResponse.EventCollision(5))))
	}
	
	@Test
	fun retrieveAggregateWithMultipleEvents() {
		val aggregateId = randomAggregateId()
		
		val responsePayload = """
            {"aggregates": [
                  {
                    "aggregateType": "Invoice",
                    "version": 4,
                    "events": [
                        {"aggregateId": "$aggregateId", "kind": "::kind 1::","timestamp": 1,"version": 1, "identityId":"::user::", "payload": "::event data::"},
                        {"aggregateId": "$aggregateId", "kind": "::kind 2::","timestamp": 2,"version": 2, "identityId":"::user::", "payload": "::event data::"},
                        {"aggregateId": "$aggregateId", "kind": "::kind 3::","timestamp": 3,"version": 3, "identityId":"::user::", "payload": "::event data::"}
                    ]
                  }
              ]
            }
            """.trimIndent()
		
		val transport = MockHttpTransport.Builder()
				.setLowLevelHttpResponse(MockLowLevelHttpResponse()
						.setStatusCode(HttpStatusCodes.STATUS_CODE_OK)
						.setContent(responsePayload))
				.build()
		
		val store = HttpEventStore(anyBackendEndpoint, transport.createRequestFactory {
			it.parser = GsonFactory.getDefaultInstance().createJsonObjectParser()
		})
		
		val result = store.getEventsFromStreams(GetEventsFromStreamsRequest("tenant1", "Invoice_$aggregateId")) as GetEventsResponse.Success
		assertThat(result.aggregates, hasItems(
				Aggregate(
						"Invoice",
						null,
						4L,
						listOf(
								EventPayload(aggregateId, "::kind 1::", 1L, "::user::", Binary("::event data::")),
								EventPayload(aggregateId, "::kind 2::", 2L, "::user::", Binary("::event data::")),
								EventPayload(aggregateId, "::kind 3::", 3L, "::user::", Binary("::event data::"))
						)
				)))
	}
	
	@Test
	fun retrieveEventsOfUnknownStream() {
		val aggregateId = randomAggregateId()
		
		val transport = MockHttpTransport.Builder()
				.setLowLevelHttpResponse(MockLowLevelHttpResponse()
						.setStatusCode(HttpStatusCodes.STATUS_CODE_NOT_FOUND)
						.setContent(""))
				.build()
		
		val store = HttpEventStore(anyBackendEndpoint, transport.createRequestFactory {
			it.parser = GsonFactory.getDefaultInstance().createJsonObjectParser()
		})
		
		store.getEventsFromStreams(GetEventsFromStreamsRequest("tenant1", "Invoice_$aggregateId")) as GetEventsResponse.AggregateNotFound
	}
	
	@Test
	fun retrieveAllEvents() {
		val aggregateId = randomAggregateId()
		
		val responsePayload = """
            {"events": [
                  {
                    "position": 1,
                    "tenant": "tenant1",
                    "aggregateType": "Invoice",
                    "version": 1,
                    "payload":  {"aggregateId": "$aggregateId","kind": "::kind 1::","timestamp": 1,"version": 1, "identityId":"::user::", "payload": "::event data::"}
                  },
                  {
                    "position": 2,
                    "tenant": "tenant1",
                    "aggregateType": "Invoice",
                    "version": 1,
                    "payload":  {"aggregateId": "$aggregateId","kind": "::kind 2::","timestamp": 2,"version": 2, "identityId":"::user::", "payload": "::event data::"}
                  },
                  {
                    "position": 3,
                    "tenant": "tenant1",
                    "aggregateType": "Invoice",
                    "version": 1,
                    "payload":  {"aggregateId": "$aggregateId","kind": "::kind 3::","timestamp": 3,"version": 3, "identityId":"::user::", "payload": "::event data::"}
                  }
              ],
              "readDirection": "FORWARD",
              "nextPosition": 4
            }
            """.trimIndent()
		
		val transport = MockHttpTransport.Builder()
				.setLowLevelHttpResponse(MockLowLevelHttpResponse()
						.setStatusCode(HttpStatusCodes.STATUS_CODE_OK)
						.setContent(responsePayload))
				.build()
		
		val store = HttpEventStore(anyBackendEndpoint, transport.createRequestFactory {
			it.parser = GsonFactory.getDefaultInstance().createJsonObjectParser()
		})
		
		val result = store.getAllEvents(GetAllEventsRequest(null, 5)) as GetAllEventsResponse.Success
		
		assertThat(result.nextPosition, `is`(equalTo(Position(4L))))
		assertThat(result.readDirection, `is`(equalTo(ReadDirection.FORWARD)))
		
		assertThat(result.events, hasItems(
				IndexedEvent(Position(1L), "tenant1", "Invoice", 1L, EventPayload(aggregateId, "::kind 1::", 1L, "::user::", Binary("::event data::"))),
				IndexedEvent(Position(2L), "tenant1", "Invoice", 1L, EventPayload(aggregateId, "::kind 2::", 2L, "::user::", Binary("::event data::"))),
				IndexedEvent(Position(3L), "tenant1", "Invoice", 1L, EventPayload(aggregateId, "::kind 3::", 3L, "::user::", Binary("::event data::")))
		))
	}
	
	@Test
	fun noEventsToRetrieve() {
		val transport = MockHttpTransport.Builder()
				.setLowLevelHttpResponse(MockLowLevelHttpResponse()
						.setStatusCode(HttpStatusCodes.STATUS_CODE_NOT_FOUND)
						.setContent(""))
				.build()
		
		val store = HttpEventStore(anyBackendEndpoint, transport.createRequestFactory {
			it.parser = GsonFactory.getDefaultInstance().createJsonObjectParser()
		})
		
		val result = store.getAllEvents(GetAllEventsRequest(null, 5)) as GetAllEventsResponse.Success
		assertThat(result, `is`(equalTo(GetAllEventsResponse.Success(listOf(), ReadDirection.FORWARD, null))))
	}
	
	
	@Test
	fun getAllEventsWasFailed() {
		val transport = MockHttpTransport.Builder()
				.setLowLevelHttpResponse(MockLowLevelHttpResponse()
						.setStatusCode(HttpStatusCodes.STATUS_CODE_SERVER_ERROR)
						.setContent(""))
				.build()
		
		val store = HttpEventStore(anyBackendEndpoint, transport.createRequestFactory {
			it.parser = GsonFactory.getDefaultInstance().createJsonObjectParser()
		})
		
		val result = store.getAllEvents(GetAllEventsRequest(null, 5)) as GetAllEventsResponse.Error
		
		assertThat(result, `is`(equalTo(GetAllEventsResponse.Error("got unknown error"))))
	}
	
	@Test
	fun getAllEventsPassesRequestParams() {
		val calls = mutableListOf<String>()
		val transport = object : MockHttpTransport() {
			override fun buildRequest(method: String, url: String): LowLevelHttpRequest {
				calls.add("$method:$url")
				return MockLowLevelHttpRequest()
						.setResponse(MockLowLevelHttpResponse()
								.setStatusCode(HttpStatusCodes.STATUS_CODE_SERVER_ERROR)
								.setContent("")
						)
			}
		}
		
		val store = HttpEventStore(anyBackendEndpoint, transport.createRequestFactory {
			it.parser = GsonFactory.getDefaultInstance().createJsonObjectParser()
		})
		
		store.getAllEvents(GetAllEventsRequest(Position(3), 5, ReadDirection.FORWARD))
		
		assertThat(calls, `is`(equalTo(listOf(
				"GET:http://localhost:8080/v2/aggregates/\$all?fromPosition=3&maxCount=5&readDirection=FORWARD"
		))))
	}
	
	
	@Test
	fun getAllEventsPassesRequestParamsWithAggregates() {
		val calls = mutableListOf<String>()
		val transport = object : MockHttpTransport() {
			override fun buildRequest(method: String, url: String): LowLevelHttpRequest {
				calls.add("$method:$url")
				return MockLowLevelHttpRequest()
						.setResponse(MockLowLevelHttpResponse()
								.setStatusCode(HttpStatusCodes.STATUS_CODE_SERVER_ERROR)
								.setContent("")
						)
			}
		}
		
		val store = HttpEventStore(anyBackendEndpoint, transport.createRequestFactory {
			it.parser = GsonFactory.getDefaultInstance().createJsonObjectParser()
		})
		
		store.getAllEvents(GetAllEventsRequest(Position(3), 5, ReadDirection.FORWARD, listOf("Stream1", "Stream2")))
		
		assertThat(calls, `is`(equalTo(listOf(
				"GET:http://localhost:8080/v2/aggregates/\$all?fromPosition=3&maxCount=5&readDirection=FORWARD&streams=Stream1,Stream2"
		))))
	}
	
	@Test
	fun getAllEventsPassesAnotherParams() {
		val calls = mutableListOf<String>()
		val transport = object : MockHttpTransport() {
			override fun buildRequest(method: String, url: String): LowLevelHttpRequest {
				calls.add("$method:$url")
				return MockLowLevelHttpRequest()
						.setResponse(MockLowLevelHttpResponse()
								.setStatusCode(HttpStatusCodes.STATUS_CODE_SERVER_ERROR)
								.setContent("")
						)
			}
		}
		
		val store = HttpEventStore(anyBackendEndpoint, transport.createRequestFactory {
			it.parser = GsonFactory.getDefaultInstance().createJsonObjectParser()
		})
		
		store.getAllEvents(GetAllEventsRequest(null, 4, ReadDirection.FORWARD))
		
		assertThat(calls, `is`(equalTo(listOf(
				"GET:http://localhost:8080/v2/aggregates/\$all?fromPosition=0&maxCount=4&readDirection=FORWARD"
		))))
	}
	
	@Test
	fun getAllEventsFailedWithcommunicationError() {
		val transport = MockHttpTransport.Builder()
				.setLowLevelHttpRequest(object : MockLowLevelHttpRequest() {
					override fun execute(): LowLevelHttpResponse {
						throw IOException("communication error")
					}
				})
				.build()
		
		val store = HttpEventStore(anyBackendEndpoint, transport.createRequestFactory {
			it.parser = GsonFactory.getDefaultInstance().createJsonObjectParser()
		})
		
		store.getAllEvents(GetAllEventsRequest(null, 5)) as GetAllEventsResponse.ErrorInCommunication
	}
	
	@Test
	fun retrieveButNoEvents() {
		val aggregateId = randomAggregateId()
		
		val responsePayload = """
            {"aggregates":
                [
                   {"aggregateId": "$aggregateId","aggregateType": "Order","version": 4, "events": []}
                ]
            }""".trimIndent()
		
		val transport = MockHttpTransport.Builder()
				.setLowLevelHttpResponse(MockLowLevelHttpResponse()
						.setStatusCode(HttpStatusCodes.STATUS_CODE_OK)
						.setContent(responsePayload))
				.build()
		
		val store = HttpEventStore(anyBackendEndpoint, transport.createRequestFactory {
			it.parser = GsonFactory.getDefaultInstance().createJsonObjectParser()
		})
		
		val result = store.getEventsFromStreams(GetEventsFromStreamsRequest("tenant1", "Invoice_$aggregateId")) as GetEventsResponse.Success
		assertThat(result, equalTo(
				GetEventsResponse.Success(listOf(Aggregate("Order", null, 4L, listOf())))
		))
	}
	
	@Test
	fun revertEvents() {
		val aggregateId = randomAggregateId()
		
		val responsePayload = """
                           {"aggregateId": "$aggregateId","version": 4}
                           """.trimIndent()
		
		val transport = MockHttpTransport.Builder()
				.setLowLevelHttpResponse(MockLowLevelHttpResponse()
						.setStatusCode(HttpStatusCodes.STATUS_CODE_OK)
						.setContent(responsePayload))
				.build()
		
		val store = HttpEventStore(anyBackendEndpoint, transport.createRequestFactory {
			it.parser = GsonFactory.getDefaultInstance().createJsonObjectParser()
		})
		
		store.revertLastEvents("Invoice", aggregateId, 3) as RevertEventsResponse.Success
	}
	
	@Test
	fun tryToRevertEventsOfUnknownAggregate() {
		val aggregateId = randomAggregateId()
		
		val transport = MockHttpTransport.Builder()
				.setLowLevelHttpResponse(MockLowLevelHttpResponse()
						.setStatusCode(HttpStatusCodes.STATUS_CODE_NOT_FOUND)
						.setContent(""))
				.build()
		
		val store = HttpEventStore(anyBackendEndpoint, transport.createRequestFactory {
			it.parser = GsonFactory.getDefaultInstance().createJsonObjectParser()
		})
		
		store.revertLastEvents("Invoice", aggregateId, 3) as RevertEventsResponse.AggregateNotFound
	}
	
	@Test
	fun revertPayloadIsSendToTheServer() {
		val aggregateId = randomAggregateId()
		
		val transport = MockHttpTransport.Builder()
				.setLowLevelHttpRequest(
						MockLowLevelHttpRequest()
								.setResponse(MockLowLevelHttpResponse()
										.setStatusCode(HttpStatusCodes.STATUS_CODE_NOT_FOUND)
								)
				).build()
		
		val store = HttpEventStore(anyBackendEndpoint, transport.createRequestFactory {
			it.parser = GsonFactory.getDefaultInstance().createJsonObjectParser()
		})
		
		store.revertLastEvents("Invoice", aggregateId, 3)
		
		val outputStream = ByteArrayOutputStream()
		transport.lowLevelHttpRequest.streamingContent.writeTo(outputStream)
		
		assertThat(outputStream.toString(), `is`(equalTo(
				"""{"aggregateId":"$aggregateId","count":3}""".trimIndent()
		)))
	}
	
	@Test
	fun snapshotIsRequired() {
		val aggregateId = randomAggregateId()
		
		val responsePayload = """
            {"currentEvents":[
                        {"kind": "::kind 1::","timestamp": 1,"version": 1, "identityId":"::user::", "payload": "::event data::"}
                             ],
              "currentSnapshot":{"version":1,"data":{"payload":[100,97,116,97]}
            }""".trimIndent()
		
		val transport = MockHttpTransport.Builder()
				.setLowLevelHttpRequest(
						MockLowLevelHttpRequest()
								.setResponse(MockLowLevelHttpResponse()
										.setStatusCode(HttpStatusCodes.STATUS_CODE_UNPROCESSABLE_ENTITY)
										.setContent(responsePayload)
								)
				).build()
		
		val store = HttpEventStore(anyBackendEndpoint, transport.createRequestFactory {
			it.parser = GsonFactory.getDefaultInstance().createJsonObjectParser()
		})
		
		store.saveEvents(SaveEventsRequest("tenant1", "Invoice_$aggregateId", "Invoice", listOf(EventPayload(aggregateId, "::kind::", 1L, "::user::", Binary("::event data::")))), SaveOptions(version = 1L, topicName = "crm", createSnapshot = CreateSnapshot(true, Snapshot(0, Binary("data")))))
		
		val outputStream = ByteArrayOutputStream()
		transport.lowLevelHttpRequest.streamingContent.writeTo(outputStream)
		
		assertThat(outputStream.toString(), `is`(equalTo(
				"""{"aggregateType":"Invoice","events":[{"aggregateId":"$aggregateId","identityId":"::user::","kind":"::kind::","payload":"::event data::","timestamp":1}],"snapshot":{"data":{"payload":[100,97,116,97]},"version":0},"snapshotRequired":true,"stream":"Invoice_$aggregateId","tenant":"tenant1","topicName":"crm","version":1}""".trimIndent()
		)))
	}
	
	@Test
	fun snapshotIsRequiredWithoutCurrentSnapshot() {
		val aggregateId = randomAggregateId()
		
		val responsePayload = """
            {"currentEvents":[
                        {"kind": "::kind 1::","timestamp": 1,"version": 1, "identityId":"::user::", "payload": "::event data::"}
                             ]
            }""".trimIndent()
		
		val transport = MockHttpTransport.Builder()
				.setLowLevelHttpRequest(
						MockLowLevelHttpRequest()
								.setResponse(MockLowLevelHttpResponse()
										.setStatusCode(HttpStatusCodes.STATUS_CODE_UNPROCESSABLE_ENTITY)
										.setContent(responsePayload)
								)
				).build()
		
		val store = HttpEventStore(anyBackendEndpoint, transport.createRequestFactory {
			it.parser = GsonFactory.getDefaultInstance().createJsonObjectParser()
		})
		
		store.saveEvents(SaveEventsRequest("tenant1", "Invoice_$aggregateId", "Invoice", listOf(EventPayload(aggregateId, "::kind::", 1L, "::user::", Binary("::event data::")))), SaveOptions(version = 1L, topicName = "crm", createSnapshot = CreateSnapshot(true, Snapshot(0, Binary("")))))
		
		val outputStream = ByteArrayOutputStream()
		transport.lowLevelHttpRequest.streamingContent.writeTo(outputStream)
		
		assertThat(outputStream.toString(), `is`(equalTo(
				"""{"aggregateType":"Invoice","events":[{"aggregateId":"$aggregateId","identityId":"::user::","kind":"::kind::","payload":"::event data::","timestamp":1}],"snapshot":{"data":{"payload":[]},"version":0},"snapshotRequired":true,"stream":"Invoice_$aggregateId","tenant":"tenant1","topicName":"crm","version":1}""".trimIndent()
		)))
	}
	
	private fun randomAggregateId() = UUID.randomUUID().toString()
}