import com.clouway.kcqrs.client.HttpEventStore
import com.clouway.kcqrs.core.*
import com.google.api.client.http.HttpRequest
import com.google.api.client.http.HttpStatusCodes
import com.google.api.client.http.LowLevelHttpRequest
import com.google.api.client.http.LowLevelHttpResponse
import com.google.api.client.json.gson.GsonFactory
import com.google.api.client.testing.http.MockHttpTransport
import com.google.api.client.testing.http.MockLowLevelHttpRequest
import com.google.api.client.testing.http.MockLowLevelHttpResponse
import org.hamcrest.Matchers
import org.hamcrest.Matchers.`is`
import org.hamcrest.Matchers.equalTo
import org.hamcrest.Matchers.hasItems
import org.jmock.integration.junit4.JUnitRuleMockery
import org.junit.Assert.assertThat
import org.junit.Assert.assertTrue
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
                        .setContent("""{"aggregateId": "$aggregateId","version": 4, "sequenceIds": [1,2,3]}"""))
                .build()

        val store = HttpEventStore(anyBackendEndpoint, transport.createRequestFactory {
            it.parser = GsonFactory.getDefaultInstance().createJsonObjectParser()
        })
        val response = store.saveEvents("Invoice", listOf(EventPayload("::kind::", 1L, "::user::", Binary("::event data::"))), SaveOptions(aggregateId)) as SaveEventsResponse.Success

        assertThat(response, `is`(equalTo(SaveEventsResponse.Success(aggregateId, 4, listOf(1L, 2L, 3L)))))
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

        store.saveEvents("Invoice", listOf(EventPayload("::kind::", 1L, "::user::", Binary("::event data::"))), SaveOptions(aggregateId, 1L, "crm"))

        val outputStream = ByteArrayOutputStream()
        transport.lowLevelHttpRequest.streamingContent.writeTo(outputStream)

        assertThat(outputStream.toString(), `is`(equalTo(
                """{"aggregateId":"$aggregateId","aggregateType":"Invoice","events":[{"identityId":"::user::","kind":"::kind::","payload":"::event data::","timestamp":1}],"snapshotRequired":false,"topicName":"crm","version":1}""".trimIndent()
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

        store.saveEvents("Invoice", listOf(EventPayload("::kind::", 1L, "::user::", Binary("::event data::"))), SaveOptions(aggregateId, 1L, "crm", CreateSnapshot(true, Snapshot(0, Binary("data")))))

        val outputStream = ByteArrayOutputStream()
        transport.lowLevelHttpRequest.streamingContent.writeTo(outputStream)

        assertThat(outputStream.toString(), `is`(equalTo(
                """{"aggregateId":"$aggregateId","aggregateType":"Invoice","events":[{"identityId":"::user::","kind":"::kind::","payload":"::event data::","timestamp":1}],"snapshot":{"data":{"payload":[100,97,116,97]},"version":0},"snapshotRequired":true,"topicName":"crm","version":1}""".trimIndent()
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
                "Order",
                listOf(EventPayload("::kind::", 1L, "::user id::", Binary("::event data::"))),
                SaveOptions(aggregateId)) as SaveEventsResponse.ErrorInCommunication

        assertThat(response, `is`(Matchers.equalTo(SaveEventsResponse.ErrorInCommunication("unable to connect"))))
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
                "Order",
                listOf(EventPayload("::kind::", 1L, "::user id::", Binary("::event data::"))),
                SaveOptions(aggregateId)) as SaveEventsResponse.Error

        assertThat(response, `is`(Matchers.equalTo(SaveEventsResponse.Error("Unable to publish event"))))
    }

    @Test
    fun savingOfEventsFails() {
        val aggregateId = randomAggregateId()

        val transport = MockHttpTransport.Builder()
                .setLowLevelHttpResponse(MockLowLevelHttpResponse()
                        .setStatusCode(HttpStatusCodes.STATUS_CODE_SERVER_ERROR)
                ).build()

        val store = HttpEventStore(anyBackendEndpoint, transport.createRequestFactory {
            it.parser = GsonFactory.getDefaultInstance().createJsonObjectParser()
        })

        val response = store.saveEvents(
                "InventoryItem",
                listOf(EventPayload("::kind::", "::event data::")),
                SaveOptions(aggregateId)) as SaveEventsResponse.Error

        assertThat(response, `is`(Matchers.equalTo(SaveEventsResponse.Error("Generic Error"))))
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

        val response = store.saveEvents("Order", listOf(EventPayload("::event kind::", 1L, "::user id::", Binary("::event data::"))), SaveOptions(aggregateId, 4)) as SaveEventsResponse.EventCollision
        assertThat(response, `is`(Matchers.equalTo(SaveEventsResponse.EventCollision(aggregateId, 5))))
    }

    @Test
    fun retrieveAggregateWithMultipleEvents() {
        val aggregateId = randomAggregateId()

        val responsePayload = """
            {"aggregates": [
                  {
                    "aggregateId": "$aggregateId",
                    "aggregateType": "Invoice",
                    "version": 4,
                    "events": [
                        {"kind": "::kind 1::","timestamp": 1,"version": 1, "identityId":"::user::", "payload": "::event data::"},
                        {"kind": "::kind 2::","timestamp": 2,"version": 2, "identityId":"::user::", "payload": "::event data::"},
                        {"kind": "::kind 3::","timestamp": 3,"version": 3, "identityId":"::user::", "payload": "::event data::"}
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

        val result = store.getEvents(aggregateId, "Invoice") as GetEventsResponse.Success
        assertThat(result.aggregates, hasItems(
                Aggregate(
                        aggregateId,
                        "Invoice",
                        null,
                        4L,
                        listOf(
                                EventPayload("::kind 1::", 1L, "::user::", Binary("::event data::")),
                                EventPayload("::kind 2::", 2L, "::user::", Binary("::event data::")),
                                EventPayload("::kind 3::", 3L, "::user::", Binary("::event data::"))
                        )
                )))
    }

    @Test
    fun retrieveAggregateForSpecificIndexWithMultipleEvents() {
      val aggregateId = randomAggregateId()

      val responsePayload = """
              {"aggregates": [
                    {
                      "aggregateId": "$aggregateId",
                      "aggregateType": "Invoice",
                      "version": 4,
                      "events": [
                          {"kind": "::kind 1::","timestamp": 1,"version": 1, "identityId":"::user::", "payload": "::event data::"},
                          {"kind": "::kind 2::","timestamp": 2,"version": 2, "identityId":"::user::", "payload": "::event data::"},
                          {"kind": "::kind 3::","timestamp": 3,"version": 3, "identityId":"::user::", "payload": "::event data::"}
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

      var request: HttpRequest? = null
      val store = HttpEventStore(anyBackendEndpoint, transport.createRequestFactory {
        it.parser = GsonFactory.getDefaultInstance().createJsonObjectParser()
        request = it
      })

      val result = store.getEvents(aggregateId, "Invoice", 1) as GetEventsResponse.Success
      assertThat(result.aggregates, hasItems(
          Aggregate(
              aggregateId,
              "Invoice",
              null,
              4L,
              listOf(
                  EventPayload("::kind 1::", 1L, "::user::", Binary("::event data::")),
                  EventPayload("::kind 2::", 2L, "::user::", Binary("::event data::")),
                  EventPayload("::kind 3::", 3L, "::user::", Binary("::event data::"))
              )
          )))

      assertTrue(request!!.url.toString().contains("&index=1"))
    }

    @Test
    fun retrieveEventsOfUnknownAggregate() {
        val aggregateId = randomAggregateId()

        val transport = MockHttpTransport.Builder()
                .setLowLevelHttpResponse(MockLowLevelHttpResponse()
                        .setStatusCode(HttpStatusCodes.STATUS_CODE_NOT_FOUND)
                        .setContent(""))
                .build()

        val store = HttpEventStore(anyBackendEndpoint, transport.createRequestFactory {
            it.parser = GsonFactory.getDefaultInstance().createJsonObjectParser()
        })

        store.getEvents(aggregateId, "Invoice") as GetEventsResponse.AggregateNotFound
    }

    @Test
    fun retrieveAllEvents() {
        val aggregateId = randomAggregateId()

        val responsePayload = """
            {"events": [
                  {
                    "position": 1,
                    "aggregateId": "$aggregateId",
                    "aggregateType": "Invoice",
                    "version": 1,
                    "payload":  {"kind": "::kind 1::","timestamp": 1,"version": 1, "identityId":"::user::", "payload": "::event data::"}
                  },
                  {
                    "position": 2,
                    "aggregateId": "$aggregateId",
                    "aggregateType": "Invoice",
                    "version": 1,
                    "payload":  {"kind": "::kind 2::","timestamp": 2,"version": 2, "identityId":"::user::", "payload": "::event data::"}
                  },
                  {
                    "position": 3,
                    "aggregateId": "$aggregateId",
                    "aggregateType": "Invoice",
                    "version": 1,
                    "payload":  {"kind": "::kind 3::","timestamp": 3,"version": 3, "identityId":"::user::", "payload": "::event data::"}
                  },
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
                IndexedEvent(Position(1L), aggregateId, "Invoice", 1L, EventPayload("::kind 1::", 1L, "::user::", Binary("::event data::"))),
                IndexedEvent(Position(2L), aggregateId, "Invoice", 1L, EventPayload("::kind 2::", 2L, "::user::", Binary("::event data::"))),
                IndexedEvent(Position(3L), aggregateId, "Invoice", 1L, EventPayload("::kind 3::", 3L, "::user::", Binary("::event data::")))
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

        store.getAllEvents(GetAllEventsRequest(Position(3), 5, ReadDirection.FORWARD, listOf("Type1", "Type2")))

        assertThat(calls, `is`(equalTo(listOf(
                "GET:http://localhost:8080/v2/aggregates/\$all?fromPosition=3&maxCount=5&readDirection=FORWARD&aggregateTypes=Type1,Type2"
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

        val result = store.getEvents(aggregateId, "Invoice") as GetEventsResponse.Success
        assertThat(result, Matchers.equalTo(
                GetEventsResponse.Success(listOf(Aggregate(aggregateId, "Order", null, 4L, listOf())))
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

        store.saveEvents("Invoice", listOf(EventPayload("::kind::", 1L, "::user::", Binary("::event data::"))), SaveOptions(aggregateId, 1L, "crm", CreateSnapshot(true, Snapshot(0, Binary("data")))))

        val outputStream = ByteArrayOutputStream()
        transport.lowLevelHttpRequest.streamingContent.writeTo(outputStream)

        assertThat(outputStream.toString(), `is`(equalTo(
                """{"aggregateId":"$aggregateId","aggregateType":"Invoice","events":[{"identityId":"::user::","kind":"::kind::","payload":"::event data::","timestamp":1}],"snapshot":{"data":{"payload":[100,97,116,97]},"version":0},"snapshotRequired":true,"topicName":"crm","version":1}""".trimIndent()
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

        store.saveEvents("Invoice", listOf(EventPayload("::kind::", 1L, "::user::", Binary("::event data::"))), SaveOptions(aggregateId, 1L, "crm", CreateSnapshot(true, Snapshot(0, Binary("")))))

        val outputStream = ByteArrayOutputStream()
        transport.lowLevelHttpRequest.streamingContent.writeTo(outputStream)

        assertThat(outputStream.toString(), `is`(equalTo(
                """{"aggregateId":"$aggregateId","aggregateType":"Invoice","events":[{"identityId":"::user::","kind":"::kind::","payload":"::event data::","timestamp":1}],"snapshot":{"data":{"payload":[]},"version":0},"snapshotRequired":true,"topicName":"crm","version":1}""".trimIndent()
        )))
    }

    private fun randomAggregateId() = UUID.randomUUID().toString()
}