import com.clouway.kcqrs.client.HttpEventStore
import com.clouway.kcqrs.core.*
import com.google.api.client.http.HttpStatusCodes
import com.google.api.client.http.LowLevelHttpResponse
import com.google.api.client.json.gson.GsonFactory
import com.google.api.client.testing.http.MockHttpTransport
import com.google.api.client.testing.http.MockLowLevelHttpRequest
import com.google.api.client.testing.http.MockLowLevelHttpResponse
import org.hamcrest.Matchers
import org.hamcrest.Matchers.`is`
import org.hamcrest.Matchers.equalTo
import org.jmock.integration.junit4.JUnitRuleMockery
import org.junit.Assert.assertThat
import org.junit.Rule
import org.junit.Test
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.net.URL
import java.util.*


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
                        .setContent("""{"aggregateId": "$aggregateId","version": 4}"""))
                .build()

        val store = HttpEventStore(anyBackendEndpoint, transport.createRequestFactory {
            it.parser = GsonFactory.getDefaultInstance().createJsonObjectParser()
        })
        val response = store.saveEvents("Invoice", listOf(EventPayload("::kind::", 1L, "::user::", Binary("::event data::"))), SaveOptions(aggregateId)) as SaveEventsResponse.Success

        assertThat(response, `is`(Matchers.equalTo(SaveEventsResponse.Success(aggregateId, 4))))
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
                """{"aggregateId":"$aggregateId","aggregateType":"Invoice","events":[{"identityId":"::user::","kind":"::kind::","payload":"::event data::","timestamp":1}],"topicName":"crm","version":1}""".trimIndent()
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

        assertThat(response, `is`(Matchers.equalTo(SaveEventsResponse.ErrorInCommunication)))
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
            """.trimIndent()

        val transport = MockHttpTransport.Builder()
                .setLowLevelHttpResponse(MockLowLevelHttpResponse()
                        .setStatusCode(HttpStatusCodes.STATUS_CODE_OK)
                        .setContent(responsePayload))
                .build()

        val store = HttpEventStore(anyBackendEndpoint, transport.createRequestFactory {
            it.parser = GsonFactory.getDefaultInstance().createJsonObjectParser()
        })

        val result = store.getEvents(aggregateId) as GetEventsResponse.Success
        assertThat(result, equalTo(
                GetEventsResponse.Success(
                        aggregateId,
                        "Invoice",
                        null,
                        4L,
                        listOf(
                                EventPayload("::kind 1::", 1L, "::user::", Binary("::event data::")),
                                EventPayload("::kind 2::", 2L, "::user::", Binary("::event data::")),
                                EventPayload("::kind 3::", 3L, "::user::", Binary("::event data::"))
                        )
                )
        ))
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

        store.getEvents(aggregateId) as GetEventsResponse.AggregateNotFound
    }


    @Test
    fun retrieveButNoEvents() {
        val aggregateId = randomAggregateId()

        val responsePayload = """
                   {"aggregateId": "$aggregateId","aggregateType": "Order","version": 4, "events": []}
                   """.trimIndent()

        val transport = MockHttpTransport.Builder()
                .setLowLevelHttpResponse(MockLowLevelHttpResponse()
                        .setStatusCode(HttpStatusCodes.STATUS_CODE_OK)
                        .setContent(responsePayload))
                .build()

        val store = HttpEventStore(anyBackendEndpoint, transport.createRequestFactory {
            it.parser = GsonFactory.getDefaultInstance().createJsonObjectParser()
        })

        val result = store.getEvents(aggregateId) as GetEventsResponse.Success
        assertThat(result, Matchers.equalTo(
                GetEventsResponse.Success(
                        aggregateId,
                        "Order",
                        null,
                4L,
                listOf()
        )
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

        store.revertLastEvents(aggregateId, 3) as RevertEventsResponse.Success
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

        store.revertLastEvents(aggregateId, 3) as RevertEventsResponse.AggregateNotFound
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

        store.revertLastEvents(aggregateId, 3)

        val outputStream = ByteArrayOutputStream()
        transport.lowLevelHttpRequest.streamingContent.writeTo(outputStream)

        assertThat(outputStream.toString(), `is`(equalTo(
                """{"aggregateId":"$aggregateId","count":3}""".trimIndent()
        )))
    }

    private fun randomAggregateId() = UUID.randomUUID().toString()
}