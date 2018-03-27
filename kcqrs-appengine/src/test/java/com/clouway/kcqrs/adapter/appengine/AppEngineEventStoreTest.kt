package com.clouway.kcqrs.adapter.appengine

import com.clouway.kcqrs.core.*
import com.clouway.kcqrs.testing.TestMessageFormat
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig
import com.google.appengine.tools.development.testing.LocalServiceTestHelper
import org.hamcrest.CoreMatchers.*
import org.junit.After
import org.junit.Assert.assertThat
import org.junit.Assert.fail
import org.junit.Before
import org.junit.Test
import java.util.*

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class AppEngineEventStoreTest {
    private val helper = LocalServiceTestHelper(LocalDatastoreServiceTestConfig()
            .setDefaultHighRepJobPolicyUnappliedJobPercentage(100f))

    @Before
    fun setUp() {
        helper.setUp()
    }

    @After
    fun tearDown() {
        helper.tearDown()
    }

    private val aggregateBase = AppEngineEventStore("Event", TestMessageFormat())

    @Test
    fun getEventsThatAreStored() {
        val result = aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data::")))
        ) as SaveEventsResponse.Success

        val response = aggregateBase.getEvents(result.aggregateId)

        when (response) {

            is GetEventsResponse.Success -> {
                assertThat(response, `is`(equalTo((
                        GetEventsResponse.Success(
                                response.aggregateId,
                                "Invoice",
                                null,
                                1,
                                listOf(
                                        EventPayload("::kind::", 1L, "::user 1::", Binary("::data::"))
                                )
                        )
                        ))))

            }
            else -> fail("got unknown response when fetching stored events")
        }
    }


    @Test
    fun multipleEvents() {
        val result = aggregateBase.saveEvents("Order", listOf(
                EventPayload("::kind1::", 1L, "::user 1::", Binary("event1-data")),
                EventPayload("::kind2::", 2L, "::user 2::", Binary("event2-data"))
        )) as SaveEventsResponse.Success

        val response = aggregateBase.getEvents(result.aggregateId)

        when (response) {
            is GetEventsResponse.Success -> {
                assertThat(response, `is`(equalTo((
                        GetEventsResponse.Success(
                                response.aggregateId,
                                "Order",
                                null,
                                2,
                                listOf(
                                        EventPayload("::kind1::", 1L, "::user 1::", Binary("event1-data")),
                                        EventPayload("::kind2::", 2L, "::user 2::", Binary("event2-data"))
                                )
                        ))
                )))
            }
            else -> fail("got unknown response when fetching stored events")
        }
    }

    @Test
    fun newAggregateIdIsIssuedIfItsNotProvided() {
        val result = aggregateBase.saveEvents("A1", listOf(EventPayload("::kind1::", 1L, "::user id1::", Binary("aggregate1-event1-data")))) as SaveEventsResponse.Success
        val result2 = aggregateBase.saveEvents("A1", listOf(EventPayload("::kind::", 2L, "::user id2::", Binary("aggregate2-event1-data")))) as SaveEventsResponse.Success

        assertThat(result.aggregateId, `is`(not(equalTo(result2.aggregateId))))
    }

    @Test
    fun detectEventCollisions() {
        val aggregateId = UUID.randomUUID().toString()

        aggregateBase.saveEvents("Order", listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data::"))), SaveOptions(aggregateId = aggregateId, version = 0))

        val saveResult = aggregateBase.saveEvents("Order", listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data::"))), SaveOptions(aggregateId = aggregateId, version = 0))

        when (saveResult) {
            is SaveEventsResponse.EventCollision -> {
                assertThat(saveResult.aggregateId, `is`(equalTo(aggregateId)))
                assertThat(saveResult.expectedVersion, `is`(equalTo(1L)))
            }
            else -> fail("got un-expected save result: $saveResult")
        }
    }

    @Test
    fun revertSavedEvents() {
        val aggregateId = UUID.randomUUID().toString()

        aggregateBase.saveEvents("Task", listOf(
                EventPayload("::kind 1::", 1L, "::user1::", Binary("data0")),
                EventPayload("::kind 2::", 2L, "::user1::", Binary("data1")),
                EventPayload("::kind 3::", 3L, "::user1::", Binary("data2")),
                EventPayload("::kind 4::", 4L, "::user2::", Binary("data3")),
                EventPayload("::kind 5::", 5L, "::user2::", Binary("data4"))
        ), SaveOptions(aggregateId = aggregateId, version = 0))

        val response = aggregateBase.revertLastEvents(aggregateId, 2)
        when (response) {
            is RevertEventsResponse.Success -> {

                val resp = aggregateBase.getEvents(aggregateId) as GetEventsResponse.Success
                assertThat(resp, `is`(equalTo(
                        GetEventsResponse.Success(aggregateId,
                                "Task",
                                null,
                                3,
                                listOf(
                                        EventPayload("::kind 1::", 1L, "::user1::", Binary("data0")),
                                        EventPayload("::kind 2::", 2L, "::user1::", Binary("data1")),
                                        EventPayload("::kind 3::", 3L, "::user1::", Binary("data2"))
                                ))
                )))
            }
            else -> fail("got un-expected response '$response' when reverting saved events")
        }
    }

    @Test(expected = IllegalArgumentException::class)
    fun revertZeroEventsIsNotAllowed() {
        aggregateBase.revertLastEvents("::any id::", 0)
    }

    @Test
    fun revertingMoreThenTheAvailableEvents() {
        val aggregateId = UUID.randomUUID().toString()

        aggregateBase.saveEvents("A1", listOf(
                EventPayload("::kind 1::", 1L, "::user id::", Binary("data0")),
                EventPayload("::kind 2::", 2L, "::user id::", Binary("data1"))
        ), SaveOptions(aggregateId = aggregateId, version = 0))

        val response = aggregateBase.revertLastEvents(aggregateId, 5)
        when (response) {
            is RevertEventsResponse.ErrorNotEnoughEventsToRevert -> {
            }
            else -> fail("got un-expected response '$response' when reverting more then available")
        }
    }

    @Test
    fun revertFromUnknownAggregate() {
        aggregateBase.revertLastEvents("::unknown aggregate::", 1) as RevertEventsResponse.AggregateNotFound
    }
}