package com.clouway.kcqrs.adapter.appengine

import com.clouway.kcqrs.core.*
import com.clouway.kcqrs.testing.TestMessageFormat
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig
import com.google.appengine.tools.development.testing.LocalServiceTestHelper
import org.hamcrest.CoreMatchers.`is`
import org.hamcrest.CoreMatchers.equalTo
import org.hamcrest.CoreMatchers.hasItems
import org.hamcrest.CoreMatchers.not
import org.junit.After
import org.junit.Assert.assertThat
import org.junit.Assert.fail
import org.junit.Before
import org.junit.Test
import java.util.UUID


/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class AppEngineEventStoreTest {
    private val helper = LocalServiceTestHelper(LocalDatastoreServiceTestConfig()
            .setDefaultHighRepJobPolicyUnappliedJobPercentage(0f))

    @Before
    fun setUp() {
        helper.setUp()
    }

    @After
    fun tearDown() {
        helper.tearDown()
    }

    private val aggregateBase = AppEngineEventStore("Event", TestMessageFormat(), IdGenerators.snowflake())

    @Test
    fun getEventsThatAreStored() {
        val result = aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data::")))
        ) as SaveEventsResponse.Success

        val response = aggregateBase.getEvents(result.aggregateId, "Invoice")

        when (response) {
            is GetEventsResponse.Success -> {
                assertThat(response, `is`(equalTo((
                        GetEventsResponse.Success(
                                listOf(Aggregate(
                                        result.aggregateId,
                                        "Invoice",
                                        null,
                                        1,
                                        listOf(
                                                EventPayload("::kind::", 1L, "::user 1::", Binary("::data::"))
                                        )
                                ))
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

        val response = aggregateBase.getEvents(result.aggregateId, "Order")

        when (response) {
            is GetEventsResponse.Success -> {
                assertThat(response, `is`(equalTo((
                        GetEventsResponse.Success(
                                listOf(Aggregate(
                                        result.aggregateId,
                                        "Order",
                                        null,
                                        2,
                                        listOf(
                                                EventPayload("::kind1::", 1L, "::user 1::", Binary("event1-data")),
                                                EventPayload("::kind2::", 2L, "::user 2::", Binary("event2-data"))
                                        )
                                ))

                        ))
                )))
            }
            else -> fail("got unknown response when fetching stored events")
        }
    }

    @Test
    fun getMultipleAggregates() {
        val result1 = aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data::")))
        ) as SaveEventsResponse.Success

        val result2 = aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data::")))
        ) as SaveEventsResponse.Success

        val response = aggregateBase.getEvents(listOf(result1.aggregateId, result2.aggregateId), "Invoice")

        when (response) {
            is GetEventsResponse.Success -> {
                assertThat(response.aggregates, `is`(hasItems(
                        Aggregate(
                                result1.aggregateId,
                                "Invoice",
                                null,
                                1,
                                listOf(
                                        EventPayload("::kind::", 1L, "::user 1::", Binary("::data::"))
                                )
                        ),
                        Aggregate(
                                result2.aggregateId,
                                "Invoice",
                                null,
                                1,
                                listOf(
                                        EventPayload("::kind::", 1L, "::user 1::", Binary("::data::"))
                                )
                        ))))

            }
            else -> fail("got unknown response when fetching stored events")
        }
    }

    @Test
    fun getMultipleAggregatesButNoneMatched() {
        val response = aggregateBase.getEvents(listOf("::id 1::", "::id 2::"), "Order")

        when (response) {
            is GetEventsResponse.Success -> {
                assertThat(response, `is`(equalTo((
                        GetEventsResponse.Success(
                                listOf())
                        ))))

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

        val response = aggregateBase.revertLastEvents("Task", aggregateId, 2)
        when (response) {
            is RevertEventsResponse.Success -> {

                val resp = aggregateBase.getEvents(aggregateId, "Task") as GetEventsResponse.Success
                assertThat(resp, `is`(equalTo(
                        GetEventsResponse.Success(listOf(Aggregate(aggregateId,
                                "Task",
                                null,
                                3,
                                listOf(
                                        EventPayload("::kind 1::", 1L, "::user1::", Binary("data0")),
                                        EventPayload("::kind 2::", 2L, "::user1::", Binary("data1")),
                                        EventPayload("::kind 3::", 3L, "::user1::", Binary("data2"))
                                ))))
                )))
            }
            else -> fail("got un-expected response '$response' when reverting saved events")
        }
    }

    @Test(expected = IllegalArgumentException::class)
    fun revertZeroEventsIsNotAllowed() {
        aggregateBase.revertLastEvents("Task", "::any id::", 0)
    }
    
    @Test
    fun getEventAfterRevert() {
        val events = listOf(
            EventPayload("::kind 1::", 1L, "::user1::", Binary("data0")),
            EventPayload("::kind 2::", 2L, "::user1::", Binary("data0")),
            EventPayload("::kind 2::", 3L, "::user1::", Binary("data0")),
            EventPayload("::kind 2::", 4L, "::user1::", Binary("data0"))
        )
        
        aggregateBase.saveEvents("Task", events, saveOptions = SaveOptions(aggregateId = "QnogQXP2kNo", version = 0))
        
        val rr = aggregateBase.revertLastEvents("Task", "QnogQXP2kNo", events.size)
        aggregateBase.getEvents("Task", "::any id::") as GetEventsResponse.AggregateNotFound
    }

    @Test
    fun revertingMoreThenTheAvailableEvents() {
        val aggregateId = UUID.randomUUID().toString()

        aggregateBase.saveEvents("A1", listOf(
                EventPayload("::kind 1::", 1L, "::user id::", Binary("data0")),
                EventPayload("::kind 2::", 2L, "::user id::", Binary("data1"))
        ), SaveOptions(aggregateId = aggregateId, version = 0))

        val response = aggregateBase.revertLastEvents("A1", aggregateId, 5)
        when (response) {
            is RevertEventsResponse.ErrorNotEnoughEventsToRevert -> {
            }
            else -> fail("got un-expected response '$response' when reverting more then available")
        }
    }

    @Test
    fun revertFromUnknownAggregate() {
        aggregateBase.revertLastEvents("Type", "::unknown aggregate::", 1) as RevertEventsResponse.AggregateNotFound
    }

    @Test
    fun saveStringWithTooBigSize() {
        val tooBigStringData = "aaaaa".repeat(150000)
        val result = aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind::", 1L, "::user 1::", Binary(tooBigStringData)))
        ) as SaveEventsResponse.Success

        val response = aggregateBase.getEvents(result.aggregateId, "Invoice")

        when (response) {
            is GetEventsResponse.Success -> {
                assertThat(response, `is`(equalTo((
                        GetEventsResponse.Success(
                                listOf(Aggregate(
                                        result.aggregateId,
                                        "Invoice",
                                        null,
                                        1,
                                        listOf(
                                                EventPayload("::kind::", 1L, "::user 1::", Binary(tooBigStringData))
                                        )
                                ))
                        )
                        ))))

            }
            else -> fail("got unknown response when fetching stored events")
        }
    }

    @Test
    fun saveEventExceedsEntityLimitationsAndReturnsCurrentEvents() {
        aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data::"))),
                SaveOptions("::aggregateId::")
        )

        val tooBigStringData = "aaaaaaaa".repeat(150000)

        val eventLimitReachedResponse = aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind::", 1L, "::user 1::", Binary(tooBigStringData))),
                SaveOptions("::aggregateId::", 1)
        ) as SaveEventsResponse.SnapshotRequired

        assertThat(eventLimitReachedResponse.currentEvents, `is`(equalTo(listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data::"))))))
    }

    @Test
    fun getAllEventsAndSingleIsAvailable() {
        aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data::")))
        )

        val response = aggregateBase.getAllEvents(GetAllEventsRequest(null, 5, ReadDirection.FORWARD)) as GetAllEventsResponse.Success

        assertThat(response.events.size, `is`(equalTo(1)))
        assertThat(response.events[0].payload, `is`(equalTo(EventPayload("::kind::", 1L, "::user 1::", Binary("::data::")))))
        assertThat(response.nextPosition!!.value, `is`(equalTo(response.events[0].position.value)))
    }

    @Test
    fun getAllEventsAndMultipleAreAvailable() {
        aggregateBase.saveEvents("Invoice",
                listOf(
                        EventPayload("::kind1::", 1L, "::user 1::", Binary("::data::")),
                        EventPayload("::kind2::", 1L, "::user 1::", Binary("::data::")),
                        EventPayload("::kind3::", 1L, "::user 1::", Binary("::data::"))
                ),
                saveOptions = SaveOptions(aggregateId = "::aggregate 1::")
        )

        val response = aggregateBase.getAllEvents(GetAllEventsRequest(null, 5, ReadDirection.FORWARD)) as GetAllEventsResponse.Success

        assertThat(response.events.size, `is`(equalTo(3)))
        assertThat(response.events[0].aggregateId, `is`(equalTo("::aggregate 1::")))
        assertThat(response.events[0].payload, `is`(equalTo(EventPayload("::kind1::", 1L, "::user 1::", Binary("::data::")))))
        assertThat(response.events[1].aggregateId, `is`(equalTo("::aggregate 1::")))
        assertThat(response.events[1].payload, `is`(equalTo(EventPayload("::kind2::", 1L, "::user 1::", Binary("::data::")))))
        assertThat(response.events[2].aggregateId, `is`(equalTo("::aggregate 1::")))
        assertThat(response.events[2].payload, `is`(equalTo(EventPayload("::kind3::", 1L, "::user 1::", Binary("::data::")))))
        assertThat(response.nextPosition!!.value, `is`(equalTo(response.events[2].position.value)))
    }

    @Test
    fun getAllEventsOfMultipleAggregates() {
        aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind 1::", 1L, "::user 1::", Binary("::data::"))),
                saveOptions = SaveOptions(aggregateId = "::aggregate 1::")
        )
        aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind 2::", 1L, "::user 1::", Binary("::data::"))),
                saveOptions = SaveOptions(aggregateId = "::aggregate 2::")
        )

        val response = aggregateBase.getAllEvents(GetAllEventsRequest(null, 5, ReadDirection.FORWARD)) as GetAllEventsResponse.Success

        assertThat(response.events.size, `is`(equalTo(2)))
        assertThat(response.events[0].aggregateId, `is`(equalTo("::aggregate 1::")))
        assertThat(response.events[0].payload, `is`(equalTo(EventPayload("::kind 1::", 1L, "::user 1::", Binary("::data::")))))
        assertThat(response.events[1].aggregateId, `is`(equalTo("::aggregate 2::")))
        assertThat(response.events[1].payload, `is`(equalTo(EventPayload("::kind 2::", 1L, "::user 1::", Binary("::data::")))))
    }

    @Test
    fun getAllEventsFilteredByAggregates() {
        aggregateBase.saveEvents("Order",
                listOf(EventPayload("::kind 1::", 1L, "::user 1::", Binary("::data::"))),
                saveOptions = SaveOptions(aggregateId = "::aggregate 1::")
        )

        aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind 2::", 1L, "::user 1::", Binary("::data::"))),
                saveOptions = SaveOptions(aggregateId = "::aggregate 2::")
        )

        aggregateBase.saveEvents("Shipment",
                listOf(EventPayload("::kind 3::", 1L, "::user 1::", Binary("::data::"))),
                saveOptions = SaveOptions(aggregateId = "::aggregate 3::")
        )

        val response = aggregateBase.getAllEvents(GetAllEventsRequest(null, 5, ReadDirection.FORWARD, listOf("Order", "Invoice"))) as GetAllEventsResponse.Success

        assertThat(response.events.size, `is`(equalTo(2)))
        assertThat(response.events[0].aggregateId, `is`(equalTo("::aggregate 1::")))
        assertThat(response.events[0].payload, `is`(equalTo(EventPayload("::kind 1::", 1L, "::user 1::", Binary("::data::")))))
        assertThat(response.events[1].aggregateId, `is`(equalTo("::aggregate 2::")))
        assertThat(response.events[1].payload, `is`(equalTo(EventPayload("::kind 2::", 1L, "::user 1::", Binary("::data::")))))
    }

    @Test
    fun onlyMaxCountIsRetrieved() {
        aggregateBase.saveEvents("Invoice",
                listOf(
                        EventPayload("::kind1::", 1L, "::user 1::", Binary("::data::")),
                        EventPayload("::kind2::", 1L, "::user 1::", Binary("::data::")),
                        EventPayload("::kind3::", 1L, "::user 1::", Binary("::data::"))
                ),
                saveOptions = SaveOptions(aggregateId = "::aggregate 1::")
        )

        val response = aggregateBase.getAllEvents(GetAllEventsRequest(null, 2, ReadDirection.FORWARD)) as GetAllEventsResponse.Success
        assertThat(response.events.size, `is`(equalTo(2)))
    }

    @Test
    fun readFromRequestedPosition() {
        val saveResponse = aggregateBase.saveEvents("Invoice",
                listOf(
                        EventPayload("::kind 1::", 1L, "::user 1::", Binary("::data::")),
                        EventPayload("::kind 2::", 1L, "::user 1::", Binary("::data::")),
                        EventPayload("::kind 3::", 1L, "::user 1::", Binary("::data::"))
                ),
                saveOptions = SaveOptions(aggregateId = "::aggregate 1::")
        ) as SaveEventsResponse.Success

        val response = aggregateBase.getAllEvents(GetAllEventsRequest(Position(saveResponse.sequenceIds[0]), 3, ReadDirection.FORWARD)) as GetAllEventsResponse.Success

        assertThat(response.events.size, `is`(equalTo(2)))
        assertThat(response.events[0].aggregateId, `is`(equalTo("::aggregate 1::")))
        assertThat(response.events[0].payload, `is`(equalTo(EventPayload("::kind 2::", 1L, "::user 1::", Binary("::data::")))))
        assertThat(response.events[1].aggregateId, `is`(equalTo("::aggregate 1::")))
        assertThat(response.events[1].payload, `is`(equalTo(EventPayload("::kind 3::", 1L, "::user 1::", Binary("::data::")))))
    }

    @Test
    fun readFromRequestedPositionWhenMultipleAggregates() {
        aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind 1::", 1L, "::user 1::", Binary("::data::"))),
                saveOptions = SaveOptions(aggregateId = "::aggregate 1::")
        ) as SaveEventsResponse.Success

        val saveResponse = aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind 1::", 1L, "::user 1::", Binary("::data::"))),
                saveOptions = SaveOptions(aggregateId = "::aggregate 2::")
        ) as SaveEventsResponse.Success

        val response = aggregateBase.getAllEvents(GetAllEventsRequest(Position(saveResponse.sequenceIds[0] - 1), 3, ReadDirection.FORWARD)) as GetAllEventsResponse.Success

        assertThat(response.events.size, `is`(equalTo(1)))
        assertThat(response.events[0].aggregateId, `is`(equalTo("::aggregate 2::")))
        assertThat(response.events[0].payload, `is`(equalTo(EventPayload("::kind 1::", 1L, "::user 1::", Binary("::data::")))))
    }

    @Test
    fun readFromBack() {
        val saveResponse = aggregateBase.saveEvents("Invoice",
                listOf(
                        EventPayload("::kind 1::", 1L, "::user 1::", Binary("::data::")),
                        EventPayload("::kind 2::", 1L, "::user 1::", Binary("::data::")),
                        EventPayload("::kind 3::", 1L, "::user 1::", Binary("::data::"))
                ),
                saveOptions = SaveOptions(aggregateId = "::aggregate 1::")
        ) as SaveEventsResponse.Success

        val response = aggregateBase.getAllEvents(GetAllEventsRequest(Position(saveResponse.sequenceIds[2] + 1), 3, ReadDirection.BACKWARD)) as GetAllEventsResponse.Success

        assertThat(response.events.size, `is`(equalTo(3)))
        assertThat(response.events[0].aggregateId, `is`(equalTo("::aggregate 1::")))
        assertThat(response.events[0].payload, `is`(equalTo(EventPayload("::kind 3::", 1L, "::user 1::", Binary("::data::")))))
        assertThat(response.events[1].payload, `is`(equalTo(EventPayload("::kind 2::", 1L, "::user 1::", Binary("::data::")))))
        assertThat(response.events[2].payload, `is`(equalTo(EventPayload("::kind 1::", 1L, "::user 1::", Binary("::data::")))))
    }

    @Test
    fun returningManyEventsOnLimitReached() {
        aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data::"))),
                SaveOptions("::aggregateId::", 0)
        )

        aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data2::"))),
                SaveOptions("::aggregateId::", 1)
        )

        val tooBigStringData = "aaaaaaaa".repeat(150000)

        val eventLimitReachedResponse = aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind::", 1L, "::user 1::", Binary(tooBigStringData))),
                SaveOptions("::aggregateId::", 2)
        ) as SaveEventsResponse.SnapshotRequired

        assertThat(eventLimitReachedResponse.currentEvents, `is`(equalTo(listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data::")), EventPayload("::kind::", 1L, "::user 1::", Binary("::data2::"))))))
    }

    @Test
    fun onEventLimitReachSnapshotIsReturned() {
        aggregateBase.saveEvents("Invoice",
            listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data0::"))),
            SaveOptions("::aggregateId::", 0, "::topic::", CreateSnapshot(true, Snapshot(0, Binary("::snapshotData::"))))
        )

        aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data::"))),
                SaveOptions("::aggregateId::", 1, "::topic::", CreateSnapshot(true, Snapshot(1, Binary("::snapshotData::"))))
        )

        val tooBigStringData = "aaaaaaaa".repeat(150000)

        val eventLimitReachedResponse = aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind::", 1L, "::user 1::", Binary(tooBigStringData))),
                SaveOptions("::aggregateId::", 2)
        ) as SaveEventsResponse.SnapshotRequired

        assertThat(eventLimitReachedResponse.currentEvents, `is`(equalTo(listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data::"))))))
        assertThat(eventLimitReachedResponse.currentSnapshot, `is`(equalTo(Snapshot(1, Binary("::snapshotData::")))))
    }

    @Test
    fun requestingSnapshotSave() {
        val saveEvents = aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data::"))),
                SaveOptions("::aggregateId::", 0, "::topic::", CreateSnapshot(true, Snapshot(0, Binary("::snapshotData::"))))
        ) as SaveEventsResponse.Success

        val response = aggregateBase.getEvents("::aggregateId::", "Invoice")
        when (response) {
            is GetEventsResponse.Success -> {
                assertThat(response, `is`(equalTo((
                        GetEventsResponse.Success(
                                listOf(Aggregate(
                                        saveEvents.aggregateId,
                                        "Invoice",
                                        Snapshot(0, Binary("::snapshotData::")),
                                        1,
                                        listOf(
                                                EventPayload("::kind::", 1L, "::user 1::", Binary("::data::"))
                                        )
                                ))
                        )
                        ))))
            }
            else -> fail("got unknown response when fetching stored events")
        }
    }

    @Test
    fun saveEventsAfterSnapshotChange() {
        aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data::"))),
                SaveOptions("::aggregateId::", 1, "::topic::", CreateSnapshot(true, Snapshot(1, Binary("::snapshotData::"))))
        )

        val success = aggregateBase.getEvents(listOf("::aggregateId::"), "Invoice") as  GetEventsResponse.Success

        val saveEvents = aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data2::"))),
                SaveOptions("::aggregateId::", success.aggregates[0].version)
        ) as SaveEventsResponse.Success

        val response = aggregateBase.getEvents(listOf("::aggregateId::"), "Invoice")
        when (response) {
            is GetEventsResponse.Success -> {
                assertThat(response, `is`(equalTo((
                        GetEventsResponse.Success(
                                listOf(Aggregate(
                                        saveEvents.aggregateId,
                                        "Invoice",
                                        Snapshot(1, Binary("::snapshotData::")),
                                        3,
                                        listOf(
                                                EventPayload("::kind::", 1L, "::user 1::", Binary("::data::")),
                                                EventPayload("::kind::", 1L, "::user 1::", Binary("::data2::"))
                                        )
                                ))
                        )
                        ))))

            }
            else -> fail("got unknown response when fetching stored events")
        }
    }

    @Test
    fun saveManySnapshots() {
        // save event for first time
        aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data::"))),
                SaveOptions("::aggregateId::", 0, "::topic::", CreateSnapshot(false))
        )


        //fetch the current aggregate value and provide the current version
        val noSnapshotResponse = aggregateBase.getEvents(listOf("::aggregateId::"), "Invoice") as GetEventsResponse.Success
        val noSnapshotVersion = noSnapshotResponse.aggregates[0].version

       aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data::"))),
                SaveOptions("::aggregateId::", noSnapshotVersion, "::topic::", CreateSnapshot(true, Snapshot(noSnapshotVersion, Binary("::snapshotData::"))))
        )
        
        //fetch the current aggregate value and provide the current version
        val firstSnapshotResponse = aggregateBase.getEvents(listOf("::aggregateId::"), "Invoice") as GetEventsResponse.Success
        val firstSnapshotVersion = firstSnapshotResponse.aggregates[0].version

        val response = aggregateBase.saveEvents("Invoice",
                listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data2::"))),
                SaveOptions("::aggregateId::", firstSnapshotVersion, "::topic::", CreateSnapshot(true, Snapshot(firstSnapshotVersion, Binary("::snapshotData2::"))))
        ) as SaveEventsResponse.Success

        val success = aggregateBase.getEvents("::aggregateId::", "Invoice")

        when (success) {
            is GetEventsResponse.Success -> {
                assertThat(success, `is`(equalTo((
                        GetEventsResponse.Success(
                                listOf(Aggregate(
                                        response.aggregateId,
                                        "Invoice",
                                        Snapshot(2, Binary("::snapshotData2::")),
                                        3,
                                        listOf(
                                                EventPayload("::kind::", 1L, "::user 1::", Binary("::data2::"))
                                        )
                                ))
                        )
                        ))))

            }
            else -> fail("got unknown response when fetching stored events")
        }
    }

    @Test
    fun getEventsForASpecificIndex() {
        // save event for first time
        aggregateBase.saveEvents("Invoice",
            listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data::"))),
            SaveOptions("::aggregateId::", 0, "::topic::", CreateSnapshot(false))
        )


        //fetch the current aggregate value and provide the current version
        val noSnapshotResponse = aggregateBase.getEvents(listOf("::aggregateId::"), "Invoice") as GetEventsResponse.Success
        val noSnapshotVersion = noSnapshotResponse.aggregates[0].version

        aggregateBase.saveEvents("Invoice",
            listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data1::"))),
            SaveOptions("::aggregateId::", noSnapshotVersion, "::topic::", CreateSnapshot(true, Snapshot(noSnapshotVersion, Binary("::snapshotData::"))))
        )

        //fetch the current aggregate value and provide the current version
        val firstSnapshotResponse = aggregateBase.getEvents(listOf("::aggregateId::"), "Invoice") as GetEventsResponse.Success
        val firstSnapshotVersion = firstSnapshotResponse.aggregates[0].version

        val response = aggregateBase.saveEvents("Invoice",
            listOf(EventPayload("::kind::", 1L, "::user 1::", Binary("::data2::"))),
            SaveOptions("::aggregateId::", firstSnapshotVersion, "::topic::", CreateSnapshot(true, Snapshot(firstSnapshotVersion, Binary("::snapshotData2::"))))
        ) as SaveEventsResponse.Success

        val success = aggregateBase.getEvents("::aggregateId::", "Invoice", 1)

        val test = success as GetEventsResponse.Success
        println(test.aggregates[0].events.size)

        when (success) {
            is GetEventsResponse.Success -> {
                assertThat(success, `is`(equalTo((
                    GetEventsResponse.Success(
                        listOf(Aggregate(
                            response.aggregateId,
                            "Invoice",
                            null,
                            2,
                            listOf(
                                EventPayload("::kind::", 1L, "::user 1::", Binary("::data1::"))
                            )
                        ))
                    )
                    ))))

            }
            else -> fail("got unknown response when fetching stored events")
        }
    }
}

