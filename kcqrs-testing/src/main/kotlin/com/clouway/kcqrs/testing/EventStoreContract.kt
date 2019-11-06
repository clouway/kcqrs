package com.clouway.kcqrs.testing

import com.clouway.kcqrs.core.Aggregate
import com.clouway.kcqrs.core.Binary
import com.clouway.kcqrs.core.CreateSnapshot
import com.clouway.kcqrs.core.EventPayload
import com.clouway.kcqrs.core.EventStore
import com.clouway.kcqrs.core.GetAllEventsRequest
import com.clouway.kcqrs.core.GetAllEventsResponse
import com.clouway.kcqrs.core.GetEventsFromStreamsRequest
import com.clouway.kcqrs.core.GetEventsResponse
import com.clouway.kcqrs.core.Position
import com.clouway.kcqrs.core.ReadDirection
import com.clouway.kcqrs.core.RevertEventsResponse
import com.clouway.kcqrs.core.SaveEventsRequest
import com.clouway.kcqrs.core.SaveEventsResponse
import com.clouway.kcqrs.core.SaveOptions
import com.clouway.kcqrs.core.Snapshot
import org.hamcrest.CoreMatchers.`is`
import org.hamcrest.CoreMatchers.equalTo
import org.hamcrest.CoreMatchers.hasItems
import org.junit.Assert.assertThat
import org.junit.Assert.fail
import org.junit.Before
import org.junit.Test
import java.util.UUID

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
abstract class EventStoreContract {
	
	private lateinit var eventStore: EventStore
	
	@Before
	fun setUp() {
		eventStore = createEventStore()
	}
	
	@Test
	fun getEventsThatAreStored() {
		eventStore.saveEvents(
				SaveEventsRequest(
						"tenant1", "Invoice_aggregate1", "Invoice", listOf(EventPayload("aggregate1", "::kind::", 1L, "::user 1::", Binary("::data::")))
				)
		) as SaveEventsResponse.Success
		
		val response = eventStore.getEventsFromStreams(GetEventsFromStreamsRequest("tenant1", "Invoice_aggregate1"))
		
		when (response) {
			is GetEventsResponse.Success -> {
				assertThat(response, `is`(equalTo((
						GetEventsResponse.Success(
								listOf(Aggregate(
										"Invoice",
										null,
										1,
										listOf(
												EventPayload("aggregate1", "::kind::", 1L, "::user 1::", Binary("::data::"))
										)
								))
						)
						))))
				
			}
			else -> fail("got unknown response when fetching stored events")
		}
	}
	
	@Test
	fun allSavedAggregateEventsAreReturned() {
		eventStore.saveEvents(SaveEventsRequest("tenant1", "invoicing", "Invoice",
				listOf(EventPayload("aggregate1", "::kind1::", 1L, "::user 1::", Binary("::data::"))))
		)
		
		val response = eventStore.saveEvents(SaveEventsRequest("tenant1", "invoicing", "Invoice",
				listOf(EventPayload("aggregate1", "::kind2::", 1L, "::user 1::", Binary("::data2::")))),
				SaveOptions(version = 1)
		) as SaveEventsResponse.Success
		
		assertThat(response.aggregate, `is`(equalTo(Aggregate("Invoice", null, 2L, listOf(
				EventPayload("aggregate1", "::kind1::", 1L, "::user 1::", Binary("::data::")),
				EventPayload("aggregate1", "::kind2::", 1L, "::user 1::", Binary("::data2::"))
		)))))
	}
	
	
	@Test
	fun multipleEvents() {
		eventStore.saveEvents(SaveEventsRequest("tenant1", "Order_aggregate1", "Order", listOf(
				EventPayload("aggregate1", "::kind1::", 1L, "::user 1::", Binary("event1-data")),
				EventPayload("aggregate1", "::kind2::", 2L, "::user 2::", Binary("event2-data"))
		))) as SaveEventsResponse.Success
		
		val response = eventStore.getEventsFromStreams(GetEventsFromStreamsRequest("tenant1", "Order_aggregate1"))
		
		when (response) {
			is GetEventsResponse.Success -> {
				assertThat(response, `is`(equalTo((
						GetEventsResponse.Success(
								listOf(Aggregate(
										"Order",
										null,
										2,
										listOf(
												EventPayload("aggregate1", "::kind1::", 1L, "::user 1::", Binary("event1-data")),
												EventPayload("aggregate1", "::kind2::", 2L, "::user 2::", Binary("event2-data"))
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
		eventStore.saveEvents(SaveEventsRequest("tenant1", "Invoice_aggregate1", "Invoice",
				
				listOf(EventPayload("aggregate1", "::kind::", 1L, "::user 1::", Binary("::data::")))
		)) as SaveEventsResponse.Success
		
		eventStore.saveEvents(SaveEventsRequest("tenant1", "Invoice_aggregate2", "Invoice",
				listOf(EventPayload("aggregate2", "::kind::", 1L, "::user 1::", Binary("::data::")))
		)) as SaveEventsResponse.Success
		
		val response = eventStore.getEventsFromStreams(GetEventsFromStreamsRequest("tenant1", listOf("Invoice_aggregate1", "Invoice_aggregate2")))
		
		when (response) {
			is GetEventsResponse.Success -> {
				assertThat(response.aggregates, `is`(hasItems(
						Aggregate(
								"Invoice",
								null,
								1,
								listOf(
										EventPayload("aggregate1", "::kind::", 1L, "::user 1::", Binary("::data::"))
								)
						),
						Aggregate(
								"Invoice",
								null,
								1,
								listOf(
										EventPayload("aggregate2", "::kind::", 1L, "::user 1::", Binary("::data::"))
								)
						))))
				
			}
			else -> fail("got unknown response when fetching stored events")
		}
	}
	
	@Test
	fun getMultipleAggregatesButNoneMatched() {
		val response = eventStore.getEventsFromStreams(GetEventsFromStreamsRequest("tenant1", listOf("Order_id1", "Order_id2")))
		
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
	fun detectEventCollisions() {
		val aggregateId = UUID.randomUUID().toString()
		
		eventStore.saveEvents(SaveEventsRequest("tenant1", "Order_$aggregateId", "Order", listOf(EventPayload(aggregateId, "::kind::", 1L, "::user 1::", Binary("::data::")))), SaveOptions(version = 0))
		
		val saveResult = eventStore.saveEvents(SaveEventsRequest(
				"tenant1",
				"Order_$aggregateId",
				"Order",
				listOf(EventPayload(aggregateId, "::kind::", 1L, "::user 1::", Binary("::data::")))),
				SaveOptions(version = 0)
		)
		
		when (saveResult) {
			is SaveEventsResponse.EventCollision -> {
				assertThat(saveResult.expectedVersion, `is`(equalTo(1L)))
			}
			else -> fail("got un-expected save result: $saveResult")
		}
	}
	
	@Test
	fun revertSavedEvents() {
		val aggregateId = UUID.randomUUID().toString()
		
		eventStore.saveEvents(SaveEventsRequest("tenant1", "Order_$aggregateId", "Order", listOf(
				EventPayload(aggregateId, "::kind 1::", 1L, "::user1::", Binary("data0")),
				EventPayload(aggregateId, "::kind 2::", 2L, "::user1::", Binary("data1")),
				EventPayload(aggregateId, "::kind 3::", 3L, "::user1::", Binary("data2")),
				EventPayload(aggregateId, "::kind 4::", 4L, "::user2::", Binary("data3")),
				EventPayload(aggregateId, "::kind 5::", 5L, "::user2::", Binary("data4"))
		)), SaveOptions(version = 0))
		
		val response = eventStore.revertLastEvents("tenant1", "Order_$aggregateId", 2)
		when (response) {
			is RevertEventsResponse.Success -> {
				val resp = eventStore.getEventsFromStreams(GetEventsFromStreamsRequest("tenant1", "Order_$aggregateId")) as GetEventsResponse.Success
				assertThat(resp, `is`(equalTo(
						GetEventsResponse.Success(listOf(Aggregate(
								"Order",
								null,
								3,
								listOf(
										EventPayload(aggregateId, "::kind 1::", 1L, "::user1::", Binary("data0")),
										EventPayload(aggregateId, "::kind 2::", 2L, "::user1::", Binary("data1")),
										EventPayload(aggregateId, "::kind 3::", 3L, "::user1::", Binary("data2"))
								))))
				)))
			}
			else -> fail("got un-expected response '$response' when reverting saved events")
		}
	}
	
	@Test(expected = IllegalArgumentException::class)
	fun revertZeroEventsIsNotAllowed() {
		eventStore.revertLastEvents("tenant1", "Task_1", 0)
	}
	
	@Test
	fun revertingMoreThenTheAvailableEvents() {
		val aggregateId = UUID.randomUUID().toString()
		
		eventStore.saveEvents(SaveEventsRequest("tenant1", "A1_$aggregateId", "A1", listOf(
				EventPayload(aggregateId, "::kind 1::", 1L, "::user id::", Binary("data0")),
				EventPayload(aggregateId, "::kind 2::", 2L, "::user id::", Binary("data1"))
		)), SaveOptions(version = 0))
		
		val response = eventStore.revertLastEvents("tenant1", "A1_$aggregateId", 5)
		when (response) {
			is RevertEventsResponse.ErrorNotEnoughEventsToRevert -> {
			}
			else -> fail("got un-expected response '$response' when reverting more then available")
		}
	}
	
	@Test
	fun revertFromUnknownAggregate() {
		eventStore.revertLastEvents("Type", "::unknown aggregate::", 1) as RevertEventsResponse.AggregateNotFound
	}
	
	@Test
	fun saveStringWithTooBigSize() {
		val tooBigStringData = "aaaaa".repeat(150000)
		eventStore.saveEvents(
				SaveEventsRequest(
						"tenant1", "Invoice_aggregate1", "Invoice",
						listOf(EventPayload("aggregate1", "::kind::", 1L, "::user 1::", Binary(tooBigStringData)))
				)
		) as SaveEventsResponse.Success
		
		val response = eventStore.getEventsFromStreams(GetEventsFromStreamsRequest("tenant1", "Invoice_aggregate1"))
		
		when (response) {
			is GetEventsResponse.Success -> {
				assertThat(response, `is`(equalTo((
						GetEventsResponse.Success(
								listOf(Aggregate(
										"Invoice",
										null,
										1,
										listOf(
												EventPayload("aggregate1", "::kind::", 1L, "::user 1::", Binary(tooBigStringData))
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
		eventStore.saveEvents(SaveEventsRequest("tenant1", "Invoice_aggregate1", "Invoice",
				listOf(EventPayload("aggregate1", "::kind::", 1L, "::user 1::", Binary("::data::"))))
		)
		
		val tooBigStringData = "aaaaaaaa".repeat(150000)
		
		val eventLimitReachedResponse = eventStore.saveEvents(SaveEventsRequest("tenant1", "Invoice_aggregate1", "Invoice",
				listOf(EventPayload("aggregate1", "::kind::", 1L, "::user 1::", Binary(tooBigStringData)))),
				SaveOptions(version = 1)
		) as SaveEventsResponse.SnapshotRequired
		
		assertThat(eventLimitReachedResponse.currentEvents, `is`(equalTo(listOf(EventPayload("aggregate1", "::kind::", 1L, "::user 1::", Binary("::data::"))))))
	}
	
	@Test
	fun getAllEventsAndSingleIsAvailable() {
		eventStore.saveEvents(SaveEventsRequest("tenant1", "Invoice_aggregate1", "Invoice",
				listOf(EventPayload("aggregate1", "::kind::", 1L, "::user 1::", Binary("::data::"))))
		)
		
		val response = eventStore.getAllEvents(GetAllEventsRequest(null, 5, ReadDirection.FORWARD)) as GetAllEventsResponse.Success
		
		assertThat(response.events.size, `is`(equalTo(1)))
		assertThat(response.events[0].payload, `is`(equalTo(EventPayload("aggregate1", "::kind::", 1L, "::user 1::", Binary("::data::")))))
		assertThat(response.nextPosition!!.value, `is`(equalTo(response.events[0].position.value)))
	}
	
	@Test
	fun getAllEventsAndMultipleAreAvailable() {
		eventStore.saveEvents(
				SaveEventsRequest(
						"tenant1", "Invoice_aggregate1", "Invoice",
						listOf(
								EventPayload("aggregate1", "::kind1::", 1L, "::user 1::", Binary("::data::")),
								EventPayload("aggregate1", "::kind2::", 1L, "::user 1::", Binary("::data::")),
								EventPayload("aggregate1", "::kind3::", 1L, "::user 1::", Binary("::data::"))
						)
				),
				saveOptions = SaveOptions(aggregateId = "aggregate1")
		)
		
		val response = eventStore.getAllEvents(GetAllEventsRequest(null, 5, ReadDirection.FORWARD)) as GetAllEventsResponse.Success
		
		assertThat(response.events.size, `is`(equalTo(3)))
		assertThat(response.events[0].payload, `is`(equalTo(EventPayload("aggregate1", "::kind1::", 1L, "::user 1::", Binary("::data::")))))
		assertThat(response.events[1].payload, `is`(equalTo(EventPayload("aggregate1", "::kind2::", 1L, "::user 1::", Binary("::data::")))))
		assertThat(response.events[2].payload, `is`(equalTo(EventPayload("aggregate1", "::kind3::", 1L, "::user 1::", Binary("::data::")))))
		assertThat(response.nextPosition!!.value, `is`(equalTo(response.events[2].position.value)))
	}
	
	@Test
	fun getAllEventsOfMultipleAggregates() {
		eventStore.saveEvents(SaveEventsRequest("tenant1", "Invoice_aggregate1", "Invoice",
				listOf(EventPayload("aggregate1", "::kind 1::", 1L, "::user 1::", Binary("::data::")))),
				saveOptions = SaveOptions(aggregateId = "aggregate1")
		)
		eventStore.saveEvents(SaveEventsRequest("tenant1", "Invoice_aggregate2", "Invoice",
				listOf(EventPayload("aggregate2", "::kind 2::", 1L, "::user 1::", Binary("::data::")))),
				saveOptions = SaveOptions(aggregateId = "aggregate2")
		)
		
		val response = eventStore.getAllEvents(GetAllEventsRequest(null, 5, ReadDirection.FORWARD)) as GetAllEventsResponse.Success
		
		assertThat(response.events.size, `is`(equalTo(2)))
		assertThat(response.events[0].payload, `is`(equalTo(EventPayload("aggregate1", "::kind 1::", 1L, "::user 1::", Binary("::data::")))))
		assertThat(response.events[1].payload, `is`(equalTo(EventPayload("aggregate2", "::kind 2::", 1L, "::user 1::", Binary("::data::")))))
	}
	
	@Test
	fun getAllEventsFilteredByStream() {
		eventStore.saveEvents(SaveEventsRequest("tenant1", "Order_aggregate1", "Order",
				listOf(EventPayload("aggregate1", "::kind 1::", 1L, "::user 1::", Binary("::data::")))),
				saveOptions = SaveOptions(aggregateId = "aggregate1")
		)
		
		eventStore.saveEvents(SaveEventsRequest("tenant1", "Invoice_aggregate2", "Invoice",
				listOf(EventPayload("aggregate2", "::kind 2::", 1L, "::user 1::", Binary("::data::")))),
				saveOptions = SaveOptions(aggregateId = "aggregate2")
		)
		
		eventStore.saveEvents(SaveEventsRequest(
				"tenant1",
				"Shipment_aggregate3",
				"Shipment",
				listOf(EventPayload("aggregate3", "::kind 3::", 1L, "::user 1::", Binary("::data::")))),
				saveOptions = SaveOptions(aggregateId = "aggregate3")
		)
		
		val response = eventStore.getAllEvents(GetAllEventsRequest(null, 5, ReadDirection.FORWARD, listOf("Order_aggregate1", "Invoice_aggregate2"))) as GetAllEventsResponse.Success
		
		assertThat(response.events.size, `is`(equalTo(2)))
		assertThat(response.events[0].payload, `is`(equalTo(EventPayload("aggregate1", "::kind 1::", 1L, "::user 1::", Binary("::data::")))))
		assertThat(response.events[1].payload, `is`(equalTo(EventPayload("aggregate2", "::kind 2::", 1L, "::user 1::", Binary("::data::")))))
	}
	
	@Test
	fun onlyMaxCountIsRetrieved() {
		eventStore.saveEvents(SaveEventsRequest("tenant1", "Invoice_aggregate1", "Invoice",
				listOf(
						EventPayload("aggregate1", "::kind1::", 1L, "::user 1::", Binary("::data::")),
						EventPayload("aggregate1", "::kind2::", 1L, "::user 1::", Binary("::data::")),
						EventPayload("aggregate1", "::kind3::", 1L, "::user 1::", Binary("::data::"))
				)),
				saveOptions = SaveOptions(aggregateId = "aggregate1")
		)
		
		val response = eventStore.getAllEvents(GetAllEventsRequest(null, 2, ReadDirection.FORWARD)) as GetAllEventsResponse.Success
		assertThat(response.events.size, `is`(equalTo(2)))
	}
	
	@Test
	fun readFromRequestedPosition() {
		val saveResponse = eventStore.saveEvents(SaveEventsRequest("tenant1", "billing", "Invoice",
				listOf(
						EventPayload("aggregate1", "::kind 1::", 1L, "::user 1::", Binary("::data::")),
						EventPayload("aggregate1", "::kind 2::", 1L, "::user 1::", Binary("::data::")),
						EventPayload("aggregate1", "::kind 3::", 1L, "::user 1::", Binary("::data::"))
				)),
				saveOptions = SaveOptions(aggregateId = "aggregate1")
		) as SaveEventsResponse.Success
		
		val response = eventStore.getAllEvents(GetAllEventsRequest(Position(saveResponse.sequenceIds[0]), 3, ReadDirection.FORWARD)) as GetAllEventsResponse.Success
		
		assertThat(response.events.size, `is`(equalTo(2)))
		assertThat(response.events[0].payload, `is`(equalTo(EventPayload("aggregate1", "::kind 2::", 1L, "::user 1::", Binary("::data::")))))
		assertThat(response.events[1].payload, `is`(equalTo(EventPayload("aggregate1", "::kind 3::", 1L, "::user 1::", Binary("::data::")))))
	}
	
	@Test
	fun readFromRequestedPositionWhenMultipleAggregates() {
		eventStore.saveEvents(SaveEventsRequest("tenant1", "Invoice_aggregate1", "Invoice",
				listOf(EventPayload("aggregate1", "::kind 1::", 1L, "::user 1::", Binary("::data::")))),
				saveOptions = SaveOptions(aggregateId = "aggregate1")
		) as SaveEventsResponse.Success
		
		val saveResponse = eventStore.saveEvents(SaveEventsRequest("tenant1", "Invoice_aggregate2", "Invoice",
				listOf(EventPayload("aggregate2", "::kind 1::", 1L, "::user 1::", Binary("::data::"))))
		) as SaveEventsResponse.Success
		
		val response = eventStore.getAllEvents(GetAllEventsRequest(Position(saveResponse.sequenceIds[0] - 1), 3, ReadDirection.FORWARD)) as GetAllEventsResponse.Success
		
		assertThat(response.events.size, `is`(equalTo(1)))
		assertThat(response.events[0].payload, `is`(equalTo(EventPayload("aggregate2", "::kind 1::", 1L, "::user 1::", Binary("::data::")))))
	}
	
	@Test
	fun readFromBack() {
		val saveResponse = eventStore.saveEvents(SaveEventsRequest("tenant1", "invoicing", "Invoice",
				listOf(
						EventPayload("aggregate1", "::kind 1::", 1L, "::user 1::", Binary("::data::")),
						EventPayload("aggregate1", "::kind 2::", 1L, "::user 1::", Binary("::data::")),
						EventPayload("aggregate1", "::kind 3::", 1L, "::user 1::", Binary("::data::"))
				))
		) as SaveEventsResponse.Success
		
		val response = eventStore.getAllEvents(GetAllEventsRequest(Position(saveResponse.sequenceIds[2] + 1), 3, ReadDirection.BACKWARD)) as GetAllEventsResponse.Success
		
		assertThat(response.events.size, `is`(equalTo(3)))
		assertThat(response.events[0].payload, `is`(equalTo(EventPayload("aggregate1", "::kind 3::", 1L, "::user 1::", Binary("::data::")))))
		assertThat(response.events[1].payload, `is`(equalTo(EventPayload("aggregate1", "::kind 2::", 1L, "::user 1::", Binary("::data::")))))
		assertThat(response.events[2].payload, `is`(equalTo(EventPayload("aggregate1", "::kind 1::", 1L, "::user 1::", Binary("::data::")))))
	}
	
	@Test
	fun returningManyEventsOnLimitReached() {
		eventStore.saveEvents(SaveEventsRequest("tenant1", "invoicing", "Invoice",
				listOf(EventPayload("invoicing", "::kind::", 1L, "::user 1::", Binary("::data::"))))
		)
		
		eventStore.saveEvents(SaveEventsRequest("tenant1", "invoicing", "Invoice",
				listOf(EventPayload("invoicing", "::kind::", 1L, "::user 1::", Binary("::data2::")))),
				SaveOptions(version = 1)
		)
		
		val tooBigStringData = "aaaaaaaa".repeat(150000)
		
		val eventLimitReachedResponse = eventStore.saveEvents(SaveEventsRequest("tenant1", "invoicing", "Invoice",
				listOf(EventPayload("invoicing", "::kind::", 1L, "::user 1::", Binary(tooBigStringData)))),
				SaveOptions(version = 2)
		) as SaveEventsResponse.SnapshotRequired
		
		assertThat(eventLimitReachedResponse.currentEvents, `is`(
				equalTo(
						listOf(
								EventPayload("invoicing", "::kind::", 1L, "::user 1::", Binary("::data::")),
								EventPayload("invoicing", "::kind::", 1L, "::user 1::", Binary("::data2::"))
						)
				)
		))
	}
	
	@Test
	fun onEventLimitReachSnapshotIsReturned() {
		eventStore.saveEvents(
				SaveEventsRequest(
						"tenant1", "Invoice_aggregate1", "Invoice",
						listOf(EventPayload("aggregate1", "::kind::", 1L, "::user 1::", Binary("::data0::")))
				),
				SaveOptions(version = 0, createSnapshot = CreateSnapshot(true, Snapshot(0, Binary("::snapshotData::"))))
		)
		
		eventStore.saveEvents(
				SaveEventsRequest(
						"tenant1", "Invoice_aggregate1", "Invoice",
						listOf(EventPayload("aggregate1", "::kind::", 1L, "::user 1::", Binary("::data::")))
				),
				SaveOptions(version = 1, createSnapshot = CreateSnapshot(true, Snapshot(1, Binary("::snapshotData::"))))
		)
		
		val tooBigStringData = "aaaaaaaa".repeat(150000)
		
		val eventLimitReachedResponse = eventStore.saveEvents(SaveEventsRequest("tenant1", "Invoice_aggregate1", "Invoice",
				listOf(EventPayload("aggregate1", "::kind::", 1L, "::user 1::", Binary(tooBigStringData)))),
				SaveOptions(version = 2)
		) as SaveEventsResponse.SnapshotRequired
		
		assertThat(eventLimitReachedResponse.currentEvents, `is`(equalTo(listOf(EventPayload("aggregate1", "::kind::", 1L, "::user 1::", Binary("::data::"))))))
		assertThat(eventLimitReachedResponse.currentSnapshot, `is`(equalTo(Snapshot(1, Binary("::snapshotData::")))))
	}
	
	@Test
	fun requestingSnapshotSave() {
		eventStore.saveEvents(SaveEventsRequest("tenant1", "Invoice_aggregate1", "Invoice",
				listOf(EventPayload("aggregate1", "::kind::", 1L, "::user 1::", Binary("::data::")))),
				SaveOptions(version = 0, topicName = "::topic::", createSnapshot = CreateSnapshot(true, Snapshot(0, Binary("::snapshotData::"))))
		) as SaveEventsResponse.Success
		
		val response = eventStore.getEventsFromStreams(GetEventsFromStreamsRequest("tenant1", "Invoice_aggregate1"))
		when (response) {
			is GetEventsResponse.Success -> {
				assertThat(response, `is`(equalTo((
						GetEventsResponse.Success(
								listOf(Aggregate(
										"Invoice",
										Snapshot(0, Binary("::snapshotData::")),
										1,
										listOf(
												EventPayload("aggregate1", "::kind::", 1L, "::user 1::", Binary("::data::"))
										)
								))
						)
						))))
			}
			else -> fail("got unknown response when fetching stored events")
		}
	}
	
	@Test
	fun saveEventIsReturningSnapshotWhenAvailable() {
		eventStore.saveEvents(SaveEventsRequest("tenant1", "Invoice_aggregate1", "Invoice",
				listOf(EventPayload("aggregate1", "::kind::", 1L, "::user 1::", Binary("::data::")))),
				SaveOptions("::aggregateId::", 1, "::topic::", CreateSnapshot(true, Snapshot(1, Binary("::snapshotData::"))))
		)
		
		val saveResponse = eventStore.saveEvents(SaveEventsRequest("tenant1", "Invoice_aggregate1", "Invoice",
				listOf(EventPayload("aggregate1", "::kind::", 1L, "::user 1::", Binary("::data2::")))),
				SaveOptions("::aggregateId::", 2L)
		) as SaveEventsResponse.Success
		
		assertThat(saveResponse.aggregate.snapshot, `is`(equalTo(Snapshot(1L, Binary("::snapshotData::")))))
	}
	
	@Test
	fun saveEventsAfterSnapshotChange() {
		eventStore.saveEvents(SaveEventsRequest("tenant1", "Invoice_aggregate1", "Invoice",
				listOf(EventPayload("aggregate1", "::kind::", 1L, "::user 1::", Binary("::data::")))),
				SaveOptions("::aggregateId::", 1, "::topic::", CreateSnapshot(true, Snapshot(1, Binary("::snapshotData::"))))
		)
		
		val success = eventStore.getEventsFromStreams(GetEventsFromStreamsRequest("tenant1", "Invoice_aggregate1")) as GetEventsResponse.Success
		
		eventStore.saveEvents(SaveEventsRequest("tenant1", "Invoice_aggregate1", "Invoice",
				listOf(EventPayload("aggregate1", "::kind::", 1L, "::user 1::", Binary("::data2::")))),
				SaveOptions("::aggregateId::", success.aggregates[0].version)
		) as SaveEventsResponse.Success
		
		val response = eventStore.getEventsFromStreams(GetEventsFromStreamsRequest("tenant1", "Invoice_aggregate1"))
		when (response) {
			is GetEventsResponse.Success -> {
				assertThat(response, `is`(equalTo((
						GetEventsResponse.Success(
								listOf(Aggregate(
										"Invoice",
										Snapshot(1, Binary("::snapshotData::")),
										3,
										listOf(
												EventPayload("aggregate1", "::kind::", 1L, "::user 1::", Binary("::data::")),
												EventPayload("aggregate1", "::kind::", 1L, "::user 1::", Binary("::data2::"))
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
		eventStore.saveEvents(SaveEventsRequest("tenant1", "Invoice_aggregate1", "Invoice",
				listOf(EventPayload("aggregate1", "::kind::", 1L, "::user 1::", Binary("::data::")))),
				SaveOptions("::aggregateId::", 0, "::topic::", CreateSnapshot(false))
		)
		
		//fetch the current aggregate value and provide the current version
		val noSnapshotResponse = eventStore.getEventsFromStreams(GetEventsFromStreamsRequest("tenant1", "Invoice_aggregate1")) as GetEventsResponse.Success
		val noSnapshotVersion = noSnapshotResponse.aggregates[0].version
		
		eventStore.saveEvents(SaveEventsRequest("tenant1", "Invoice_aggregate1", "Invoice",
				listOf(EventPayload("aggregate1", "::kind::", 1L, "::user 1::", Binary("::data::")))),
				SaveOptions("::aggregateId::", noSnapshotVersion, "::topic::", CreateSnapshot(true, Snapshot(noSnapshotVersion, Binary("::snapshotData::"))))
		)
		
		//fetch the current aggregate value and provide the current version
		val firstSnapshotResponse = eventStore.getEventsFromStreams(GetEventsFromStreamsRequest("tenant1", "Invoice_aggregate1")) as GetEventsResponse.Success
		val firstSnapshotVersion = firstSnapshotResponse.aggregates[0].version
		
		val response = eventStore.saveEvents(SaveEventsRequest("tenant1", "Invoice_aggregate1", "Invoice",
				listOf(EventPayload("aggregate1", "::kind::", 1L, "::user 1::", Binary("::data2::")))),
				SaveOptions("::aggregateId::", firstSnapshotVersion, "::topic::", CreateSnapshot(true, Snapshot(firstSnapshotVersion, Binary("::snapshotData2::"))))
		) as SaveEventsResponse.Success
		
		val success = eventStore.getEventsFromStreams(GetEventsFromStreamsRequest("tenant1", "Invoice_aggregate1"))
		
		when (success) {
			is GetEventsResponse.Success -> {
				assertThat(success, `is`(equalTo((
						GetEventsResponse.Success(
								listOf(Aggregate(
										"Invoice",
										Snapshot(2, Binary("::snapshotData2::")),
										3,
										listOf(
												EventPayload("aggregate1", "::kind::", 1L, "::user 1::", Binary("::data2::"))
										)
								))
						)
						))))
				
			}
			else -> fail("got unknown response when fetching stored events")
		}
	}
	
	abstract fun createEventStore(): EventStore
}