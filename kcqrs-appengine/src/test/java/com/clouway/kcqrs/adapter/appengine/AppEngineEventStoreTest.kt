package com.clouway.kcqrs.adapter.appengine

import com.clouway.kcqrs.core.AggregateRootBase
import com.clouway.kcqrs.core.Event
import com.clouway.kcqrs.core.EventCollisionException
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

    private val eventStore = AppEngineEventStore(messageFormat = TestingJsonFormat())

    @Before
    fun setUp() {
        helper.setUp()
    }

    @After
    fun tearDown() {
        helper.tearDown()
    }


    @Test
    fun getEventsThatAreStored() {
        val aggregateId = UUID.randomUUID()

        val aggregate = User(aggregateId, "John")

        eventStore.saveEvents(aggregateId, 0, aggregate.getUncommittedChanges())

        val stored = eventStore.getEvents(aggregateId, User::class.java)

        assertThat(stored, hasItem(UserCreatedEvent(aggregateId, "John")))
    }

    @Test
    fun detectEventCollisions() {
        val aggregateId = UUID.randomUUID()

        val aggregate = User(aggregateId, "John")

        eventStore.saveEvents(aggregateId, 0, aggregate.getUncommittedChanges())
        try {
            eventStore.saveEvents(aggregateId, 0, aggregate.getUncommittedChanges())
            fail("collision exception was not thrown when version check failed ?")
        } catch (ex: EventCollisionException) {
            assertThat(ex.version, `is`(equalTo(0)))
        }
    }
    

    @Test
    fun multipleEvents() {
        val aggregateId = UUID.randomUUID()

        val user = User(aggregateId, "John")
        user.changeName("Peter")

        eventStore.saveEvents(aggregateId, 0, user.getUncommittedChanges())

        val stored = eventStore.getEvents(aggregateId, User::class.java)

        assertThat(stored, hasItems(
                UserCreatedEvent(aggregateId, "John"),
                UserNameChangeEvent("Peter")
        ))
    }
    
    @Test
    fun multipleEventsAfterGet() {
        val aggregateId = UUID.randomUUID()

        var user = User(aggregateId, "John")
        eventStore.saveEvents(aggregateId, 0, user.getUncommittedChanges())

        val events = eventStore.getEvents(aggregateId, User::class.java)
        user = User(aggregateId, "")
        user.loadFromHistory(events)

        user.changeName("Peter")

        eventStore.saveEvents(aggregateId, 1, user.getUncommittedChanges())

        val stored = eventStore.getEvents(aggregateId, User::class.java)

        assertThat(stored, hasItems(
                UserCreatedEvent(aggregateId, "John"),
                UserNameChangeEvent("Peter")
        ))
    }
    
    class User(id: UUID, initialName: String) : AggregateRootBase() {
        private var name: String? = null

        init {
            applyChange(UserCreatedEvent(id, initialName))
        }

        fun changeName(newName: String) {
            applyChange(UserNameChangeEvent(newName))
        }

        fun apply(event: UserCreatedEvent) {
            uuid = event.id
            name = event.name
        }

        fun apply(event: UserNameChangeEvent) {
            name = event.newName
        }


    }

    data class UserCreatedEvent(val id: UUID?, val name: String) : Event {
        constructor() : this(null,"")
    }

    data class UserNameChangeEvent(val newName: String) : Event {
        constructor() : this("")
    }
}