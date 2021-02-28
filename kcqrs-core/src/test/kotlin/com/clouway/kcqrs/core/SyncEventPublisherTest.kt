package com.clouway.kcqrs.client

import com.clouway.kcqrs.core.Binary
import com.clouway.kcqrs.core.Event
import com.clouway.kcqrs.core.EventWithPayload
import com.clouway.kcqrs.core.MessageBus
import com.clouway.kcqrs.core.PublishErrorException
import com.clouway.kcqrs.core.SyncEventPublisher
import com.clouway.kcqrs.testing.InMemoryMessageBus
import org.hamcrest.Matchers.`is`
import org.hamcrest.Matchers.containsInAnyOrder
import org.hamcrest.Matchers.equalTo
import org.jmock.AbstractExpectations
import org.jmock.Expectations
import org.jmock.integration.junit4.JUnitRuleMockery
import org.junit.Assert.assertThat
import org.junit.Assert.fail
import org.junit.Rule
import org.junit.Test

/**
 * @author Vasil Mitov <vasil.mitov></vasil.mitov>@clouway.com>
 */
class SyncEventPublisherTest {

    @Rule
    @JvmField
    val context = JUnitRuleMockery()

    private val mockedMessageBus = context.mock(MessageBus::class.java)

    @Test
    fun handleEvents() {
        val messageBus = InMemoryMessageBus()
        val syncEventPublisher = SyncEventPublisher(messageBus)
        val firstEvent = EventWithPayload(MyEvent("Foo"), Binary("::payload::"))
        val secondEvent = EventWithPayload(MyEvent("Bar"), Binary("::otherPayload::"))
        syncEventPublisher.publish(listOf(
                firstEvent,
                secondEvent
        ))

        assertThat(messageBus.handledEvents, containsInAnyOrder(firstEvent, secondEvent))
    }

    @Test
    fun handlingTheMessageThrowsException() {
        val syncEventPublisher = SyncEventPublisher(mockedMessageBus)
        val firstEvent = EventWithPayload(MyEvent("Foo"), Binary("::payload::"))

        context.checking(object : Expectations() {
            init {
                oneOf(mockedMessageBus).handle(firstEvent)
                will(AbstractExpectations.throwException(MyException("Some message!")))
            }
        })
        try {
            syncEventPublisher.publish(listOf(firstEvent))
            fail("Exception was not thrown")
        } catch (e: PublishErrorException) {
            assertThat(e.reason, `is`(equalTo(MyException("Some message!") as Exception)))
        }
    }
}

data class MyException(override val message: String) : Exception(message)

data class MyEvent(val foo: String) : Event