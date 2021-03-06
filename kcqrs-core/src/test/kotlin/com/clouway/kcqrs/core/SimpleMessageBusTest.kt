package com.clouway.kcqrs.core


import org.hamcrest.CoreMatchers.`is`
import org.hamcrest.CoreMatchers.equalTo
import org.hamcrest.CoreMatchers.nullValue
import org.junit.Assert.assertThat
import org.junit.Assert.fail
import org.junit.Test
import java.util.UUID


/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class SimpleMessageBusTest {

    @Test
    fun handleEventWithSingleHandler() {
        val msgBus = SimpleMessageBus()

        val handler = MyEventHandler()
        msgBus.registerEventHandler(MyEvent::class.java, handler)

        val event = EventWithPayload(MyEvent(UUID.randomUUID()), Binary(""))
        msgBus.handle(event)

        assertThat(handler.lastEvent, `is`(equalTo(event.event)))
    }

    @Test
    fun handleEventWithMultipleHandlers() {
        val msgBus = SimpleMessageBus()

        val firstHandler = MyEventHandler()
        val secondHandler = AnotherHandler()

        msgBus.registerEventHandler(MyEvent::class.java, firstHandler)
        msgBus.registerEventHandler(MyEvent::class.java, secondHandler)

        val event = EventWithPayload(MyEvent(UUID.randomUUID()), Binary(""))
        msgBus.handle(event)

        assertThat(firstHandler.lastEvent, `is`(equalTo(event.event)))
        assertThat(secondHandler.lastEvent, `is`(equalTo(event.event)))

    }

    @Test
    fun noHandlersAreAttached() {
        val msgBus = SimpleMessageBus()
        msgBus.handle(EventWithPayload(MyEvent(UUID.randomUUID()), Binary("")))
    }

    @Test
    fun handleCommandWithCommandHandler() {
        val msgBus = SimpleMessageBus()

        val handler = ChangeCustomerNameHandler()
        msgBus.registerCommandHandler(ChangeCustomerName::class.java, handler)

        val changeCustomerNameAction = ChangeCustomerName("Action")
        msgBus.send(changeCustomerNameAction)

        assertThat(handler.lastCommand, `is`(changeCustomerNameAction))
    }

    @Test
    fun handleCommandWithCommandHandlerAndReturnResponse() {
        val msgBus = SimpleMessageBus()

        val handler = ChangeCustomerNameHandler()
        msgBus.registerCommandHandler(ChangeCustomerName::class.java, handler)

        val changeCustomerNameAction = ChangeCustomerName("Action")
        val response = msgBus.send(changeCustomerNameAction)

        assertThat(response, `is`("OK"))
        assertThat(handler.lastCommand, `is`(changeCustomerNameAction))
    }

    @Test(expected = IllegalArgumentException::class)
    fun noProperHandlerFound() {
        val msgBus = SimpleMessageBus()

        val changeCustomerNameAction = ChangeCustomerName("Action")
        msgBus.send(changeCustomerNameAction)
    }

    @Test
    fun sendIsValidatingReceivedCommand() {
        val msgBus = SimpleMessageBus()

        val handler = ChangeCustomerNameHandler()
        msgBus.registerCommandHandler(ChangeCustomerName::class.java, handler, Validation {
            "name" {
                be {
                    name.length > 5
                } not "name: must be at least 5 characters long"
            }
        })

        val changeCustomerNameAction = ChangeCustomerName("Jo")
        try {
            msgBus.send(changeCustomerNameAction)
            fail("validation was not performed during sending of an invalidation action")
        } catch (ex: ViolationErrorException) {
            assertThat(ex.errors, `is`(equalTo(mapOf("name" to listOf("name: must be at least 5 characters long")))))
            assertThat(handler.lastCommand, `is`(nullValue()))
        }
    }

    @Test(expected = java.lang.IllegalArgumentException::class)
    fun noCommandHandler() {
        val msgBus = SimpleMessageBus()
        msgBus.send(ChangeCustomerName("Action"))
    }

    @Test(expected = java.lang.IllegalArgumentException::class)
    fun commandsAreDispatchedByCommandType() {
        val msgBus = SimpleMessageBus()

        val handler = ChangeCustomerNameHandler()
        msgBus.registerCommandHandler(ChangeCustomerName::class.java, handler)

        msgBus.send(DummyCommand())

        assertThat(handler.lastCommand, `is`(nullValue()))
    }

    @Test
    fun eventIsDispatchedThroughInterceptors() {
        val msgBus = SimpleMessageBus()
        val callLog = mutableListOf<String>()

        msgBus.registerEventHandler(MyEvent::class.java, object : EventHandler<MyEvent> {
            override fun handle(event: MyEvent) {
                callLog.add("called handler")
            }

        })

        msgBus.registerInterceptor(object : Interceptor {
            override fun intercept(chain: Interceptor.Chain) {
                callLog.add("called before")
                chain.proceed(chain.event())
                callLog.add("called after")
            }
        })

        val event = EventWithPayload(MyEvent(UUID.randomUUID()), Binary(""))
        msgBus.handle(event)

        assertThat(callLog, `is`(equalTo(listOf(
                "called before",
                "called handler",
                "called after")
        )))
    }

    @Test
    fun onlyInterceptorIsAttached() {
        val msgBus = SimpleMessageBus()
        val callLog = mutableListOf<String>()

        msgBus.registerInterceptor(object : Interceptor {
            override fun intercept(chain: Interceptor.Chain) {
                callLog.add("called before")
                chain.proceed(chain.event())
                callLog.add("called after")
            }
        })

        val event = EventWithPayload(MyEvent(UUID.randomUUID()), Binary(""))
        msgBus.handle(event)

        assertThat(callLog, `is`(equalTo(listOf(
                "called before",
                "called after")
        )))
    }


    class ChangeCustomerNameHandler : CommandHandler<ChangeCustomerName, String> {

        var lastCommand: ChangeCustomerName? = null

        override fun handle(command: ChangeCustomerName): String {
            lastCommand = command
            return "OK"
        }
    }

    class DummyCommand : Command<String>

    class ChangeCustomerName(val name: String) : Command<String>

    class MyEventHandler : EventHandler<MyEvent> {
        var lastEvent: MyEvent? = null
        override fun handle(event: MyEvent) {
            lastEvent = event
        }
    }

    class AnotherHandler : EventHandler<MyEvent> {
        var lastEvent: MyEvent? = null

        override fun handle(event: MyEvent) {
            lastEvent = event
        }
    }

    class MyEvent(@JvmField val name: UUID) : Event

}