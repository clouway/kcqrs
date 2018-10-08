package com.clouway.kcqrs.testing

import com.clouway.kcqrs.core.Binary
import com.clouway.kcqrs.core.Command
import com.clouway.kcqrs.core.Event
import com.clouway.kcqrs.core.EventWithPayload
import org.hamcrest.CoreMatchers.`is`
import org.hamcrest.CoreMatchers.equalTo
import org.junit.Assert.assertThat
import org.junit.Test

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class InMemoryMessageBusTest {

    @Test
    fun handleCommands() {
        val messageBus = InMemoryMessageBus()
        val command = DummyCommand()
        messageBus.send(command)

        assertThat(messageBus.sentCommands[0], `is`(equalTo(command as Command)))
    }

    @Test
    fun handleEvents() {
        val messageBus = InMemoryMessageBus()
        messageBus.handle(EventWithPayload(DummyEvent(), Binary("::payload::")))
        assertThat(messageBus.handledEvents[0].payload.payload, `is`(equalTo("::payload::".toByteArray())))
    }

    class DummyCommand : Command
    class DummyEvent : Event
}