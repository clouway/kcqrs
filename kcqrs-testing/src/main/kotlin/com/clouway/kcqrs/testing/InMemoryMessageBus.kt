package com.clouway.kcqrs.testing

import com.clouway.kcqrs.core.Command
import com.clouway.kcqrs.core.CommandHandler
import com.clouway.kcqrs.core.Event
import com.clouway.kcqrs.core.EventHandler
import com.clouway.kcqrs.core.EventWithPayload
import com.clouway.kcqrs.core.Interceptor
import com.clouway.kcqrs.core.MessageBus
import com.clouway.kcqrs.core.ValidatedCommandHandler
import com.clouway.kcqrs.core.Validation

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class InMemoryMessageBus() : MessageBus {
    val handledEvents = mutableListOf<EventWithPayload>()
    val sentCommands = mutableListOf<Command<Any>>()


    private val commandHandlers: MutableMap<String, ValidatedCommandHandler<Command<Any>, Any>> = mutableMapOf()


    override fun handle(event: EventWithPayload) {
        handledEvents.add(event)
    }

    override fun <T : Command<V>, V> registerCommandHandler(aClass: Class<T>, handler: CommandHandler<T, V>, validation: Validation<T>) {
        val key = aClass.name

        val commandHandler = ValidatedCommandHandler(handler, validation)
        @Suppress("UNCHECKED_CAST")
        commandHandlers[key] = commandHandler as ValidatedCommandHandler<Command<Any>, Any>

    }

    override fun <T : Event> registerEventHandler(aClass: Class<T>, handler: EventHandler<T>) {

    }


    override fun <T : Command<V>, V> send(command: T): V {
        sentCommands.add(command as Command<Any>)

        val key = command::class.java.name

        if (!commandHandlers.containsKey(key)) {
            throw IllegalArgumentException("No proper handler found!")
        }

        val handler = commandHandlers[key] as ValidatedCommandHandler<T, V>

        return handler.handler.handle(command)

    }

    override fun registerInterceptor(interceptor: Interceptor) {

    }
}