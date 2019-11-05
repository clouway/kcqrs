package com.clouway.kcqrs.core

/**
 * SimpleMessageBus is representing a simple message bus implementation which uses basic collections for storing and
 * looking for command and event handlers.
 *
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class SimpleMessageBus : MessageBus {

    /**
     * List of command handlers per command
     */
    private val commandHandlers: MutableMap<String, ValidatedCommandHandler<Command<Any>, Any>> = mutableMapOf()

    /**                  `
     * List of event handlers per event
     */
    private val eventHandlers: MutableMap<String, MutableList<EventHandler<Any>>> = mutableMapOf()

    /**
     * List of event interceptors
     */
    private val interceptors = mutableListOf<Interceptor>()


    @SuppressWarnings("unchecked")
    override fun <T : Command<R>, R> registerCommandHandler(aClass: Class<T>, handler: CommandHandler<T,R>, validation: Validation<T>) {
        val key = aClass.name

        val commandHandler = ValidatedCommandHandler(handler, validation)
        @Suppress("UNCHECKED_CAST")
        commandHandlers[key] = commandHandler as ValidatedCommandHandler<Command<Any>, Any>
    }

    override fun <T : Event> registerEventHandler(aClass: Class<T>, handler: EventHandler<T>) {
        val key = aClass.name
        if (!eventHandlers.containsKey(key)) {
            eventHandlers.put(key, mutableListOf())
        }

        @Suppress("UNCHECKED_CAST")
        eventHandlers[key]!!.add(handler as EventHandler<Any>)
    }

    override fun registerInterceptor(interceptor: Interceptor) {
        interceptors.add(interceptor)
    }

    override fun <T : Command<R>, R> send(command: T) : R  {
        val key = command::class.java.name

        if (!commandHandlers.containsKey(key)) {
             throw IllegalArgumentException("No proper handler found!")
        }

        val handler = commandHandlers[key] as ValidatedCommandHandler<T, R>

        val errors = handler.validation.validate(command)
        if (!errors.isEmpty()) {
            throw ViolationErrorException(errors)
        }

        return handler.handler.handle(command)
    }
    
    override fun handle(event: EventWithPayload) {
        val key = event.event::class.java.name

        if (!eventHandlers.containsKey(key)) {
            interceptors.forEach { it.intercept(SimpleChain(event, listOf())) }
            return
        }

        val handlers = eventHandlers[key]!!

        if (!interceptors.isEmpty()) {
            val chain = SimpleChain(event, handlers)
            interceptors.forEach { it.intercept(chain) }
            return
        }

        handlers.forEach {
            it.handle(event.event)
        }
    }


}

data class ValidatedCommandHandler<in T : Command<R>, R>(val handler: CommandHandler<T, R>, val validation: Validation<T>)