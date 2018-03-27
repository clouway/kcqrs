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
    private val commandHandlers: MutableMap<String, CommandHandler<Command>> = mutableMapOf()

    /**                  `
     * List of event handlers per event
     */
    private val eventHandlers: MutableMap<String, MutableList<EventHandler<Event>>> = mutableMapOf()

    /**
     * List of event interceptors
     */
    private val interceptors = mutableListOf<Interceptor>()


    @SuppressWarnings("unchecked")
    override fun <T : Command> registerCommandHandler(aClass: Class<T>, handler: CommandHandler<T>) {
        val key = aClass.name

        @Suppress("UNCHECKED_CAST")
        commandHandlers[key] = handler as CommandHandler<Command>
    }

    override fun <T : Event> registerEventHandler(aClass: Class<T>, handler: EventHandler<T>) {
        val key = aClass.name
        if (!eventHandlers.containsKey(key)) {
            eventHandlers.put(key, mutableListOf())
        }

        @Suppress("UNCHECKED_CAST")
        eventHandlers[key]!!.add(handler as EventHandler<Event>)
    }

    override fun registerInterceptor(interceptor: Interceptor) {
        interceptors.add(interceptor)
    }

    override fun <T : Command> send(command: T) {
        val key = command::class.java.name

        if (!commandHandlers.containsKey(key)) {
            return
        }

        val handler = commandHandlers[key] as CommandHandler<T>
        handler.handle(command)
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