package com.clouway.kcqrs.core

/**
 * MessageBus is the core component in the CQRS architecture which provides method for dispatching of Commands for
 * execution and event handlers which to be
 *
 *
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
interface MessageBus {

    /**
     * Register a command handler
     *
     * @param aClass
     * @param handler
     */
    fun <T : Command> registerCommandHandler(aClass: Class<T>, handler: CommandHandler<T>, validation: Validation<T> = Validation {})

    /**
     * Register an event handler
     *
     * @param aClass
     * @param handler
     */
    fun <T : Event> registerEventHandler(aClass: Class<T>, handler: EventHandler<T>)

    /**
     * Register an interceptor through which all events will be intercepted.
     */
    fun registerInterceptor(interceptor: Interceptor)

    /**
     * Execute a command
     *
     * @param command
     * @throws EventCollisionException
     * @throws HydrationException
     * @throws AggregateNotFoundException
     */
    @Throws(EventCollisionException::class, HydrationException::class, AggregateNotFoundException::class)
    fun <T : Command> send(command: T)

    /**
     * Handles event using the registered event handlers.
     *
     * @throws Exception different exceptions could be thrown when trying to handle the event.
     */
    @Throws(Exception::class)
    fun handle(event: EventWithPayload)

}

interface Interceptor {

    interface Chain {

        fun event(): EventWithPayload

        fun proceed(event: EventWithPayload)

    }

    fun intercept(chain: Chain)

}

class SimpleChain(val event: EventWithPayload, private val eventHandlers: List<EventHandler<Event>>) : Interceptor.Chain {

    override fun event(): EventWithPayload {
        return event
    }

    override fun proceed(event: EventWithPayload) {
        eventHandlers.forEach {
            it.handle(event.event)
        }
    }

}

    