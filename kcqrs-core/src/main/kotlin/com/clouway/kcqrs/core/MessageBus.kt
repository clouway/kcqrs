package com.clouway.kcqrs.core

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
interface MessageBus {

    /**
     * Register a command handler
     *
     * @param aClass
     * @param handler
     */
    fun <T : Command> registerCommandHandler(aClass: Class<T>, handler: CommandHandler<T>)

    /**
     * Register an event handler
     *
     * @param aClass
     * @param handler
     */
    fun  <T : Event> registerEventHandler(aClass: Class<T>, handler: EventHandler<T>)

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
     */
    fun <T : Event> handle(event: T)

}

interface Interceptor {

  interface Chain {

    fun event(): Event

    fun proceed(event: Event)

  }

  fun intercept(chain: Chain)

}

class SimpleChain(val event: Event, private val eventHandlers: List<EventHandler<Event>>) : Interceptor.Chain {
    override fun event(): Event {
        return event
    }

    override fun proceed(event: Event) {
        eventHandlers.forEach {
            it.handle(event)
        }
    }

}

    