package com.clouway.kcqrs.adapter.appengine

import com.clouway.kcqrs.core.*

/**
 * AppEngineKcqrs is an AppEngine implementation of Kcqrs which uses the GAE datastore for persistence of events
 * and TaskQueues for there handling.
 *
 * In most cases the events produced from commands may be published to messager broker like Pub-Sub by using the Intercetors
 * in the MessageBus. More information can be taken from Mess
 *
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class AppEngineKcqrs(eventKind: String = "Event", eventHandlerEndpoint: String) : Kcqrs {
    private val messageBus = SimpleMessageBus()
    private val eventStore = AppEngineEventStore(eventKind)
    private val eventPublisher = TaskQueueEventPublisher(eventHandlerEndpoint)
    private val repository = EventRepository(eventStore, eventPublisher)

    override fun messageBus(): MessageBus {
        return messageBus
    }

    override fun repository(): Repository {
        return repository
    }
}