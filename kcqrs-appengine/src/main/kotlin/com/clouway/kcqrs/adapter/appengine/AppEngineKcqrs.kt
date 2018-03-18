package com.clouway.kcqrs.adapter.appengine

import com.clouway.kcqrs.core.*
import com.clouway.kcqrs.core.messages.MessageFormatFactory

/**
 * AppEngineKcqrs is an AppEngine implementation of Kcqrs which uses the GAE datastore for persistence of events
 * and TaskQueues for there handling.
 *
 * In most cases the events produced from commands may be published to messager broker like Pub-Sub by using the Intercetors
 * in the MessageBus. More information can be taken from Mess
 *
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class AppEngineKcqrs(eventKind: String = "Event", eventHandlerEndpoint: String, messageFormatFactory: MessageFormatFactory) : Kcqrs {
    private val messageBus = SimpleMessageBus()
    private val eventStore = AppEngineEventStore(eventKind, messageFormatFactory.createMessageFormat())
    private val eventPublisher = TaskQueueEventPublisher(eventHandlerEndpoint)
    private val repository = EventRepository(eventStore, eventPublisher)

    override fun messageBus(): MessageBus {
        return messageBus
    }

    override fun repository(): Repository {
        return repository
    }
}