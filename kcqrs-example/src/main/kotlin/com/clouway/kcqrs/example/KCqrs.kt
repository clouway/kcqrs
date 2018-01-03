package com.clouway.kcqrs.example

import com.clouway.kcqrs.adapter.appengine.AppEngineEventStore
import com.clouway.kcqrs.adapter.appengine.TaskQueueEventPublisher
import com.clouway.kcqrs.core.EventRepository
import com.clouway.kcqrs.core.MessageBus
import com.clouway.kcqrs.core.SimpleMessageBus

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
object KCqrs {
    private var messageBus = SimpleMessageBus()
    private var eventStore = AppEngineEventStore()
    private var eventRepository = EventRepository(eventStore, TaskQueueEventPublisher("/worker/kcqrs"))

    fun messageBus() : MessageBus {
        return messageBus
    }

    fun eventRepository() : EventRepository {
        return eventRepository
    }
}