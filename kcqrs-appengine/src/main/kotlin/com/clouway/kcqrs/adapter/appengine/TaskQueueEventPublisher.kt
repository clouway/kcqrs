package com.clouway.kcqrs.adapter.appengine

import com.clouway.kcqrs.core.EventPublisher
import com.clouway.kcqrs.core.EventWithPayload
import com.google.appengine.api.taskqueue.Queue
import com.google.appengine.api.taskqueue.QueueFactory
import com.google.appengine.api.taskqueue.TaskOptions
import java.util.*

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
internal class TaskQueueEventPublisher(private val handlerEndpoint: String,
                                       private val queueName: String? = null) : EventPublisher {
    override fun publish(events: Iterable<EventWithPayload>) {


        val tasks = events.map {
            TaskOptions.Builder.withUrl(handlerEndpoint)
                    .method(TaskOptions.Method.POST)
                    .param("type", it.event::class.java.name)
                    .param("payload", Base64.getEncoder().encodeToString(it.payload.payload))
        }

        if (tasks.isEmpty()) {
            return
        }

        val queue: Queue = if (queueName == null) {
            QueueFactory.getDefaultQueue()
        } else {
            QueueFactory.getQueue(queueName)
        }

        queue.add(tasks)
    }
}