package com.clouway.kcqrs.adapter.appengine

import com.clouway.kcqrs.core.Event
import com.clouway.kcqrs.core.EventPublisher
import com.google.appengine.api.taskqueue.Queue
import com.google.appengine.api.taskqueue.QueueFactory
import com.google.appengine.api.taskqueue.TaskOptions
import com.google.gson.Gson

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
internal class TaskQueueEventPublisher(private val handlerEndpoint: String, private val queueName: String? = null) : EventPublisher {
    private val gson = Gson()
    
    override fun publish(events: Iterable<Event>) {
        val tasks = events.map {
            TaskOptions.Builder.withUrl(handlerEndpoint)
                    .method(TaskOptions.Method.POST)
                    .param("type", it::class.java.name)
                    .param("payload", gson.toJson(it))
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