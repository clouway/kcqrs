package com.clouway.kcqrs.adapter.appengine

import com.clouway.kcqrs.core.Event
import com.clouway.kcqrs.core.EventPublisher
import com.google.appengine.api.taskqueue.QueueFactory
import com.google.appengine.api.taskqueue.TaskOptions
import com.google.gson.Gson

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class TaskQueueEventPublisher(val handlerEndpoint: String) : EventPublisher {
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

        val queue = QueueFactory.getDefaultQueue()
        queue.add(tasks)
    }

}