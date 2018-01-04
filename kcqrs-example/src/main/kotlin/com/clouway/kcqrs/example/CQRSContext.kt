package com.clouway.kcqrs.example



import com.clouway.kcqrs.adapter.appengine.AppEngineKcqrs
import com.clouway.kcqrs.core.MessageBus
import com.clouway.kcqrs.core.Repository

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
object CQRSContext {
    private var cqrs = AppEngineKcqrs("Event", "/worker/kcqrs")

    fun messageBus() : MessageBus {
        return cqrs.messageBus()
    }

    fun eventRepository() : Repository {
        return cqrs.repository()
    }
}