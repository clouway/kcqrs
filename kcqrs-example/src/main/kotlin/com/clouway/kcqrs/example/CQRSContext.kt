package com.clouway.kcqrs.example


import com.clouway.kcqrs.adapter.appengine.AppEngineKcqrs
import com.clouway.kcqrs.client.gson.GsonMessageFormatFactory
import com.clouway.kcqrs.core.*

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
object CQRSContext {
    private val messageFormatFactory = GsonMessageFormatFactory()
    private val configuration: Configuration = object : Configuration {
        override fun topicName(aggregate: AggregateRoot): String {
            return "products"
        }
    }

    private var cqrs = AppEngineKcqrs.Builder(configuration, messageFormatFactory).build {
        kind = "Event"
        identityProvider = IdentityProvider.Default()
    }

    fun messageBus(): MessageBus {
        return cqrs.messageBus()
    }

    fun eventRepository(): AggregateRepository {
        return cqrs.repository()
    }
}