package com.clouway.kcqrs.adapter.appengine

import com.clouway.kcqrs.core.AggregateRepository
import com.clouway.kcqrs.core.AuthoredAggregateRepository
import com.clouway.kcqrs.core.Configuration
import com.clouway.kcqrs.core.EventPublisher
import com.clouway.kcqrs.core.EventStore
import com.clouway.kcqrs.core.IdGenerator
import com.clouway.kcqrs.core.IdGenerators
import com.clouway.kcqrs.core.IdentityProvider
import com.clouway.kcqrs.core.Kcqrs
import com.clouway.kcqrs.core.MessageBus
import com.clouway.kcqrs.core.SimpleAggregateRepository
import com.clouway.kcqrs.core.SimpleMessageBus
import com.clouway.kcqrs.core.messages.DataModelFormat
import com.clouway.kcqrs.core.messages.MessageFormatFactory


/**
 * AppEngineKcqrs is an AppEngine implementation of Kcqrs which uses the GAE datastore for persistence of events
 * and TaskQueues for there handling.
 *
 * In most cases the events produced from commands may be published to messager broker like Pub-Sub by using the Intercetors
 * in the MessageBus.
 *
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class AppEngineKcqrs private constructor(
    private val messageBus: MessageBus,
    private val eventStore: EventStore,
    private val aggregateRepository: AggregateRepository
) : Kcqrs {

    override fun eventStore(): EventStore {
        return eventStore
    }

    override fun messageBus(): MessageBus {
        return messageBus
    }

    override fun repository(): AggregateRepository {
        return aggregateRepository
    }

    class Builder(private val configuration: Configuration,
                  private val dataModelFormat: DataModelFormat,
                  private val messageFormatFactory: MessageFormatFactory
                  ) {
        private val messageBus = SimpleMessageBus()
        var kind: String = "Event"
        var kcqrsHandlerEndpoint = "/worker/kcqrs"
        var queueName: String? = null
        var identityProvider: IdentityProvider = IdentityProvider.Default()
        var idGenerator: IdGenerator = IdGenerators.snowflake()
        var eventStore = AppEngineEventStore(kind, dataModelFormat, idGenerator)
        var eventPublisher: EventPublisher = TaskQueueEventPublisher(kcqrsHandlerEndpoint, queueName)

        fun build(init: Builder.() -> Unit): Kcqrs {
            init()
            val aggregateRepository = SimpleAggregateRepository(
                    eventStore,
                    messageFormatFactory.createMessageFormat(),
                    eventPublisher,
                    configuration

            )
            return AppEngineKcqrs(messageBus, eventStore, AuthoredAggregateRepository(identityProvider, aggregateRepository))
        }
    }
}