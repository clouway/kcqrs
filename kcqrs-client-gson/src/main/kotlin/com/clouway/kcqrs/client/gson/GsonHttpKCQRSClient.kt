package com.clouway.kcqrs.client.gson

import com.clouway.kcqrs.client.HttpEventStore
import com.clouway.kcqrs.client.SyncEventPublisher
import com.clouway.kcqrs.core.*
import com.google.api.client.http.HttpTransport
import com.google.api.client.json.gson.GsonFactory
import java.net.URL

/**
 * GsonHttpKCQRSClient is responsible for the creation of the HTTP client which provides HTTP RPC capabilities for working
 * with a remote event store and is using GSON for formatting of messages.
 *
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class GsonHttpKCQRSClient(private val messageBus: MessageBus,
                          private val eventStore: EventStore,
                          private val aggregateRepository: AggregateRepository) : Kcqrs {

    override fun messageBus(): MessageBus {
        return messageBus
    }

    override fun eventStore(): EventStore {
        return eventStore
    }

    override fun repository(): AggregateRepository {
        return aggregateRepository
    }

    class Builder(private val configuration: Configuration,
                  private val endpoint: URL,
                  private val transport: HttpTransport) {
        val messageBus: MessageBus = SimpleMessageBus()

        var identityProvider: IdentityProvider = IdentityProvider.Default()
        var eventPublisher: EventPublisher = SyncEventPublisher(messageBus)

        fun build(init: Builder.() -> Unit): Kcqrs {
            init()

            val eventStore = HttpEventStore(
                    endpoint,
                    transport.createRequestFactory { request ->
                        request.parser = GsonFactory.getDefaultInstance().createJsonObjectParser()
                    }
            )

            val messageBus = SimpleMessageBus()
            val messageFormat = GsonMessageFormatFactory().createMessageFormat()

            val aggregateRepository = SimpleAggregateRepository(eventStore, messageFormat, eventPublisher, configuration)

            return GsonHttpKCQRSClient(messageBus, eventStore, AuthoredAggregateRepository(identityProvider, aggregateRepository))
        }

    }
}

