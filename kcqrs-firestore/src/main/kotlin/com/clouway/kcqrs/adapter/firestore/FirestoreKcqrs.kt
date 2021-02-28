package com.clouway.kcqrs.adapter.firestore

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
import com.clouway.kcqrs.core.SyncEventPublisher
import com.clouway.kcqrs.core.messages.DataModelFormat
import com.clouway.kcqrs.core.messages.MessageFormatFactory
import com.google.cloud.firestore.Firestore
import com.google.cloud.firestore.FirestoreOptions

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class FirestoreKcqrs private constructor(
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
	
	class Builder(
    private val projectId: String,
		private val configuration: Configuration,
		private val dataModelFormat: DataModelFormat,
		private val messageFormatFactory: MessageFormatFactory
	) {
		private val messageBus = SimpleMessageBus()
		var firestore: Firestore = FirestoreOptions.newBuilder().setProjectId(projectId).build().service
		var identityProvider: IdentityProvider = IdentityProvider.Default()
		var idGenerator: IdGenerator = IdGenerators.snowflake()
		var eventStore = FirestoreEventStore(firestore, dataModelFormat, idGenerator)
		var eventPublisher: EventPublisher = SyncEventPublisher(SimpleMessageBus())
		
		fun build(init: Builder.() -> Unit): Kcqrs {
			init()
			val aggregateRepository = SimpleAggregateRepository(
				eventStore,
				messageFormatFactory.createMessageFormat(),
				eventPublisher,
				configuration
			
			)
			return FirestoreKcqrs(messageBus, eventStore, AuthoredAggregateRepository(identityProvider, aggregateRepository))
		}
	}
}