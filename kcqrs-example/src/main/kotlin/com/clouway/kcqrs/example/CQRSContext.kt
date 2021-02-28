package com.clouway.kcqrs.example


import com.clouway.kcqrs.adapter.firestore.FirestoreKcqrs
import com.clouway.kcqrs.core.AggregateRepository
import com.clouway.kcqrs.core.AggregateRoot
import com.clouway.kcqrs.core.Configuration
import com.clouway.kcqrs.core.IdentityProvider
import com.clouway.kcqrs.core.MessageBus
import com.clouway.kcqrs.core.messages.DataModelFormat
import com.clouway.kcqrs.example.domain.ProductNameChangedEvent
import com.clouway.kcqrs.example.domain.ProductRegisteredEvent
import com.clouway.kcqrs.messages.proto.Events
import com.google.gson.Gson
import java.io.InputStream
import java.io.InputStreamReader
import java.lang.reflect.Type
import com.clouway.kcqrs.example.pb.ProductProtos as Pb

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
object CQRSContext {
	private val messageFormatFactory = ProtoMessageFactory(
		Events.adapter(
			ProductRegisteredEvent::class.java,
			Pb.ProductRegisteredEvent::class.java,
			{ Pb.ProductRegisteredEvent.newBuilder().setId(it.id).setName(it.name).build() },
			{ ProductRegisteredEvent(it.id, it.name) }
		),
		Events.adapter(
			ProductNameChangedEvent::class.java,
			Pb.ProductNameChangedEvent::class.java,
			{ Pb.ProductNameChangedEvent.newBuilder().setName(it.name).build() },
			{ ProductNameChangedEvent(it.name) }
		)
	)
	
	private val configuration: Configuration = object : Configuration {
		override fun topicName(aggregate: AggregateRoot): String {
			return "products"
		}
	}
	
	private var cqrs = FirestoreKcqrs.Builder(
		System.getenv("FIRESTORE_PROJECT_ID"),
		configuration,
		JsonDataModelFormat(),
		messageFormatFactory
	).build {
		identityProvider = IdentityProvider.Default()
	}
	
	fun messageBus(): MessageBus {
		return cqrs.messageBus()
	}
	
	fun eventRepository(): AggregateRepository {
		return cqrs.repository()
	}
	
	internal class JsonDataModelFormat : DataModelFormat {
		private val gson = Gson()
		override fun <T> parse(stream: InputStream, type: Type): T {
			return gson.fromJson(InputStreamReader(stream), type)
		}
		
		override fun formatToString(value: Any): String {
			return gson.toJson(value)
		}
		
	}
}
