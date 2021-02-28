package com.clouway.kcqrs.messages.proto

import com.google.protobuf.Message

/**
 * Events is a helper type used to configure Event <-> PB adapters for transformation of the Event messages.
 *
 * Example usage:
 * <pre>
 *  Events.adapter(UserCreated::class.java, Pb.UserCreated::class.java, { Pb.UserCreated(it.name, it.email) }, { UserCreated(it.name, it.email) }
 * </pre>
 */
object Events {
	fun <T, P> adapter(
		eventType: Class<T>,
		protoType: Class<P>,
		encoder: Encoder<T, P>,
		decoder: Decoder<T, P>
	): EventAdapter<T, P> where T : Any, P : Message =
		EventAdapter(eventType, protoType, encoder, decoder)
}

typealias Encoder<T, P> = (T) -> P
typealias Decoder<T, P> = (P) -> T

class EventAdapter<T, P>(
	val type: Class<T>,
	val proto: Class<P>,
	val encoder: Encoder<T, P>,
	val decoder: Decoder<T, P>
) where T : Any, P : Message {
	
	@Suppress("UNCHECKED_CAST")
	fun encode(t: Any): P {
		return encoder(t as T)
	}
	
	@Suppress("UNCHECKED_CAST")
	fun decode(p: Any): T {
		return decoder(p as P)
	}
}
