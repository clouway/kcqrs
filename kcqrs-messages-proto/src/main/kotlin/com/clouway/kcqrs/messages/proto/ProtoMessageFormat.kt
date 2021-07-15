package com.clouway.kcqrs.messages.proto

import com.clouway.kcqrs.core.messages.MessageFormat
import com.clouway.kcqrs.core.messages.TypeLookup
import java.io.InputStream
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method


/**
 * ProtoMessageFormat uses the passed EventAdapter instances to adapt newly created events to proto messages
 * that to be saved in the eventstore and loaded proto messages into domain Events.
 * 
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
@Suppress("UNCHECKED_CAST")
class ProtoMessageFormat(vararg params: EventAdapter<*, *>) : MessageFormat {
	// An internal lookup map used for lookups "kind" to adapter.
	var kindToAdapter = params.map { it.type.simpleName to it }.toMap()
	
	override fun isSupporting(kind: String, typeLookup: TypeLookup): Boolean {
		return kindToAdapter.containsKey(kind)
	}
	
	override fun <T> parse(stream: InputStream, kind: String, typeLookup: TypeLookup): T {
		val adapter = kindToAdapter.getValue(kind)
		
		val method: Method
		val protoMessage = try {
			method = adapter.proto.getDeclaredMethod("parseFrom", InputStream::class.java)
			method.invoke(null, stream)
		} catch (e: NoSuchMethodException) {
			throw IllegalStateException("Proto message cannot be parsed", e)
		} catch (e: IllegalAccessException) {
			throw IllegalStateException("Proto message cannot be parsed", e)
		} catch (e: InvocationTargetException) {
			throw IllegalStateException("Proto message cannot be parsed", e)
		}
		return adapter.decode(protoMessage) as T
	}
	
	override fun formatToBytes(value: Any): ByteArray {
		val adapter = kindToAdapter.getValue(value::class.java.simpleName)
		val protoMessage = adapter.encode(value)
		return protoMessage.toByteArray()
	}
}



