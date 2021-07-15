package com.clouway.kcqrs.testing

import com.clouway.kcqrs.core.messages.DataModelFormat
import com.clouway.kcqrs.core.messages.MessageFormat
import com.clouway.kcqrs.core.messages.TypeLookup
import com.google.gson.Gson
import java.io.InputStream
import java.io.InputStreamReader
import java.lang.reflect.Type

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */

class TestMessageFormat(vararg types: Class<*>) : MessageFormat, DataModelFormat {
	private val gson = Gson()
	private val kindToType = mutableMapOf<String, Class<*>>()
	
	init {
		types.forEach {
			kindToType[it.simpleName] = it
		}
	}
	
	override fun isSupporting(kind: String, typeLookup: TypeLookup): Boolean {
		return kindToType.containsKey(kind)
	}
	
	@Suppress("UNCHECKED_CAST")
	override fun <T> parse(stream: InputStream, kind: String, typeLookup: TypeLookup): T {
		val type = kindToType.getValue(kind)
		return gson.fromJson(InputStreamReader(stream, Charsets.UTF_8), type) as T
	}
	
	override fun formatToBytes(value: Any): ByteArray {
		return gson.toJson(value).toByteArray(Charsets.UTF_8)
	}
	
	override fun <T> parse(stream: InputStream, type: Type): T {
		return gson.fromJson(InputStreamReader(stream, Charsets.UTF_8), type)
	}
	
	override fun formatToString(value: Any): String {
		return gson.toJson(value)
	}
}