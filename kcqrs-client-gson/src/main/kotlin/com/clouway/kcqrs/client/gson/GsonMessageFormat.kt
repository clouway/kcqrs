package com.clouway.kcqrs.client.gson

import com.clouway.kcqrs.core.messages.MessageFormat
import com.google.gson.GsonBuilder
import java.io.InputStream
import java.io.InputStreamReader
import java.time.LocalDate
import java.time.LocalDateTime

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
internal class GsonMessageFormat(vararg types: Class<*>) : MessageFormat {
	private val gson = GsonFactory.create()
	
	private val kindToType = mutableMapOf<String, Class<*>>()
	
	init {
		types.forEach {
			kindToType[it.simpleName] = it
		}
	}
	
	override fun isSupporting(kind: String): Boolean {
		return kindToType.containsKey(kind)
	}
	
	@Suppress("UNCHECKED_CAST")
	override fun <T> parse(stream: InputStream, kind: String): T {
		val type = kindToType.getValue(kind)
		return gson.fromJson(InputStreamReader(stream, Charsets.UTF_8), type) as T
	}
	
	override fun formatToBytes(value: Any): ByteArray {
		return gson.toJson(value).toByteArray(Charsets.UTF_8)
	}
}