package com.clouway.kcqrs.client.gson

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import java.time.LocalDate
import java.time.LocalDateTime

/**
 * GsonFactory is a factory class used to provide Gson serializer with attached internal type adapters
 * for LocalDate and LocalDateTime.
 * 
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
object GsonFactory {
	fun create(): Gson {
		return GsonBuilder()
			.registerTypeAdapter(LocalDate::class.java, ISOLocalDateAdapter())
			.registerTypeAdapter(LocalDateTime::class.java, ISOLocalDateTimeAdapter())
			.create()
	}
}