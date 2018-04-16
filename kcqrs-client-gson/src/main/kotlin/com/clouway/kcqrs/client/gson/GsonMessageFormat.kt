package com.clouway.kcqrs.client.gson

import com.clouway.kcqrs.core.messages.MessageFormat
import com.google.gson.GsonBuilder
import java.io.InputStream
import java.io.InputStreamReader
import java.lang.reflect.Type
import java.time.LocalDate
import java.time.LocalDateTime

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
internal class GsonMessageFormat : MessageFormat {
    private val gson = GsonBuilder()
            .registerTypeAdapter(LocalDate::class.java, ISOLocalDateAdapter())
            .registerTypeAdapter(LocalDateTime::class.java, ISOLocalDateTimeAdapter())
            .create()

    override fun <T> parse(stream: InputStream, type: Type): T {
        return gson.fromJson<T>(InputStreamReader(stream, Charsets.UTF_8), type)
    }

    override fun format(value: Any): String {
        return gson.toJson(value)
    }
}