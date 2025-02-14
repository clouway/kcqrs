package com.clouway.kcqrs.client.gson

import com.google.gson.JsonDeserializationContext
import com.google.gson.JsonDeserializer
import com.google.gson.JsonElement
import com.google.gson.JsonPrimitive
import com.google.gson.JsonSerializationContext
import com.google.gson.JsonSerializer
import java.lang.reflect.Type
import java.time.LocalDate
import java.time.format.DateTimeFormatter

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
internal class ISOLocalDateAdapter :
    JsonSerializer<LocalDate>,
    JsonDeserializer<LocalDate> {
    override fun serialize(
        src: LocalDate?,
        typeOfSrc: Type,
        context: JsonSerializationContext,
    ): JsonElement? {
        val dateFormatAsString = DateTimeFormatter.ISO_LOCAL_DATE.format(src)
        return JsonPrimitive(dateFormatAsString)
    }

    override fun deserialize(
        json: JsonElement,
        typeOfT: Type,
        context: JsonDeserializationContext?,
    ): LocalDate? = DateTimeFormatter.ISO_LOCAL_DATE.parse(json.asString, LocalDate::from)
}
