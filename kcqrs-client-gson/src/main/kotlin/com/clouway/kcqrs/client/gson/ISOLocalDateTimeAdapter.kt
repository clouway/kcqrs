package com.clouway.kcqrs.client.gson

import com.google.gson.*
import java.lang.reflect.Type
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
internal class ISOLocalDateTimeAdapter : JsonSerializer<LocalDateTime>, JsonDeserializer<LocalDateTime> {

    override fun serialize(src: LocalDateTime?, type: Type, context: JsonSerializationContext): JsonElement? {
        val dateFormatAsString = DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(src)
        return JsonPrimitive(dateFormatAsString)
    }

    override fun deserialize(json: JsonElement, type: Type, context: JsonDeserializationContext?): LocalDateTime? {
        return DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(json.asString, LocalDateTime::from)
    }
}