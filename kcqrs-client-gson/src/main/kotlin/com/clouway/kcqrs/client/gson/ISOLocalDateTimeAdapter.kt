package com.clouway.kcqrs.client.gson

import com.google.gson.*
import java.lang.reflect.Type
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
internal class ISOLocalDateTimeAdapter :
    JsonSerializer<LocalDateTime>,
    JsonDeserializer<LocalDateTime> {
    override fun serialize(
        src: LocalDateTime?,
        type: Type,
        context: JsonSerializationContext,
    ): JsonElement? {
        val dateFormatAsString = DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(src)
        return JsonPrimitive(dateFormatAsString)
    }

    override fun deserialize(
        json: JsonElement?,
        type: Type,
        context: JsonDeserializationContext?,
    ): LocalDateTime? {
        if (json == null) {
            return null
        }
        if (json.isJsonObject) {
            val jsonDate = json.asJsonObject.get("date").asJsonObject
			
            val year = jsonDate.get("year").asInt
            val month = jsonDate.get("month").asInt
            val day = jsonDate.get("day").asInt
			
            val jsonHour = json.asJsonObject.get("time").asJsonObject
            val hour = jsonHour.get("hour").asInt
            val minute = jsonHour.get("minute").asInt
            val second = jsonHour.get("second").asInt
            val nano = jsonHour.get("nano").asInt
			
            return LocalDateTime.of(year, month, day, hour, minute, second, nano)
        }
		
        return DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(json.asString, LocalDateTime::from)
    }
}
