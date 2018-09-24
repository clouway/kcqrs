package com.clouway.kcqrs.client.gson

import org.hamcrest.CoreMatchers.`is`
import org.hamcrest.CoreMatchers.nullValue
import org.hamcrest.core.IsEqual.equalTo
import org.junit.Assert.assertThat
import org.junit.Test
import java.io.ByteArrayInputStream
import java.time.LocalDate
import java.time.LocalDateTime

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class FormatMessagesUsingGsonTest {
    private val messageFormat = GsonMessageFormatFactory().createMessageFormat()

    @Test
    fun parseFormattedMessage() {

        val msg = messageFormat.formatToString(User("Emilia Clark", "Daenerys Targaryen"))

        val user = messageFormat.parse<User>(ByteArrayInputStream(msg.toByteArray(Charsets.UTF_8)), User::class.java)
        assertThat(user.name, `is`(equalTo("Emilia Clark")))
        assertThat(user.nickname, `is`(equalTo("Daenerys Targaryen")))
    }

    @Test
    fun encodesLocalDateTimeInISO8601Format() {
        val jsonPayload = messageFormat.formatToString(DummyLocalDateTime(LocalDateTime.of(2018, 4, 15, 12, 3, 4)))
        val nullPayload = messageFormat.formatToString(DummyLocalDateTime(null))
        assertThat(jsonPayload, `is`(equalTo("""{"issuedOn":"2018-04-15T12:03:04"}""")))
        assertThat(nullPayload, `is`(equalTo("{}")))
    }

    @Test
    fun decodeLocalDateTimeFromISO8601Format() {
        val content = messageFormat.parse<DummyLocalDateTime>(
                ByteArrayInputStream("""{"issuedOn":"2018-04-15T11:04:05"}""".toByteArray(Charsets.UTF_8)),
                DummyLocalDateTime::class.java
        )

        assertThat(content, `is`(equalTo(DummyLocalDateTime(LocalDateTime.of(2018, 4, 15, 11, 4, 5)))))
    }
    
    @Test
    fun using24HourFormat() {
        val jsonPayload = messageFormat.formatToString(DummyLocalDateTime(LocalDateTime.of(2018, 4, 15, 16, 3, 4)))
        assertThat(jsonPayload, `is`(equalTo("""{"issuedOn":"2018-04-15T16:03:04"}""")))
    }

    @Test
    fun decodeNullLocalDateTime() {
        val content = messageFormat.parse<DummyLocalDateTime>(
                ByteArrayInputStream("""{"issuedOn":null}""".toByteArray(Charsets.UTF_8)),
                DummyLocalDateTime::class.java
        )

        assertThat(content.issuedOn, `is`(nullValue()))
    }

    @Test
    fun encodeLocalDateInISO8601Format() {
        val jsonPayload = messageFormat.formatToString(DummyLocalDate(LocalDate.of(2018, 4, 15)))
        val nullPayload = messageFormat.formatToString(DummyLocalDate(null))
        assertThat(jsonPayload, `is`(equalTo("""{"issuedOn":"2018-04-15"}""")))
        assertThat(nullPayload, `is`(equalTo("{}")))
    }

    @Test
    fun decodeLocalDateFromISO8601Format() {
        val content = messageFormat.parse<DummyLocalDate>(
                ByteArrayInputStream("""{"issuedOn":"2018-06-23"}""".toByteArray(Charsets.UTF_8)),
                DummyLocalDate::class.java
        )

        assertThat(content, `is`(equalTo(DummyLocalDate(LocalDate.of(2018, 6, 23)))))
    }

}

internal data class DummyLocalDateTime(val issuedOn: LocalDateTime?)

internal data class DummyLocalDate(val issuedOn: LocalDate?)

internal data class User(@JvmField val name: String, @JvmField val nickname: String)