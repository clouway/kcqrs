package com.clouway.kcqrs.client.gson

import com.clouway.kcqrs.core.messages.TypeLookup
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
    private val messageFormat =
        GsonMessageFormatFactory(
            User::class.java,
            DummyLocalDateTime::class.java,
            DummyLocalDate::class.java,
        ).createMessageFormat()

    @Test
    fun parseFormattedMessage() {
        val msg = messageFormat.formatToBytes(User("Emilia Clark", "Daenerys Targaryen"))
		
        val user =
            messageFormat.parse<User>(
                ByteArrayInputStream(msg),
                "User",
                object : TypeLookup {
                    override fun lookup(kind: String): Class<*> = User::class.java
                },
            )
        assertThat(user.name, `is`(equalTo("Emilia Clark")))
        assertThat(user.nickname, `is`(equalTo("Daenerys Targaryen")))
    }

    @Test
    fun encodesLocalDateTimeInISO8601Format() {
        val jsonPayload = messageFormat.formatToBytes(DummyLocalDateTime(LocalDateTime.of(2018, 4, 15, 12, 3, 4)))
        val nullPayload = messageFormat.formatToBytes(DummyLocalDateTime(null))
        assertThat(jsonPayload.toString(Charsets.UTF_8), `is`(equalTo("""{"issuedOn":"2018-04-15T12:03:04"}""")))
        assertThat(nullPayload.toString(Charsets.UTF_8), `is`(equalTo("{}")))
    }

    @Test
    fun decodeLocalDateTimeFromISO8601Format() {
        val content =
            messageFormat.parse<DummyLocalDateTime>(
                ByteArrayInputStream("""{"issuedOn":"2018-04-15T11:04:05"}""".toByteArray(Charsets.UTF_8)),
                "DummyLocalDateTime",
                object : TypeLookup {
                    override fun lookup(kind: String): Class<*> = DummyLocalDateTime::class.java
                },
            )
		
        assertThat(content, `is`(equalTo(DummyLocalDateTime(LocalDateTime.of(2018, 4, 15, 11, 4, 5)))))
    }

    @Test
    fun decodeLocalDateTimeFromLegacySerialization() {
        val content =
            messageFormat.parse<DummyLocalDateTime>(
                ByteArrayInputStream(
                    """{"issuedOn":{"date":{"year":2019,"month":7,"day":24},"time":{"hour":11,"minute":11,"second":21,"nano":7000000}}}"""
                        .toByteArray(
                            Charsets.UTF_8,
                        ),
                ),
                "DummyLocalDateTime",
                object : TypeLookup {
                    override fun lookup(kind: String): Class<*> = DummyLocalDateTime::class.java
                },
            )
				
        assertThat(content, `is`(equalTo(DummyLocalDateTime(LocalDateTime.of(2019, 7, 24, 11, 11, 21, 7000000)))))
    }

    @Test
    fun using24HourFormat() {
        val jsonPayload = messageFormat.formatToBytes(DummyLocalDateTime(LocalDateTime.of(2018, 4, 15, 16, 3, 4)))
        assertThat(jsonPayload.toString(Charsets.UTF_8), `is`(equalTo("""{"issuedOn":"2018-04-15T16:03:04"}""")))
    }

    @Test
    fun decodeNullLocalDateTime() {
        val content =
            messageFormat.parse<DummyLocalDateTime>(
                ByteArrayInputStream("""{"issuedOn":null}""".toByteArray(Charsets.UTF_8)),
                "DummyLocalDateTime",
                object : TypeLookup {
                    override fun lookup(kind: String): Class<*> = DummyLocalDateTime::class.java
                },
            )
		
        assertThat(content.issuedOn, `is`(nullValue()))
    }

    @Test
    fun encodeLocalDateInISO8601Format() {
        val jsonPayload = messageFormat.formatToBytes(DummyLocalDate(LocalDate.of(2018, 4, 15)))
        val nullPayload = messageFormat.formatToBytes(DummyLocalDate(null))
        assertThat(jsonPayload.toString(Charsets.UTF_8), `is`(equalTo("""{"issuedOn":"2018-04-15"}""")))
        assertThat(nullPayload.toString(Charsets.UTF_8), `is`(equalTo("{}")))
    }

    @Test
    fun decodeLocalDateFromISO8601Format() {
        val content =
            messageFormat.parse<DummyLocalDate>(
                ByteArrayInputStream("""{"issuedOn":"2018-06-23"}""".toByteArray(Charsets.UTF_8)),
                "DummyLocalDate",
                object : TypeLookup {
                    override fun lookup(typeName: String): Class<*> = DummyLocalDate::class.java
                },
            )
		
        assertThat(content, `is`(equalTo(DummyLocalDate(LocalDate.of(2018, 6, 23)))))
    }
}

internal data class DummyLocalDateTime(
    val issuedOn: LocalDateTime?,
)

internal data class DummyLocalDate(
    val issuedOn: LocalDate?,
)

internal data class User(
    @JvmField val name: String,
    @JvmField val nickname: String,
)
