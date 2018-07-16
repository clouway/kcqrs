package com.clouway.kcqrs.core

import com.clouway.kcqrs.testing.TestMessageFormat
import org.hamcrest.CoreMatchers.`is`
import org.hamcrest.CoreMatchers.equalTo
import org.hamcrest.MatcherAssert.assertThat
import org.junit.Test
import java.io.ByteArrayInputStream
import java.time.LocalDateTime
import java.time.ZoneOffset

/**
 * @author Vasil Mitov <vasil.mitov></vasil.mitov>@clouway.com>
 */
class AuthoredEventTest {

    @Test
    fun serializeAndDeserializeAuthoredEvent() {
        val instant = LocalDateTime.of(2018, 4, 1, 10, 12, 34).toInstant(ZoneOffset.UTC)
        val anyIdentity = Identity("::user id::", instant)

        val myAuthoredEvent = MyAuthoredEvent("foo")
        myAuthoredEvent.identity = anyIdentity


        val testMessageFormat = TestMessageFormat()

        val format = testMessageFormat.format(myAuthoredEvent)
        val parsedAuthoredEvent = testMessageFormat.parse<MyAuthoredEvent>(ByteArrayInputStream(format.toByteArray(Charsets.UTF_8)), MyAuthoredEvent::class.java)

        assertThat(parsedAuthoredEvent, `is`(equalTo(myAuthoredEvent)))
        assertThat(parsedAuthoredEvent.identity, `is`(equalTo(anyIdentity)))
    }
}

data class MyAuthoredEvent(val foo: String) : AuthoredEvent()