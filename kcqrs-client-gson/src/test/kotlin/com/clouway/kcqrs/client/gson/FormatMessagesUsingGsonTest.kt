package com.clouway.kcqrs.client.gson

import org.hamcrest.CoreMatchers.`is`
import org.hamcrest.core.IsEqual.equalTo
import org.junit.Assert.assertThat
import org.junit.Test
import java.io.ByteArrayInputStream

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class FormatMessagesUsingGsonTest {

    @Test
    fun parseFormattedMessage() {
        val messageFormat  = GsonMessageFormatFactory().createMessageFormat()
        val msg = messageFormat.format(User("Emilia Clark", "Daenerys Targaryen"))

        val user = messageFormat.parse<User>(ByteArrayInputStream(msg.toByteArray(Charsets.UTF_8)), User::class.java)
        assertThat(user.name, `is`(equalTo("Emilia Clark")))
        assertThat(user.nickname, `is`(equalTo("Daenerys Targaryen")))
    }
}

data class User(@JvmField val name: String, @JvmField val nickname: String)