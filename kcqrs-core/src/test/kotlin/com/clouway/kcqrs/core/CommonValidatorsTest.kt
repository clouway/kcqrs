package com.clouway.kcqrs.core

import org.junit.Assert.fail
import org.junit.Test

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class CommonValidatorsTest {
    
    @Test
    fun emailValidation() {
        val cases = listOf(
                Case("m@m.com", true),
                Case("m.m@test.com", true),
                Case("m.m@t.com", true),
                Case("m@.co", false),
                Case("m@", false),
                Case("mk12()*@gmail.com", false),
                Case("m", false)
        )

        cases.forEach {
            val valid = anEmailAddress.validate(it.email)
            if (valid != it.expected) {
                fail("expected validation of ${it.email} to be ${it.expected} but was $valid")
            }
        }
    }
}

internal data class Case(val email: String, val expected: Boolean)