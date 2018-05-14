package com.clouway.kcqrs.core

import org.hamcrest.CoreMatchers.`is`
import org.hamcrest.core.IsEqual.equalTo
import org.junit.Assert.assertThat
import org.junit.Test
import java.time.LocalDateTime

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class ValidationTest {

    @Test
    fun happyPath() {
        val confirmUserValidator = Validation<ConfirmUserCommand> {
            "email" {
                be { email shouldBe anEmailAddress } not "email: in format yourname@yourmail.com"
            }

            "amount" {
                be { amount > 0.0 } not "amount: greather then zero"
            }
        }

        val command = ConfirmUserCommand("user11", "testtest.com", 0.0, LocalDateTime.now())
        val errors = confirmUserValidator.validate(command)

        assertThat(errors, `is`(equalTo(
                mapOf(
                        "email" to listOf("email: in format yourname@yourmail.com"),
                        "amount" to listOf("amount: greather then zero")
                )
        )))
    }

    @Test
    fun usingCustomValidator() {
        val anyInstantTime = LocalDateTime.now()

        val confirmUserValidation = Validation<ConfirmUserCommand> {
            "createdOn" {
                be {
                    createOn shouldBe notEqualTo(anyInstantTime)
                } not "createdOn: must be specified"
            }
        }

        val command = ConfirmUserCommand("user11", "test", 0.0, anyInstantTime)
        val errors = confirmUserValidation.validate(command)

        assertThat(errors, `is`(equalTo(
                mapOf(
                        "createdOn" to listOf("createdOn: must be specified")
                )
        )))
    }

    @Test
    fun allValidationsArePassing() {
        val confirmUserValidator = Validation<ConfirmUserCommand> {
            "email" {
                be { email shouldBe anEmailAddress } not "email: in format yourname@yourmail.com"
            }

            "amount" {
                be { amount > 0.0 } not "amount: greather then zero"
            }
        }

        val command = ConfirmUserCommand("user11", "yourname@yourmail.com", 5.0, LocalDateTime.now())
        val errors = confirmUserValidator.validate(command)

        assertThat(errors, `is`(equalTo(mapOf())))
    }
}

data class ConfirmUserCommand(val userId: String, val email: String, val amount: Double, val createOn: LocalDateTime)
