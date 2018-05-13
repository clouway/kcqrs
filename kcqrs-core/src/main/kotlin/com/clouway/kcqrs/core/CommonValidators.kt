package com.clouway.kcqrs.core

private val emailRegex = kotlin.text.Regex("^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$")

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
val anEmailAddress = object : Validator<String> {
    override fun validate(fieldValue: String): Boolean {
        return fieldValue.matches(emailRegex)
    }
}

fun <T> equal(expected: T?): Validator<T?> {
    return object : Validator<T?> {
        override fun validate(fieldValue: T?): Boolean {
            return expected == fieldValue
        }
    }
}

fun <T> notEqualTo(expected: T?): Validator<T?> {
    return object : Validator<T?> {
        override fun validate(fieldValue: T?): Boolean {
            return expected != fieldValue
        }
    }
}

fun <T : Comparable<T>> greatherThan(expected: T): Validator<T> {
    return object : Validator<T> {
        override fun validate(fieldValue: T): Boolean {
            return fieldValue > expected
        }
    }
}

fun <T : Comparable<T>> lowerThan(expected: T): Validator<T> {
    return object : Validator<T> {
        override fun validate(fieldValue: T): Boolean {
            return fieldValue < expected
        }
    }
}

infix fun <T> T.shouldBe(validator: Validator<T>): Boolean {
    return validator.validate(this)
}
