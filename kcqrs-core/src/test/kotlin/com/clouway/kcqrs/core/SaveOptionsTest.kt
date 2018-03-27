package com.clouway.kcqrs.core

import org.hamcrest.CoreMatchers.*
import org.junit.Assert.assertThat
import org.junit.Test

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class SaveOptionsTest {

    @Test
    fun aggregateIdIsSpecified() {
        val saveOptions = SaveOptions(aggregateId = "::id::")

        assertThat(saveOptions.aggregateId, `is`(equalTo("::id::")))
        assertThat(saveOptions.aggregateId, `is`(equalTo("::id::")))
    }

    @Test
    fun aggregateIdIsNotSpecified() {
        val saveOptions = SaveOptions()

        assertThat(saveOptions.aggregateId, `is`(equalTo(saveOptions.aggregateId)))
    }

    @Test
    fun aggregateIdIsUniquePerInstance() {
        val saveOptions1 = SaveOptions()
        val saveOptions2 = SaveOptions()

        assertThat(saveOptions1.aggregateId, `is`(not(equalTo(saveOptions2.aggregateId))))
    }
}