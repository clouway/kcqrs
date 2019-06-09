package com.clouway.kcqrs.core

import org.hamcrest.CoreMatchers.`is`
import org.junit.Assert.assertThat
import org.junit.Test

/**
 * SnowflakeSequenceGeneratorTest is testing the SnowflakeSequenceGenerator implementation.
 *
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class SnowflakeIdGeneratorTest : IdGeneratorContractTest() {

    @Test
    fun useCustomNodeId() {
        val generator = IdGenerators.snowflake("node1")
        val id1 = generator.nextId()
        val id2 = generator.nextId()
        assertThat(id1 < id2, `is`(true))
    }

    override fun createSequenceGenerator(): IdGenerator {
        return IdGenerators.snowflake()
    }
}