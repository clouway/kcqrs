package com.clouway.kcqrs.core

import org.hamcrest.CoreMatchers.`is`
import org.hamcrest.CoreMatchers.equalTo
import org.junit.Assert.assertThat
import org.junit.Test

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
abstract class IdGeneratorContractTest {

    @Test
    fun sequentialIdsAreGenerated() {
        val generator = createSequenceGenerator()
        val id1 = generator.nextId()
        val id2 = generator.nextId()
        assertThat(id1 < id2, `is`(true))
    }

    @Test
    fun generateFewIds() {
        val generator = createSequenceGenerator()

        val ids = mutableListOf<Long>()
        for (i in 1..10) {
            ids.add(generator.nextId())
        }

        val ascComparator = naturalOrder<Long>()
        val sortedIds = ids.toMutableList().sortedWith(ascComparator)

        assertThat(ids, `is`(equalTo(sortedIds)))
    }

    @Test
    fun generateBulkIds() {
        val generator = createSequenceGenerator()
        val ids = generator.nextIds(10)

        assertThat(ids.size, `is`(equalTo(10)))
    }

    abstract fun createSequenceGenerator(): IdGenerator

}