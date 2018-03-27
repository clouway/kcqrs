package com.clouway.kcqrs.client.jmock

import org.jmock.Expectations
import org.jmock.integration.junit4.JUnitRuleMockery
import java.util.*

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
fun JUnitRuleMockery.expecting(block: MyExpectations.() -> Unit) {
    this.checking(MyExpectations().apply(block))
}

fun List<ByteArray>.isEqualWith(b: List<ByteArray>): Boolean {
    if (this.size != b.size) {
        return false
    }
    for ((index, value) in this.withIndex()) {
        if (!Arrays.equals(value, b[index])) {
             return false;
        }
    }
    return true
}