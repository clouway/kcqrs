package com.clouway.kcqrs.core

/**
 * Key is keeping common functions for storing of the data.
 * 
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */

object StreamKey {
    fun of(aggregateType: String, aggregateId: String) = "${aggregateType}_$aggregateId"
}
