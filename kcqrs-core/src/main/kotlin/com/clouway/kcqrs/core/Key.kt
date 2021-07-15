package com.clouway.kcqrs.core

/**
 * Key is keeping common functions for storing of the data.
 * 
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */

object StreamKey {
    fun of(aggregateType: String, aggregateId: String) = "${aggregateType}_$aggregateId"
    
    fun from(value: String): Key {
        val parts =  value.split("_")
        return Key(parts[0], parts[1])
    }
}

data class Key(val aggregateType: String, val aggregateId: String)
