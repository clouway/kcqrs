package com.clouway.kcqrs.core

/**
 * IdGenerator is a number generator whose goal is to provide ordered numbers for received
 * events.
 * 
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
interface IdGenerator {
    /**
     * Generates a new id for the sequence.
     */
    fun nextId(): Long

    /**
     * Generates a number of sequence ids.
     * @param size the number of ids
     */
    fun nextIds(size: Int): List<Long>
}