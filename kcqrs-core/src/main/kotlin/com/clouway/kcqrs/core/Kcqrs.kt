package com.clouway.kcqrs.core

/**
 * Kcqrs is an root class that wires-up all KCQRS related components.
 *
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
interface Kcqrs {

    fun messageBus(): MessageBus

    fun eventStore(): EventStore

    fun repository(): AggregateRepository

}