package com.clouway.kcqrs.core

/**
 * Kcqrs is an root class that
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
interface Kcqrs {

    fun messageBus(): MessageBus

    fun repository(): Repository
    
}