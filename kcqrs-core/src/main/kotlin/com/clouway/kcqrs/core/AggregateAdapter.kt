package com.clouway.kcqrs.core

import com.clouway.kcqrs.core.messages.TypeLookup

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class AggregateAdapter<T : AggregateRoot>(private val applyCallName: String) : TypeLookup {

    private val supportedEventNameToType = mutableMapOf<String, String>()
    private val supportedEventTypes = mutableMapOf<String, Class<*>>()

    fun fetchMetadata(type: Class<T>) {
        val methods = type.declaredMethods
        methods.forEach {
            if (it.name === applyCallName) {
                it.parameters.forEach {
                    supportedEventNameToType[it.type.simpleName] = it.type.name
                    supportedEventTypes[it.type.simpleName] = it.type
                }
            }
        }
    }

    fun eventType(eventName: String): String? {
        return supportedEventNameToType[eventName]
    }
    
    override fun lookup(kind: String): Class<*>? {
        return supportedEventTypes[kind]
    }
}