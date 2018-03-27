package com.clouway.kcqrs.core

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class AggregateAdapter<T : AggregateRoot>(private val applyCallName: String) {

    private val supportedEventNameToType = mutableMapOf<String, String>()

    fun fetchMetadata(type: Class<T>) {
        val methods = type.declaredMethods
        methods.forEach {
            if (it.name === applyCallName) {
                it.parameters.forEach {
                    supportedEventNameToType[it.type.simpleName] = it.type.name
                }
            }
        }
    }

    fun eventType(eventName: String): String? {
        return supportedEventNameToType[eventName]
    }
}