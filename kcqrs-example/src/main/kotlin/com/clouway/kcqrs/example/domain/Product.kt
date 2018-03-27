package com.clouway.kcqrs.example.domain

import com.clouway.kcqrs.core.AggregateRootBase
import com.clouway.kcqrs.core.Event

data class ProductRegisteredEvent(@JvmField val id: String, @JvmField val name: String) : Event

data class ProductNameChangedEvent(@JvmField val name: String) : Event

class Product(@JvmField var name: String) : AggregateRootBase() {

    constructor() : this("")

    constructor(uuid: String, name: String) : this(name) {
        applyChange(ProductRegisteredEvent(uuid, name))
    }

    fun changeName(newName: String) {
        applyChange(ProductNameChangedEvent(newName))
    }

    fun apply(event: ProductRegisteredEvent) {
        this.uuid = event.id
        this.name = event.name
    }

    fun apply(event: ProductNameChangedEvent) {
        this.name = event.name
    }
}