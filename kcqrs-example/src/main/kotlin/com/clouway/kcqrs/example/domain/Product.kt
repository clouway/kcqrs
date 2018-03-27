package com.clouway.kcqrs.example.domain

import com.clouway.kcqrs.core.AggregateRootBase
import com.clouway.kcqrs.core.Event
import java.util.*

data class ProductRegisteredEvent(@JvmField val uuid: UUID?, @JvmField val name: String) : Event

data class ProductNameChangedEvent(@JvmField val name: String) : Event

class Product(@JvmField var name: String) : AggregateRootBase() {

    constructor() : this("")

    constructor(uuid: UUID?, name: String) : this(name) {
        applyChange(ProductRegisteredEvent(uuid, name))
    }

    fun changeName(newName: String) {
        applyChange(ProductNameChangedEvent(newName))
    }

    fun apply(event: ProductRegisteredEvent) {
        this.uuid = event.uuid
        this.name = event.name
    }

    fun apply(event: ProductNameChangedEvent) {
        this.name = event.name
    }
}