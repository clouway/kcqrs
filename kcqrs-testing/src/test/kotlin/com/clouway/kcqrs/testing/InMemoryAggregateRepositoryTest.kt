package com.clouway.kcqrs.testing

import com.clouway.kcqrs.core.AggregateRootBase
import com.clouway.kcqrs.core.Event
import org.hamcrest.CoreMatchers.`is`
import org.hamcrest.CoreMatchers.equalTo
import org.junit.Assert.assertThat
import org.junit.Test

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class InMemoryAggregateRepositoryTest {
    
    @Test
    fun happyPath() {
        val aggregateRepository = InMemoryAggregateRepository()
        val order = Order("1", "My Customer")

        aggregateRepository.save(order)

        val loaded = aggregateRepository.getById(order.getId()!!, Order::class.java)
        assertThat(loaded.customerName, `is`(equalTo("My Customer")))
    }

    @Test
    fun updateAggregateRoot() {
        val aggregateRepository = InMemoryAggregateRepository()
        val order = Order("1", "My Customer")

        aggregateRepository.save(order)

        val loadedBeforeChange = aggregateRepository.getById(order.getId()!!, Order::class.java)
        loadedBeforeChange.updateCustomer("New Name")
        aggregateRepository.save(loadedBeforeChange)

        val loadedAfterChange = aggregateRepository.getById(order.getId()!!, Order::class.java)

        assertThat(loadedAfterChange.getId(), `is`(equalTo("1")))
        assertThat(loadedAfterChange.customerName, `is`(equalTo("New Name")))
    }

    @Test
    fun getManyAggregates() {
        val aggregateRepository = InMemoryAggregateRepository()

        aggregateRepository.save(Order("1", "Customer A"))
        aggregateRepository.save(Order("2", "Customer B"))

        val loaded = aggregateRepository.getByIds(listOf("1", "2"), Order::class.java)
        assertThat(loaded.size, `is`(2))
        assertThat(loaded["1"]!!.customerName, `is`(equalTo("Customer A")))
        assertThat(loaded["2"]!!.customerName, `is`(equalTo("Customer B")))
    }
}

internal class Order private constructor(var customerName: String) : AggregateRootBase() {
    @Suppress("UNUSED")
    constructor() : this("")

    constructor(id: String, customerName: String) : this(customerName) {
        applyChange(OrderCreatedEvent(id, customerName))
    }

    fun updateCustomer(newName: String) {
        applyChange(CustomerUpdatedEvent(newName))
    }

    fun apply(event: OrderCreatedEvent) {
        aggregateId = event.id
        customerName = event.customerName
    }

    fun apply(event: CustomerUpdatedEvent) {
        customerName = event.newCustomerName
    }
}

data class OrderCreatedEvent(val id: String, val customerName: String) : Event

data class CustomerUpdatedEvent(val newCustomerName: String) : Event