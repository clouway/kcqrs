package com.clouway.kcqrs.core


import com.clouway.kcqrs.testing.InMemoryEventPublisher
import com.clouway.kcqrs.testing.InMemoryEventStore
import org.hamcrest.CoreMatchers.`is`
import org.hamcrest.CoreMatchers.equalTo
import org.junit.Assert.assertThat
import org.junit.Assert.fail
import org.junit.Test
import java.math.BigDecimal
import java.util.*

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class EventRepositoryTest {

    @Test
    fun happyPath() {
        val invoice = Invoice(UUID.randomUUID(), "John")
        val eventRepository = EventRepository(InMemoryEventStore(), InMemoryEventPublisher())
        eventRepository.save(invoice)

        val loadedInvoice = eventRepository.getById(invoice.getId()!!, Invoice::class.java)
        assertThat(loadedInvoice.customerName, equalTo("John"))
    }

    @Test
    fun applyChangeAndUpdate() {
        val initialInvoice = Invoice(UUID.randomUUID(), "John")

        val eventPublisher = InMemoryEventPublisher()
        val eventRepository = EventRepository(InMemoryEventStore(), eventPublisher)
        eventRepository.save(initialInvoice)

        var invoice = eventRepository.getById(initialInvoice.getId()!!, Invoice::class.java)

        invoice.changeCustomerName("Peter")
        eventRepository.save(invoice)

        invoice = eventRepository.getById(invoice.getId()!!, Invoice::class.java)

        assertThat(invoice.customerName, equalTo("Peter"))
        assertThat(eventPublisher.events.size, equalTo(2))
    }

    @Test
    fun eventsArePublishedAfterSave() {
        val invoice = Invoice(UUID.randomUUID(), "John")
        val eventPublisher = InMemoryEventPublisher()
        val eventRepository = EventRepository(InMemoryEventStore(), eventPublisher)
        eventRepository.save(invoice)

        assertThat(eventPublisher.events, equalTo(listOf<Event>(InvoiceCreatedEvent(invoice.getId()!!, "John"))))
    }

    @Test
    fun rollbackEventsIfSendFails() {
        val invoice = Invoice(UUID.randomUUID(), "John")
        val eventPublisher = InMemoryEventPublisher()
        val eventStore = InMemoryEventStore()
        val eventRepository = EventRepository(eventStore, eventPublisher)

        eventPublisher.pretendThatNextPublishWillFail()

        try {
            eventRepository.save(invoice)
            fail("exception wasn't re-thrown when publishing failed?")
        } catch (ex: PublishErrorException) {
            val events = eventStore.getEvents(invoice.getId()!!, Invoice::class.java)
            assertThat(events.toList().isEmpty(), `is`(true))
        }
    }

    @Test
    fun rollbackOnlyFailedEvents() {
        val invoice = Invoice(UUID.randomUUID(), "John")
        val eventStore = InMemoryEventStore()
        val eventPublisher = InMemoryEventPublisher()
        val eventRepository = EventRepository(eventStore, eventPublisher)

        eventRepository.save(invoice)

        invoice.changeCustomerName("Peter")
        
        eventPublisher.pretendThatNextPublishWillFail()
        try {
            eventRepository.save(invoice)
            fail("exception wasn't re-thrown when publishing failed?")
        } catch (ex: PublishErrorException) {
            val events = eventStore.getEvents(invoice.getId()!!, Invoice::class.java)
            assertThat(events.toList().size, `is`(1))
        }
    }

    data class InvoiceCreatedEvent(@JvmField val invoiceId: UUID, @JvmField val customerName: String) : Event

    data class ChangeCustomerName(@JvmField val invoiceId: UUID, @JvmField val newCustomerName: String) : Event

    class Invoice private constructor(@JvmField var customerName: String, @JvmField val amount: BigDecimal) : AggregateRootBase() {

        constructor() : this("", BigDecimal.ZERO)

        constructor(id: UUID, customerName: String) : this(customerName, BigDecimal.ZERO) {
            applyChange(InvoiceCreatedEvent(id, customerName))
        }

        fun changeCustomerName(customerName: String) {
            applyChange(ChangeCustomerName(getId()!!, customerName))
        }

        fun apply(event: InvoiceCreatedEvent) {
            uuid = event.invoiceId
            customerName = event.customerName
        }

        fun apply(event: ChangeCustomerName) {
            customerName = event.newCustomerName
        }
    }

}