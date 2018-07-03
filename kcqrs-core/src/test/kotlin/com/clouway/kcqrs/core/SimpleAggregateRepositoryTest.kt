package com.clouway.kcqrs.core


import com.clouway.kcqrs.testing.InMemoryEventPublisher
import com.clouway.kcqrs.testing.InMemoryEventStore
import com.clouway.kcqrs.testing.TestMessageFormat
import org.hamcrest.CoreMatchers.`is`
import org.hamcrest.CoreMatchers.equalTo
import org.junit.Assert.assertThat
import org.junit.Assert.fail
import org.junit.Test
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.UUID

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class SimpleAggregateRepositoryTest {

    private val configuration = object : Configuration {
        override fun topicName(aggregate: AggregateRoot): String {
            return "::any topic::"
        }
    }

    private val anyIdentity = Identity("::user id::", LocalDateTime.of(2018, 4, 1, 10, 12, 34).toInstant(ZoneOffset.UTC))

    @Test
    fun happyPath() {
        val invoice = Invoice(invoiceId(), "John")
        val eventRepository = SimpleAggregateRepository(InMemoryEventStore(), TestMessageFormat(), InMemoryEventPublisher(), configuration)
        eventRepository.save(invoice, anyIdentity)

        val loadedInvoice = eventRepository.getById(invoice.getId()!!, Invoice::class.java)
        assertThat(loadedInvoice.customerName, equalTo("John"))
    }

    @Test(expected = AggregateNotFoundException::class)
    fun notSaveAggregateWithoutEvents() {
        val eventRepository = SimpleAggregateRepository(InMemoryEventStore(), TestMessageFormat(), InMemoryEventPublisher(), configuration)

        val invoice = Invoice(invoiceId(), "John")
        invoice.markChangesAsCommitted()

        eventRepository.save(invoice, anyIdentity)

        eventRepository.getById(invoice.getId()!!, Invoice::class.java)
    }

    @Test
    fun applyChangeAndUpdate() {
        val initialInvoice = Invoice(invoiceId(), "John")

        val eventPublisher = InMemoryEventPublisher()
        val eventRepository = SimpleAggregateRepository(InMemoryEventStore(), TestMessageFormat(), eventPublisher, configuration)
        eventRepository.save(initialInvoice, anyIdentity)

        var invoice = eventRepository.getById(initialInvoice.getId()!!, Invoice::class.java)

        invoice.changeCustomerName("Peter")
        eventRepository.save(invoice, anyIdentity)

        invoice = eventRepository.getById(invoice.getId()!!, Invoice::class.java)

        assertThat(invoice.customerName, equalTo("Peter"))
        assertThat(eventPublisher.events.size, equalTo(2))
    }

    @Test
    fun eventsArePublishedAfterSave() {
        val invoice = Invoice(invoiceId(), "John")
        val eventPublisher = InMemoryEventPublisher()
        val eventRepository = SimpleAggregateRepository(
                InMemoryEventStore(),
                TestMessageFormat(),
                eventPublisher,
                configuration
        )
        eventRepository.save(invoice, anyIdentity)

        assertThat(eventPublisher.events, equalTo(
                listOf(
                        EventWithPayload(
                                InvoiceCreatedEvent(invoice.getId()!!, "John"),
                                """{"invoiceId":"${invoice.getId()}","customerName":"John"}"""
                        )
                )
        ))
    }

    @Test(expected = EventCollisionException::class)
    fun eventCollision() {
        val invoice = Invoice(invoiceId(), "John")
        val eventStore = InMemoryEventStore()
        val eventRepository = SimpleAggregateRepository(eventStore, TestMessageFormat(), InMemoryEventPublisher(), configuration)

        eventStore.pretendThatNextSaveWillReturn(SaveEventsResponse.EventCollision(invoice.getId()!!, 3L))

        eventRepository.save(invoice, anyIdentity)
    }

    @Test
    fun rollbackEventsIfSendFails() {
        val invoice = Invoice(invoiceId(), "John")
        val eventPublisher = InMemoryEventPublisher()
        val eventStore = InMemoryEventStore()
        val eventRepository = SimpleAggregateRepository(eventStore, TestMessageFormat(), eventPublisher, configuration)

        eventPublisher.pretendThatNextPublishWillFail()

        try {
            eventRepository.save(invoice, anyIdentity)
            fail("exception wasn't re-thrown when publishing failed?")
        } catch (ex: PublishErrorException) {
            val response = eventStore.getEvents(invoice.getId()!!) as GetEventsResponse.Success
            assertThat(response.aggregates[0].events.isEmpty(), `is`(true))
        }
    }

    @Test
    fun rollbackOnlyFailedEvents() {
        val invoice = Invoice(invoiceId(), "John")
        val eventStore = InMemoryEventStore()
        val eventPublisher = InMemoryEventPublisher()
        val eventRepository = SimpleAggregateRepository(
                eventStore,
                TestMessageFormat(),
                eventPublisher,
                configuration
        )

        eventRepository.save(invoice, anyIdentity)

        invoice.changeCustomerName("Peter")

        eventPublisher.pretendThatNextPublishWillFail()
        try {
            eventRepository.save(invoice, anyIdentity)
            fail("exception wasn't re-thrown when publishing failed?")
        } catch (ex: PublishErrorException) {
            val response = eventStore.getEvents(invoice.getId()!!) as GetEventsResponse.Success
            assertThat(response.aggregates[0].events.size, `is`(1))
        }
    }

    @Test(expected = AggregateNotFoundException::class)
    fun getUnknownAggregate() {
        val eventRepository = SimpleAggregateRepository(
                InMemoryEventStore(),
                TestMessageFormat(),
                InMemoryEventPublisher(),
                configuration
        )

        eventRepository.getById("::any id::", Invoice::class.java)
    }

    @Test
    fun getMultipleAggregates() {
        val firstInvoice = Invoice(invoiceId(), "John")
        val secondInvoice = Invoice(invoiceId(), "Peter")
        val eventRepository = SimpleAggregateRepository(InMemoryEventStore(), TestMessageFormat(), InMemoryEventPublisher(), configuration)
        eventRepository.save(firstInvoice, anyIdentity)
        eventRepository.save(secondInvoice, anyIdentity)

        val loadedInvoices = eventRepository.getByIds(listOf(firstInvoice.getId()!!, secondInvoice.getId()!!), Invoice::class.java)
        assertThat(loadedInvoices, `is`(equalTo(mapOf(
                firstInvoice.getId()!! to firstInvoice,
                secondInvoice.getId()!! to secondInvoice
        ))))
    }

    @Test
    fun getMultipleInvoicesOneFoundOneNot() {
        val firstInvoice = Invoice(invoiceId(), "John")
        val eventRepository = SimpleAggregateRepository(InMemoryEventStore(), TestMessageFormat(), InMemoryEventPublisher(), configuration)
        eventRepository.save(firstInvoice, anyIdentity)

        val loadedInvoices = eventRepository.getByIds(listOf(firstInvoice.getId()!!, "::any unknown id::"), Invoice::class.java)
        assertThat(loadedInvoices, `is`(equalTo(mapOf(
                firstInvoice.getId()!! to firstInvoice
        ))))
    }
    
    @Test
    fun getMultipleAggregatesAndNothingIsReturned() {
        val eventRepository = SimpleAggregateRepository(InMemoryEventStore(), TestMessageFormat(), InMemoryEventPublisher(), configuration)

        val invoices = eventRepository.getByIds(listOf("::id 1::", "::id 2::"), Invoice::class.java)
        assertThat(invoices, `is`(equalTo(mapOf())))
    }

    private fun invoiceId() = UUID.randomUUID().toString()

    data class InvoiceCreatedEvent(@JvmField val invoiceId: String, @JvmField val customerName: String) : Event

    data class ChangeCustomerName(@JvmField val invoiceId: String, @JvmField val newCustomerName: String) : Event

    data class Invoice private constructor(@JvmField var customerName: String) : AggregateRootBase() {

        constructor() : this("")

        constructor(id: String, customerName: String) : this(customerName) {
            applyChange(InvoiceCreatedEvent(id, customerName))
        }

        fun changeCustomerName(customerName: String) {
            applyChange(ChangeCustomerName(getId()!!, customerName))
        }

        fun apply(event: InvoiceCreatedEvent) {
            aggregateId = event.invoiceId
            customerName = event.customerName
        }

        fun apply(event: ChangeCustomerName) {
            customerName = event.newCustomerName
        }
    }

}