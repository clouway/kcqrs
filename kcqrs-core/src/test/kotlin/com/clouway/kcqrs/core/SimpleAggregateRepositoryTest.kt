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
import java.util.*

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class SimpleAggregateRepositoryTest {

    private val configuration = object : Configuration {
        override fun topicName(aggregate: AggregateRoot): String {
            return "::any topic::"
        }
    }

    private val anyIdentity = Identity("::user id::", "tenant1", LocalDateTime.of(2018, 4, 1, 10, 12, 34).toInstant(ZoneOffset.UTC))

    @Test
    fun happyPath() {
        val invoice = Invoice(invoiceId(), "John")
        val eventRepository = SimpleAggregateRepository(InMemoryEventStore(5), TestMessageFormat(), InMemoryEventPublisher(), configuration)
        eventRepository.save(invoice, anyIdentity)

        val loadedInvoice = eventRepository.getById(invoice.getId()!!, Invoice::class.java, anyIdentity)
        assertThat(loadedInvoice.customerName, equalTo("John"))
    }

    @Test
    fun getStreams() {
        val invoice1 = Invoice("invoice1", "John")
        val invoice2 = Invoice("invoice1", "John")
        
        val eventRepository = SimpleAggregateRepository(InMemoryEventStore(5), TestMessageFormat(), InMemoryEventPublisher(), configuration)

        eventRepository.save(invoice1, anyIdentity)
        eventRepository.save(invoice2, anyIdentity)

        val results = eventRepository.getByIds(listOf("invoice1", "invoice2"), Invoice::class.java, anyIdentity)
    }

    @Test(expected = AggregateNotFoundException::class)
    fun notSaveAggregateWithoutEvents() {
        val eventRepository = SimpleAggregateRepository(InMemoryEventStore(5), TestMessageFormat(), InMemoryEventPublisher(), configuration)

        val invoice = Invoice(invoiceId(), "John")
        invoice.markChangesAsCommitted()

        eventRepository.save(invoice, anyIdentity)

        eventRepository.getById(invoice.getId()!!, Invoice::class.java, anyIdentity)
    }

    @Test
    fun applyChangeAndUpdate() {
        val initialInvoice = Invoice(invoiceId(), "John")

        val eventPublisher = InMemoryEventPublisher()
        val eventRepository = SimpleAggregateRepository(InMemoryEventStore(5), TestMessageFormat(), eventPublisher, configuration)
        eventRepository.save(initialInvoice, anyIdentity)

        var invoice = eventRepository.getById(initialInvoice.getId()!!, Invoice::class.java, anyIdentity)

        invoice.changeCustomerName("Peter")
        eventRepository.save(invoice, anyIdentity)

        invoice = eventRepository.getById(invoice.getId()!!, Invoice::class.java, anyIdentity)

        assertThat(invoice.customerName, equalTo("Peter"))
        assertThat(eventPublisher.events.size, equalTo(2))
    }

    @Test
    fun eventsArePublishedAfterSave() {
        val invoice = Invoice(invoiceId(), "John")
        val eventPublisher = InMemoryEventPublisher()
        val eventRepository = SimpleAggregateRepository(
                InMemoryEventStore(5),
                TestMessageFormat(),
                eventPublisher,
                configuration
        )
        eventRepository.save(invoice, anyIdentity)

        assertThat(eventPublisher.events, equalTo(
                listOf(
                        EventWithPayload(
                                InvoiceCreatedEvent(invoice.getId()!!, "John"),
                                Binary("""{"invoiceId":"${invoice.getId()}","customerName":"John"}""")
                        )
                )
        ))
    }

    @Test(expected = EventCollisionException::class)
    fun eventCollision() {
        val invoice = Invoice(invoiceId(), "John")
        val eventStore = InMemoryEventStore(5)
        val eventRepository = SimpleAggregateRepository(eventStore, TestMessageFormat(), InMemoryEventPublisher(), configuration)

        eventStore.pretendThatNextSaveWillReturn(SaveEventsResponse.EventCollision(3L))

        eventRepository.save(invoice, anyIdentity)
    }

    @Test
    fun rollbackEventsIfSendFails() {
        val invoiceId = invoiceId()
        val invoice = Invoice(invoiceId, "John")
        val eventPublisher = InMemoryEventPublisher()
        val eventStore = InMemoryEventStore(5)
        val eventRepository = SimpleAggregateRepository(eventStore, TestMessageFormat(), eventPublisher, configuration)

        eventPublisher.pretendThatNextPublishWillFail()

        try {
            eventRepository.save("Invoice_$invoiceId", invoice, anyIdentity)
            fail("exception wasn't re-thrown when publishing failed?")
        } catch (ex: PublishErrorException) {
            val response = eventStore.getEventsFromStreams(GetEventsFromStreamsRequest(anyIdentity.tenant, "Invoice_$invoiceId")) as GetEventsResponse.Success
            assertThat(response.aggregates.isEmpty(), `is`(true))
        }
    }

    @Test
    fun rollbackOnlyFailedEvents() {
        val invoice = Invoice(invoiceId(), "John")
        val eventStore = InMemoryEventStore(5)
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
            val response = eventStore.getEventsFromStreams(GetEventsFromStreamsRequest(anyIdentity.tenant, "Invoice_${invoice.getId()}")) as GetEventsResponse.Success
            assertThat(response.aggregates[0].events.size, `is`(1))
        }
    }

    @Test(expected = AggregateNotFoundException::class)
    fun getUnknownAggregate() {
        val eventRepository = SimpleAggregateRepository(
                InMemoryEventStore(5),
                TestMessageFormat(),
                InMemoryEventPublisher(),
                configuration
        )

        eventRepository.getById("::any id::", Invoice::class.java, anyIdentity)
    }

    @Test
    fun getMultipleAggregates() {
        val firstInvoice = Invoice(invoiceId(), "John")
        val secondInvoice = Invoice(invoiceId(), "Peter")
        val eventRepository = SimpleAggregateRepository(InMemoryEventStore(5), TestMessageFormat(), InMemoryEventPublisher(), configuration)
        eventRepository.save(firstInvoice, anyIdentity)
        eventRepository.save(secondInvoice, anyIdentity)

        val loadedInvoices = eventRepository.getByIds(listOf(firstInvoice.getId()!!, secondInvoice.getId()!!), Invoice::class.java, anyIdentity)
        assertThat(loadedInvoices, `is`(equalTo(mapOf(
                firstInvoice.getId()!! to firstInvoice,
                secondInvoice.getId()!! to secondInvoice
        ))))
    }

    @Test
    fun getMultipleInvoicesOneFoundOneNot() {
        val firstInvoice = Invoice(invoiceId(), "John")
        val eventRepository = SimpleAggregateRepository(InMemoryEventStore(5), TestMessageFormat(), InMemoryEventPublisher(), configuration)
        eventRepository.save(firstInvoice, anyIdentity)

        val loadedInvoices = eventRepository.getByIds(listOf(firstInvoice.getId()!!, "::any unknown id::"), Invoice::class.java, anyIdentity)
        assertThat(loadedInvoices, `is`(equalTo(mapOf(
                firstInvoice.getId()!! to firstInvoice
        ))))
    }

    @Test
    fun getMultipleAggregatesAndNothingIsReturned() {
        val eventRepository = SimpleAggregateRepository(InMemoryEventStore(5), TestMessageFormat(), InMemoryEventPublisher(), configuration)

        val invoices = eventRepository.getByIds(listOf("::id 1::", "::id 2::"), Invoice::class.java, anyIdentity)
        assertThat(invoices, `is`(equalTo(mapOf())))
    }

    @Test
    fun reachingPersistenceLimitForEventsWillCreateSnapshotAndNewEventEntity() {
        var aggregate = TestAggreagate("::id::", "::string::", 1, TestObject("::value::"), listOf(TestObject("::value2::")))

        val eventPublisher = InMemoryEventPublisher()
        val eventStore = InMemoryEventStore(1)
        val eventRepository = SimpleAggregateRepository(eventStore, TestMessageFormat(), eventPublisher, configuration)
        eventRepository.save(aggregate, anyIdentity)


        assertThat(eventStore.saveEventOptions.last.version, `is`(0L))

        aggregate = eventRepository.getById(aggregate.getId()!!, TestAggreagate::class.java, anyIdentity)

        aggregate.changeLong(123)
        eventRepository.save(aggregate, anyIdentity)

        assertThat(eventStore.saveEventOptions.last.version, `is`(1L))
        assertThat(eventStore.saveEventOptions.last.createSnapshot.snapshot!!.version, `is`(1L))

        aggregate = eventRepository.getById(aggregate.getId()!!, TestAggreagate::class.java, anyIdentity)

        assertThat(aggregate.getExpectedVersion(), `is`(2L)) // after the last event
        assertThat(aggregate.long, equalTo(123L))
    }

    @Test
    fun creatingManySnapshots() {
        val eventPublisher = InMemoryEventPublisher()
        val eventRepository = SimpleAggregateRepository(InMemoryEventStore(1), TestMessageFormat(), eventPublisher, configuration)

        var aggregate = TestAggreagate("::id::", "::string::", 1, TestObject("::value::", Foo("bar")), listOf(TestObject("::value2::", Foo("baar"))))

        eventRepository.save(aggregate, anyIdentity)
        aggregate = eventRepository.getById(aggregate.getId()!!, TestAggreagate::class.java, anyIdentity)

        aggregate.changeLong(123)
        eventRepository.save(aggregate, anyIdentity)
        aggregate = eventRepository.getById(aggregate.getId()!!, TestAggreagate::class.java, anyIdentity)

        aggregate.changeString("newString")
        eventRepository.save(aggregate, anyIdentity)
        aggregate = eventRepository.getById(aggregate.getId()!!, TestAggreagate::class.java, anyIdentity)

        aggregate.changeObject(TestObject("otherValue", Foo("FooBar")))
        eventRepository.save(aggregate, anyIdentity)
        aggregate = eventRepository.getById(aggregate.getId()!!, TestAggreagate::class.java, anyIdentity)

        aggregate.changeList(listOf(TestObject("otherValueInList", Foo("BarFoo"))))
        eventRepository.save(aggregate, anyIdentity)

        aggregate = eventRepository.getById(aggregate.getId()!!, TestAggreagate::class.java, anyIdentity)

        assertThat(aggregate.long, equalTo(123L))
        assertThat(aggregate.string, equalTo("newString"))
        assertThat(aggregate.testObject, equalTo(TestObject("otherValue", Foo("FooBar"))))
        assertThat(aggregate.list[0], equalTo(TestObject("otherValueInList", Foo("BarFoo"))))
    }

    @Test
    fun usingDefaultSnapshotMapper() {
        val eventRepository = SimpleAggregateRepository(InMemoryEventStore(1), TestMessageFormat(), InMemoryEventPublisher(), configuration)
        val id = invoiceId()

        var invoice = Invoice(id, "John")
        eventRepository.save(invoice, anyIdentity)

        invoice.changeCustomerName("Smith")
        eventRepository.save(invoice, anyIdentity)
        invoice = eventRepository.getById(id, Invoice::class.java, anyIdentity)

        invoice.changeCustomerName("Foo")
        eventRepository.save(invoice, anyIdentity)
        invoice = eventRepository.getById(id, Invoice::class.java, anyIdentity)

        assertThat(invoice.customerName, `is`(equalTo("Foo")))
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

data class TestAggreagate private constructor(var string: String, var long: Long, var testObject: TestObject, var list: List<TestObject>) : AggregateRootBase() {
    constructor() : this("", 0, TestObject(), listOf())

    constructor(id: String, string: String, long: Long, testObject: TestObject, list: List<TestObject>) : this(string, long, testObject, list) {
        applyChange(TestClassCreatedEvent(id, string, long, testObject, list))
    }

    fun changeString(newString: String) {
        applyChange(ChangeStringEvent(newString))
    }

    fun changeLong(newLong: Long) {
        applyChange(ChangeLongEvent(newLong))
    }

    fun changeObject(newObject: TestObject) {
        applyChange(ChangeObjectEvent(newObject))
    }

    fun changeList(newList: List<TestObject>) {
        applyChange(ChangeListEvent(newList))
    }

    fun apply(event: TestClassCreatedEvent) {
        aggregateId = event.id
        string = event.string
        long = event.long
        testObject = event.testObject
        list = event.list
    }

    fun apply(event: ChangeLongEvent) {
        long = event.newLong
    }

    fun apply(event: ChangeStringEvent) {
        string = event.newString
    }

    fun apply(event: ChangeObjectEvent) {
        testObject = event.newObject
    }

    fun apply(event: ChangeListEvent) {
        list = event.newList
    }
}

data class TestClassCreatedEvent(val id: String, val string: String, val long: Long, val testObject: TestObject, val list: List<TestObject>) : Event

data class ChangeStringEvent(val newString: String) : Event

data class ChangeLongEvent(val newLong: Long) : Event

data class ChangeObjectEvent(val newObject: TestObject) : Event

data class ChangeListEvent(val newList: List<TestObject>) : Event

data class TestObject(val value: String = "", val innerClass: Foo = Foo())

data class Foo(val bar: String = "")