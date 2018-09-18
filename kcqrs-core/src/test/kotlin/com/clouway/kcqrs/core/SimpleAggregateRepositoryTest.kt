package com.clouway.kcqrs.core


import com.clouway.kcqrs.testing.InMemoryEventPublisher
import com.clouway.kcqrs.testing.InMemoryEventStore
import com.clouway.kcqrs.testing.TestMessageFormat
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
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
        val eventRepository = SimpleAggregateRepository(InMemoryEventStore(5), TestMessageFormat(), InMemoryEventPublisher(), configuration)
        eventRepository.save(invoice, anyIdentity)

        val loadedInvoice = eventRepository.getById(invoice.getId()!!, Invoice::class.java)
        assertThat(loadedInvoice.customerName, equalTo("John"))
    }

    @Test(expected = AggregateNotFoundException::class)
    fun notSaveAggregateWithoutEvents() {
        val eventRepository = SimpleAggregateRepository(InMemoryEventStore(5), TestMessageFormat(), InMemoryEventPublisher(), configuration)

        val invoice = Invoice(invoiceId(), "John")
        invoice.markChangesAsCommitted()

        eventRepository.save(invoice, anyIdentity)

        eventRepository.getById(invoice.getId()!!, Invoice::class.java)
    }

    @Test
    fun applyChangeAndUpdate() {
        val initialInvoice = Invoice(invoiceId(), "John")

        val eventPublisher = InMemoryEventPublisher()
        val eventRepository = SimpleAggregateRepository(InMemoryEventStore(5), TestMessageFormat(), eventPublisher, configuration)
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
                                """{"invoiceId":"${invoice.getId()}","customerName":"John"}"""
                        )
                )
        ))
    }

    @Test(expected = EventCollisionException::class)
    fun eventCollision() {
        val invoice = Invoice(invoiceId(), "John")
        val eventStore = InMemoryEventStore(5)
        val eventRepository = SimpleAggregateRepository(eventStore, TestMessageFormat(), InMemoryEventPublisher(), configuration)

        eventStore.pretendThatNextSaveWillReturn(SaveEventsResponse.EventCollision(invoice.getId()!!, 3L))

        eventRepository.save(invoice, anyIdentity)
    }

    @Test
    fun rollbackEventsIfSendFails() {
        val invoice = Invoice(invoiceId(), "John")
        val eventPublisher = InMemoryEventPublisher()
        val eventStore = InMemoryEventStore(5)
        val eventRepository = SimpleAggregateRepository(eventStore, TestMessageFormat(), eventPublisher, configuration)

        eventPublisher.pretendThatNextPublishWillFail()

        try {
            eventRepository.save(invoice, anyIdentity)
            fail("exception wasn't re-thrown when publishing failed?")
        } catch (ex: PublishErrorException) {
            val response = eventStore.getEvents(invoice.getId()!!, Invoice::class.java.simpleName) as GetEventsResponse.Success
            assertThat(response.aggregates[0].events.isEmpty(), `is`(true))
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
            val response = eventStore.getEvents(invoice.getId()!!, Invoice::class.java.simpleName) as GetEventsResponse.Success
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

        eventRepository.getById("::any id::", Invoice::class.java)
    }

    @Test
    fun getMultipleAggregates() {
        val firstInvoice = Invoice(invoiceId(), "John")
        val secondInvoice = Invoice(invoiceId(), "Peter")
        val eventRepository = SimpleAggregateRepository(InMemoryEventStore(5), TestMessageFormat(), InMemoryEventPublisher(), configuration)
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
        val eventRepository = SimpleAggregateRepository(InMemoryEventStore(5), TestMessageFormat(), InMemoryEventPublisher(), configuration)
        eventRepository.save(firstInvoice, anyIdentity)

        val loadedInvoices = eventRepository.getByIds(listOf(firstInvoice.getId()!!, "::any unknown id::"), Invoice::class.java)
        assertThat(loadedInvoices, `is`(equalTo(mapOf(
                firstInvoice.getId()!! to firstInvoice
        ))))
    }

    @Test
    fun getMultipleAggregatesAndNothingIsReturned() {
        val eventRepository = SimpleAggregateRepository(InMemoryEventStore(5), TestMessageFormat(), InMemoryEventPublisher(), configuration)

        val invoices = eventRepository.getByIds(listOf("::id 1::", "::id 2::"), Invoice::class.java)
        assertThat(invoices, `is`(equalTo(mapOf())))
    }

    @Test
    fun reachingPersistenceLimitForEventsWillCreateSnapshotAndNewEventEntity() {
        var testClass = TestClass("::id::", "::string::", 1, TestObject("::value::"), listOf(TestObject("::value2::")))

        val eventPublisher = InMemoryEventPublisher()
        val eventRepository = SimpleAggregateRepository(InMemoryEventStore(1), TestMessageFormat(), eventPublisher, configuration)
        eventRepository.save(testClass, anyIdentity)

        testClass = eventRepository.getById(testClass.getId()!!, TestClass::class.java)

        testClass.changeLong(123)
        eventRepository.save(testClass, anyIdentity)

        testClass = eventRepository.getById(testClass.getId()!!, TestClass::class.java)

        assertThat(testClass.long, equalTo(123L))
    }

    @Test
    fun creatingManySnapshots() {
        val eventPublisher = InMemoryEventPublisher()
        val eventRepository = SimpleAggregateRepository(InMemoryEventStore(1), TestMessageFormat(), eventPublisher, configuration)

        var testClass = TestClass("::id::", "::string::", 1, TestObject("::value::", Foo("bar")), listOf(TestObject("::value2::", Foo("baar"))))

        eventRepository.save(testClass, anyIdentity)
        testClass = eventRepository.getById(testClass.getId()!!, TestClass::class.java)

        testClass.changeLong(123)
        eventRepository.save(testClass, anyIdentity)
        testClass = eventRepository.getById(testClass.getId()!!, TestClass::class.java)

        testClass.changeString("newString")
        eventRepository.save(testClass, anyIdentity)
        testClass = eventRepository.getById(testClass.getId()!!, TestClass::class.java)

        testClass.changeObject(TestObject("otherValue", Foo("FooBar")))
        eventRepository.save(testClass, anyIdentity)
        testClass = eventRepository.getById(testClass.getId()!!, TestClass::class.java)

        testClass.changeList(listOf(TestObject("otherValueInList", Foo("BarFoo"))))
        eventRepository.save(testClass, anyIdentity)

        testClass = eventRepository.getById(testClass.getId()!!, TestClass::class.java)

        assertThat(testClass.long, equalTo(123L))
        assertThat(testClass.string, equalTo("newString"))
        assertThat(testClass.testObject, equalTo(TestObject("otherValue", Foo("FooBar"))))
        assertThat(testClass.list[0], equalTo(TestObject("otherValueInList", Foo("BarFoo"))))
    }

    @Test
    fun usingDefaultSnapshotMapper() {
        val eventRepository = SimpleAggregateRepository(InMemoryEventStore(1), TestMessageFormat(), InMemoryEventPublisher(), configuration)
        val id = invoiceId()

        var invoice = Invoice(id, "John")
        eventRepository.save(invoice, anyIdentity)

        invoice.changeCustomerName("Smith")
        eventRepository.save(invoice, anyIdentity)
        invoice = eventRepository.getById(id, Invoice::class.java)

        invoice.changeCustomerName("Foo")
        eventRepository.save(invoice, anyIdentity)
        invoice = eventRepository.getById(id, Invoice::class.java)

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

class TestClassSnapshotMapper : SnapshotMapper<TestClass> {
    private val gson = Gson()

    override fun toSnapshot(data: TestClass): Snapshot {
        val mapped = mapOf(
                "id" to data.getId()!!,
                "string" to data.string,
                "long" to data.long,
                "testObject" to mapOf("value" to data.testObject.value, "innerClass" to mapOf("bar" to data.testObject.innerClass.bar)),
                "list" to listOf(data.list.map { "value" to it.value }.toMap())
        )
        return Snapshot(0, Binary(gson.toJson(mapped)))
    }

    override fun fromSnapshot(snapshot: String, snapshotVersion: Long): TestClass {
        val jsonMap = gson.fromJson<Map<String, Any>>(snapshot, object : TypeToken<Map<String, Any>>() {}.type)
        val list = jsonMap["list"] as List<Map<String, Any>>

        return TestClass(
                jsonMap["id"] as String,
                jsonMap["string"] as String,
                (jsonMap["long"] as Double).toLong(),
                adaptTestObject(jsonMap["testObject"] as? Map<String, Any> ?: mapOf()),
                list.map { adaptTestObject(it) })
    }

    private fun adaptTestObject(jsonMap: Map<String, Any>): TestObject {
        val innerClass = jsonMap["innerClass"] as? Map<*, *> ?: mapOf<String, Any>()
        val foo = Foo(innerClass["bar"] as String? ?: "")
        return TestObject(jsonMap["value"]!! as String, foo)
    }
}

data class TestClass private constructor(var string: String, var long: Long, var testObject: TestObject, var list: List<TestObject>) : AggregateRootBase() {
    constructor() : this("", 0, TestObject(), listOf())

    override fun getSnapshotMapper(): SnapshotMapper<AggregateRoot> {
        return TestClassSnapshotMapper() as SnapshotMapper<AggregateRoot>
    }

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