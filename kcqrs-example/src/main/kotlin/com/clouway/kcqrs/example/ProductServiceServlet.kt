package com.clouway.kcqrs.example

import com.clouway.kcqrs.core.*
import java.util.*
import javax.servlet.ServletConfig
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse


/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class ProductServiceServlet : HttpServlet() {
    private lateinit var messageBus: MessageBus
    private lateinit var eventRepository: EventRepository

    override fun init(config: ServletConfig?) {
        messageBus = KCqrs.messageBus()

        eventRepository = KCqrs.eventRepository()

        messageBus.registerInterceptor(object : Interceptor {
            override fun intercept(chain: Interceptor.Chain) {

                chain.proceed(chain.event())
            }

        })

        messageBus.registerCommandHandler(RegisterProductCommand::class.java, RegisterProductCommandHandler(eventRepository))
        messageBus.registerEventHandler(ProductRegisteredEvent::class.java, ProductRegisteredEventHandler())
    }

    override fun doGet(req: HttpServletRequest, resp: HttpServletResponse) {
        val id = req.getParameter("id")

        try {
            val product = eventRepository.getById(UUID.fromString(id), Product::class.java)

            val writer = resp.writer
            writer.println("Product: ${product.name}")
            writer.flush()
        } catch (ex: AggregateNotFoundException) {
            resp.status = HttpServletResponse.SC_NOT_FOUND
        }

    }

    override fun doPut(req: HttpServletRequest, resp: HttpServletResponse) {
        val id = req.getParameter("id")
        val newName = req.getParameter("name")

        val product = eventRepository.getById(UUID.fromString(id), Product::class.java)
        product.changeName(newName)
        eventRepository.save(product)

    }

    override fun doPost(req: HttpServletRequest, resp: HttpServletResponse) {
        messageBus.send(RegisterProductCommand(UUID.randomUUID(), "My Product"))

        resp.status = HttpServletResponse.SC_CREATED
    }

    data class RegisterProductCommand(val uuid: UUID, val name: String) : Command

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

    class RegisterProductCommandHandler(private val eventRepository: Repository) : CommandHandler<RegisterProductCommand> {

        override fun handle(command: RegisterProductCommand) {
            try {
                eventRepository.getById(command.uuid, Product::class.java)

            } catch (ex: AggregateNotFoundException) {

                val product = Product(command.uuid, command.name)
                eventRepository.save(product)
            }
        }

    }

}