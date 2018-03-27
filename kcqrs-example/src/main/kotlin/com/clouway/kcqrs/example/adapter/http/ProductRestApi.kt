package com.clouway.kcqrs.example.adapter.http

import com.clouway.kcqrs.core.AggregateNotFoundException
import com.clouway.kcqrs.core.MessageBus
import com.clouway.kcqrs.core.Repository
import com.clouway.kcqrs.example.commands.RegisterProductCommand
import com.clouway.kcqrs.example.domain.Product
import spark.Request
import spark.Response
import spark.Route
import java.util.UUID
import javax.servlet.http.HttpServletResponse

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class RegisterProductHandler(private val messageBus: MessageBus) : Route {
    override fun handle(request: Request, response: Response): Any {
        val uuid = UUID.randomUUID()

        val req = request.readJson<RegisterProductRequestDto>(RegisterProductRequestDto::class.java)
        messageBus.send(RegisterProductCommand(uuid, req.name))

        return ProductAddedResponseDto(uuid.toString(), req.name)
    }

}


class GetProductHandler(private val eventRepository: Repository) : Route {
    override fun handle(request: Request, response: Response): Any? {
        val id = request.params(":id")

        return try {
            val product = eventRepository.getById(UUID.fromString(id), Product::class.java)
            ProductDto(product.name)
        } catch (ex: AggregateNotFoundException) {
            response.status(HttpServletResponse.SC_NOT_FOUND)
            null
        }
    }

}

class ChangeProductNameHandler(private val eventRepository: Repository) : Route {
    override fun handle(request: Request, response: Response): Any? {
        val id = request.params(":id")

        val req = request.readJson<ChangeProductNameRequest>(ChangeProductNameRequest::class.java)

        val product = eventRepository.getById(UUID.fromString(id), Product::class.java)
        product.changeName(req.name)

        eventRepository.save(product)

        return ProductUpdatedResponseDto(id, req.name)
    }
}

data class RegisterProductRequestDto(val name: String)
data class ProductAddedResponseDto(val id: String, val name: String)

data class ChangeProductNameRequest(val name: String)
data class ProductUpdatedResponseDto(val id: String, val name: String)

data class ProductDto(val name: String)