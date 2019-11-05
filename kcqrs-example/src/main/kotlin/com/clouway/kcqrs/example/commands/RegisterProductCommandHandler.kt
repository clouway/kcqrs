package com.clouway.kcqrs.example.commands

import com.clouway.kcqrs.core.AggregateNotFoundException
import com.clouway.kcqrs.core.AggregateRepository
import com.clouway.kcqrs.core.CommandHandler
import com.clouway.kcqrs.example.domain.Product


class RegisterProductCommandHandler(private val eventRepository: AggregateRepository) : CommandHandler<RegisterProductCommand, String> {

    override fun handle(command: RegisterProductCommand): String {
        val id = command.id.toString()
        try {
            eventRepository.getById(id, Product::class.java)

        } catch (ex: AggregateNotFoundException) {

            val product = Product(id, command.name)
            eventRepository.save(product)
        }
        return "OK"
    }

}