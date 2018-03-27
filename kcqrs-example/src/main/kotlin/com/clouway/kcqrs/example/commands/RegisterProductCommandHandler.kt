package com.clouway.kcqrs.example.commands

import com.clouway.kcqrs.core.AggregateNotFoundException
import com.clouway.kcqrs.core.CommandHandler
import com.clouway.kcqrs.core.Repository
import com.clouway.kcqrs.example.domain.Product


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