package com.clouway.kcqrs.testing

import com.clouway.kcqrs.core.*

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class InMemoryMessageBus : MessageBus {
    
    override fun handle(event: EventWithPayload) {

    }

    override fun <T : Command> registerCommandHandler(aClass: Class<T>, handler: CommandHandler<T>) {

    }

    override fun <T : Event> registerEventHandler(aClass: Class<T>, handler: EventHandler<T>) {

    }

    override fun <T : Command> send(command: T) {

    }

    override fun registerInterceptor(interceptor: Interceptor) {

    }

}