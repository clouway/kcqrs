package com.clouway.kcqrs.core

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class SyncEventPublisher(private val messageBus: MessageBus) : EventPublisher {
    override fun publish(events: Iterable<EventWithPayload>) {
        events.forEach {
            try {
                messageBus.handle(it)
            } catch (ex: Exception) {
                throw PublishErrorException(ex)
            }
        }
    }
}