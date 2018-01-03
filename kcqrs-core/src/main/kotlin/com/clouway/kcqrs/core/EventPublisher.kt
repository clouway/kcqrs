package com.clouway.kcqrs.core

/**
 * EventPublisher is an EventPublisher class which is responsble for publishing
 * 
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
interface EventPublisher {

    /**
     * Publish a set of events
     *
     * @param events the events to be published
     */
    @Throws(PublishErrorException::class)
    fun publish(events: Iterable<Event>)
}