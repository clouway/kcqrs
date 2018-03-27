package com.clouway.kcqrs.testing

import com.clouway.kcqrs.core.EventPublisher
import com.clouway.kcqrs.core.EventWithPayload
import com.clouway.kcqrs.core.PublishErrorException

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class InMemoryEventPublisher : EventPublisher {
    var events = mutableListOf<EventWithPayload>()
    var nextPublishFailsWithError = false

    override fun publish(events: Iterable<EventWithPayload>) {
        if (nextPublishFailsWithError) {
            nextPublishFailsWithError = false
            throw PublishErrorException()
        }
        this.events.addAll(events)
    }

    fun cleanUp() {
        events = mutableListOf()
    }

    fun pretendThatNextPublishWillFail() {
        nextPublishFailsWithError = true
    }

}