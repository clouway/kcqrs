package com.clouway.kcqrs.testing

import com.clouway.kcqrs.core.Event
import com.clouway.kcqrs.core.EventPublisher
import com.clouway.kcqrs.core.PublishErrorException

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class InMemoryEventPublisher : EventPublisher {
    var events = mutableListOf<Event>()
    var nextPublishFailsWithError = false

    override fun publish(events: Iterable<Event>) {
        if (nextPublishFailsWithError) {
            nextPublishFailsWithError = false
            throw PublishErrorException()
        }

        this.events.addAll(events)
    }

    fun cleanUp() {
        events = mutableListOf<Event>()
    }

    fun pretendThatNextPublishWillFail() {
        nextPublishFailsWithError = true
    }

}