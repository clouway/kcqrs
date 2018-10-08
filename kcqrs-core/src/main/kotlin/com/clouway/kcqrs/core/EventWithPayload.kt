package com.clouway.kcqrs.core

/**
 * EventWithPayload is an abstraction of Event which wraps it's payload into the same object. The payload
 * part is used as caching of the serialization as different layers are working either with event or
 * with it's payload.
 */
data class EventWithPayload(val event: Any, val payload: Binary)