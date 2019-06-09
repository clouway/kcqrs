package com.clouway.kcqrs.core

/**
 * Position represents the Position of the Event in the EventStore.
 */
data class Position(val value: Long)