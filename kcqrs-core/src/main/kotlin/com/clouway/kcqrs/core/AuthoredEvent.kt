package com.clouway.kcqrs.core

/**
 * AuthoredEvent will be used when you want to have the Identity of
 * the event provided, e.g. you want the id of the author of the event
 * and the time of the event.
 *
 * @author Vasil Mitov <vasil.mitov@clouway.com>
 */

abstract class AuthoredEvent private constructor(var identity: Identity?) : Event {
    constructor() : this(null)
}