package com.clouway.kcqrs.core

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class EventCollisionException(aggregateId: String, val version: Long, message: String) : AggregateException(aggregateId, message) {
    constructor(aggregateId: String, version: Long) : this(aggregateId, version, "Data has been changed between loading and state changes.")
}
