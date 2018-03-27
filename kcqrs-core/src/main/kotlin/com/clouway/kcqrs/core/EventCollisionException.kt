package com.clouway.kcqrs.core

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class EventCollisionException(aggregateId: String, val version: Int, message: String) : AggregateException(aggregateId, message) {
    constructor(aggregateId: String, version: Int) : this(aggregateId, version, "Data has been changed between loading and state changes.")
}
