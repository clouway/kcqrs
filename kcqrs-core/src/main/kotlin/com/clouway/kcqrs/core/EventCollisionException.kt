package com.clouway.kcqrs.core

import java.util.*

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class EventCollisionException(aggregateId: UUID, val version: Int,  message: String) : AggregateException(aggregateId, message) {
    constructor(aggregateId: UUID, version: Int) : this(aggregateId, version, "Data has been changed between loading and state changes.")
}
