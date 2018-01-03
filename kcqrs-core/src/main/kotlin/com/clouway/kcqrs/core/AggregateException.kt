package com.clouway.kcqrs.core

import java.util.UUID

/**
 * AggregateException is an exceptional class which indicates errors occurred during loading of AggregateRoots.
 * s
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
open class AggregateException(val aggregateId: UUID, message: String) : RuntimeException(message)