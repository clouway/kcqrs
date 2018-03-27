package com.clouway.kcqrs.core

/**
 * AggregateException is an exceptional class which indicates errors occurred during loading of AggregateRoots.
 * s
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
open class AggregateException(val aggregateId: String, message: String) : RuntimeException(message)