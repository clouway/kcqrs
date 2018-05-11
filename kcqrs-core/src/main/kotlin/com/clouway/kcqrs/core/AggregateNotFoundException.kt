package com.clouway.kcqrs.core

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class AggregateNotFoundException(aggregateId: String) : AggregateException(aggregateId, "Aggregate was not found")