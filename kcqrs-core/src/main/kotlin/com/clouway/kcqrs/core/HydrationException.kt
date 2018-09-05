package com.clouway.kcqrs.core


/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class HydrationException(val aggregateId: String, message: String?) : RuntimeException(message)