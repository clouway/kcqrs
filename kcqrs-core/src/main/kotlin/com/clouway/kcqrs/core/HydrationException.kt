package com.clouway.kcqrs.core

import java.util.*

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class HydrationException(val aggregateId: UUID, message: String?) : RuntimeException(message)