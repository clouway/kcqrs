package com.clouway.kcqrs.core

import java.time.Instant

/**
 * Identity is representing a single identity which represents the initiator of the request.
 *
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
data class Identity(val id: String, val time: Instant)