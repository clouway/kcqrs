package com.clouway.kcqrs.adapter.appengine

/**
 * IndexedEventKey is representing the index key structure.
 *
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
internal data class IndexedEventKey(
		val tenant: String,
		val stream: String,
		val sequenceId: Long,
		val aggregateId: String,
		val aggregateType: String,
		val aggregateIndex: Long,
		val version: Long
)