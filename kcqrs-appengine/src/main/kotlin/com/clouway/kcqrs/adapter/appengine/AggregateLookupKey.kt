package com.clouway.kcqrs.adapter.appengine

/**
 * AggregateLookupKey is a key used for lookup of aggregates.
 * 
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
internal data class AggregateLookupKey(val tenant: String, val stream: String, val aggregateIndex: Long)