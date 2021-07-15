package com.clouway.kcqrs.core.messages

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
interface TypeLookup {
	fun lookup(kind: String): Class<*>?
}