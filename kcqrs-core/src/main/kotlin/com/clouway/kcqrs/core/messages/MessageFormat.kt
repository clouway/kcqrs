package com.clouway.kcqrs.core.messages

import java.io.InputStream

/**
 * JsonFormat is an abstract JSON message format used for parsing and serializing of input messages.
 *
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
interface MessageFormat {
	
	/**
	 * Ensures that the provided kind could be supported.
	 */
	fun isSupporting(kind: String, typeLookup: TypeLookup): Boolean
	
	/**
	 * Parses JSON content from the provided input stream.
	 */
	fun <T> parse(stream: InputStream, kind: String, typeLookup: TypeLookup): T
	
	/**
	 * Formats the provided value into binary value.
	 */
	fun formatToBytes(value: Any): ByteArray
	
}