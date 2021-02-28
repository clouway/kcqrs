package com.clouway.kcqrs.core.messages

import java.io.InputStream
import java.lang.reflect.Type

/**
 * DataModelFormat provides the internal format for storing of Events data. This interface provides
 * the EventStore implementations with ability to transform event records.
 *
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
interface DataModelFormat {
	/**
	 * Parses JSON content from the provided input stream.
	 */
	fun <T> parse(stream: InputStream, type: Type): T
	
	/**
	 * Formats the provided value into string value.
	 */
	fun formatToString(value: Any): String
	
}