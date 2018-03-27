package com.clouway.kcqrs.core.messages

import java.io.InputStream
import java.lang.reflect.Type

/**
 * JsonFormat is an abstract JSON message format used for parsing and serializing of input messages.
 *
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
interface MessageFormat {

    /**
     * Parses JSON content from the provided input stream.
     */
    fun <T> parse(stream: InputStream, type: Type): T

    /**
     * Formats the provided value into a JSON object
     */
    fun format(value: Any): String
}