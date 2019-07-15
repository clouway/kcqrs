package com.clouway.kcqrs.core.messages

import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
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
     * Parses JSON content from the provided input stream.
     */
    fun <T> parse(json: String, type: Class<T>): T

    /**
     * Formats the provided value into string value.
     */
    fun formatToString(value: Any): String

    /**
     * Formats the provided value into binary value.
     */
    fun formatToBytes(value: Any): ByteArray

    /**
     * Writes the content of passed value to the provided output stream.
     */
    @Throws(IOException::class)
    fun writeTo(value: Any, stream: OutputStream)
}
