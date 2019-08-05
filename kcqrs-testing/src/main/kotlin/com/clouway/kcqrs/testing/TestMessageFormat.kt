package com.clouway.kcqrs.testing

import com.clouway.kcqrs.core.messages.MessageFormat
import com.google.gson.Gson
import java.io.InputStream
import java.io.InputStreamReader
import java.io.OutputStream
import java.io.OutputStreamWriter
import java.lang.reflect.Type

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */

class TestMessageFormat : MessageFormat {

    override fun <T> parse(json: String, type: Class<T>): T {
        return gson.fromJson<T>(json, type)
    }

    private val gson = Gson()
    override fun <T> parse(stream: InputStream, type: Type): T {
        return gson.fromJson(InputStreamReader(stream, Charsets.UTF_8), type)
    }

    override fun formatToString(value: Any): String {
        return gson.toJson(value)
    }

    override fun formatToBytes(value: Any): ByteArray {
        return gson.toJson(value).toByteArray(Charsets.UTF_8)
    }

    override fun writeTo(value: Any, stream: OutputStream) {
        val writer = OutputStreamWriter(stream)
        gson.toJson(value, writer)
        writer.close()
    }
}