package com.clouway.kcqrs.adapter.appengine

import com.clouway.kcqrs.core.messages.MessageFormat
import com.google.gson.Gson
import java.io.InputStream
import java.io.InputStreamReader
import java.lang.reflect.Type

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class TestingJsonFormat : MessageFormat {
    private val gson = Gson()

    override fun <T> parse(stream: InputStream, type: Type): T {
        return gson.fromJson(InputStreamReader(stream, Charsets.UTF_8), type)
    }

    override fun format(value: Any): String {
        return gson.toJson(value)
    }

}