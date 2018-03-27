package com.clouway.kcqrs.example.adapter.http

import com.clouway.kcqrs.client.gson.GsonMessageFormatFactory
import spark.Request
import spark.ResponseTransformer
import java.lang.reflect.Type

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
private val messageFormat = GsonMessageFormatFactory().createMessageFormat()

object Transports {

    class JsonTransformer : ResponseTransformer {
        override fun render(model: Any?): String {
            if (model == null) {
                return ""
            }
            return messageFormat.format(model)
        }
    }


    private val jsonTransformer = JsonTransformer()

    fun json(): ResponseTransformer {
        return jsonTransformer
    }

}

fun <T> Request.readJson(type: Type): T {
    return messageFormat.parse(this.raw().inputStream, type)
}