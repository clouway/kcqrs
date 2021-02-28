package com.clouway.kcqrs.example.adapter.http

import com.clouway.kcqrs.client.gson.GsonFactory
import spark.Request
import spark.ResponseTransformer
import java.io.InputStreamReader
import java.lang.reflect.Type

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
private val gson = GsonFactory.create()

object Transports {
 
	class JsonTransformer : ResponseTransformer {
		override fun render(model: Any?): String {
			if (model == null) {
				return ""
			}
			return gson.toJson(model)
		}
	}
	
	private val jsonTransformer = JsonTransformer()
	
	fun json(): ResponseTransformer {
		return jsonTransformer
	}
	
}

fun <T> Request.readJson(type: Type): T {
	return gson.fromJson(InputStreamReader(this.raw().inputStream, Charsets.UTF_8), type)
}