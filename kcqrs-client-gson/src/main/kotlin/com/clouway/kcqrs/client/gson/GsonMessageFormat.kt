package com.clouway.kcqrs.client.gson

import com.clouway.kcqrs.core.messages.MessageFormat
import com.clouway.kcqrs.core.messages.TypeLookup
import java.io.InputStream
import java.io.InputStreamReader

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
internal class GsonMessageFormat(
    vararg types: Class<*>,
) : MessageFormat {
    private val gson = GsonFactory.create()
	
    private val kindToType = mutableMapOf<String, Class<*>>()

    init {
        types.forEach {
            kindToType[it.simpleName] = it
        }
    }

    override fun isSupporting(
        kind: String,
        typeLookup: TypeLookup,
    ): Boolean {
        var type = typeLookup.lookup(kind)
        if (type == null) {
            type = kindToType.get(kind)
        }
        return type != null
    }

    override fun <T> parse(
        stream: InputStream,
        kind: String,
        typeLookup: TypeLookup,
    ): T {
        var type = typeLookup.lookup(kind)
        if (type == null && kindToType.containsKey(kind)) {
            type = kindToType.getValue(kind)
        }
        if (type == null) {
            throw IllegalArgumentException("Type not found for kind: $kind")
        }
        return gson.fromJson(InputStreamReader(stream, Charsets.UTF_8), type) as T
    }

    override fun formatToBytes(value: Any): ByteArray = gson.toJson(value).toByteArray(Charsets.UTF_8)
}
