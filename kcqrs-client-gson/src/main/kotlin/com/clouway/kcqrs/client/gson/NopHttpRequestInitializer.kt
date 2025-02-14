package com.clouway.kcqrs.client.gson

import com.google.api.client.http.HttpRequest
import com.google.api.client.http.HttpRequestInitializer

/**
 * NopHttpRequestInitializer is an initializer which is doing nothing and is used when
 * client code doesn't have concrete one set.
 *
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
internal class NopHttpRequestInitializer : HttpRequestInitializer {
    override fun initialize(request: HttpRequest) {
        // nop
    }
}
