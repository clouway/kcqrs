package com.clouway.kcqrs.example

import com.google.appengine.api.NamespaceManager
import javax.servlet.*

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class NamespaceEnforceFilter : Filter {
    private val exampleNamespace = "example"

    override fun doFilter(request: ServletRequest, response: ServletResponse, chain: FilterChain) {
        val oldNamespace = NamespaceManager.get()
        try {
            NamespaceManager.set(exampleNamespace)

            chain.doFilter(request, response)

        } finally {
            if (!oldNamespace.isNullOrEmpty()) {
                NamespaceManager.set(oldNamespace)
            }
        }
    }

    override fun destroy() {

    }

    override fun init(filterConfig: FilterConfig?) {

    }


}