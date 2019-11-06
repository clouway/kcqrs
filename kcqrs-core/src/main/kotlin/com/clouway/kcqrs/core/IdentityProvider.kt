package com.clouway.kcqrs.core

import java.time.Instant

/**
 * IdentityProvider is an identity provider which is provided to apps as hook
 *
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
interface IdentityProvider {

    /**
     * Get returns the identity associated with the request.
     */
    fun get(): Identity


    class Default : IdentityProvider {
        override fun get(): Identity {
            return Identity("-1", "default", Instant.now())
        }
    }
}

