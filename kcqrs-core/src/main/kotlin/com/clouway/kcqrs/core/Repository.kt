package com.clouway.kcqrs.core

import java.util.UUID

/**
 * Repository is representing an Repository which operates with the AggregateRoot objects.
 *
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
interface Repository {
    
    /**
     * Creates a new or updates an existing aggregate in the repository.
     *
     * @param aggregate the aggregate to be registered
     * @throws EventCollisionException is thrown in case of
     */
    @Throws(PublishErrorException::class, EventCollisionException::class)
    fun <T : AggregateRoot> save(aggregate: T)

    /**
     * Get the aggregate
     *
     * @param id
     * @return
     * @throws HydrationException
     * @throws AggregateNotFoundException
     */
    @Throws(HydrationException::class, AggregateNotFoundException::class)
    fun <T : AggregateRoot> getById(id: UUID, type: Class<T>): T

}