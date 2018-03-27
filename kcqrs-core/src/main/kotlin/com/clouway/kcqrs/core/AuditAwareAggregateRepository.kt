package com.clouway.kcqrs.core

/**
 * Repository is representing an Repository which operates with the AggregateRoot objects.
 *
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
interface AuditAwareAggregateRepository {

    /**
     * Creates a new or updates an existing aggregate in the repository.
     *
     * @param aggregate the aggregate to be registered
     * @throws EventCollisionException is thrown in case of
     */
    @Throws(PublishErrorException::class, EventCollisionException::class)
    fun <T : AggregateRoot> save(aggregate: T, identity: Identity)

    /**
     * Get the aggregate
     *
     * @param id
     * @return
     * @throws HydrationException
     * @throws AggregateNotFoundException
     */
    @Throws(HydrationException::class, AggregateNotFoundException::class)
    fun <T : AggregateRoot> getById(id: String, type: Class<T>): T

}