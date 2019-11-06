package com.clouway.kcqrs.core

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class AuthoredAggregateRepository(private val identityProvider: IdentityProvider,
                                  private val auditAwareAggregateRepository: AuditAwareAggregateRepository
) : AggregateRepository {
    
    override fun <T : AggregateRoot> save(aggregate: T) {
        val identity = identityProvider.get()
        auditAwareAggregateRepository.save(aggregate, identity)
    }
    
    override fun <T : AggregateRoot> save(stream: String, aggregate: T) {
        val identity = identityProvider.get()
        auditAwareAggregateRepository.save(stream, aggregate, identity)
    }
    
    override fun <T : AggregateRoot> getById(id: String, type: Class<T>): T {
        val identity = identityProvider.get()
        return auditAwareAggregateRepository.getById(id, type, identity)
    }
    
    override fun <T : AggregateRoot> getById(stream: String, aggregateId: String, type: Class<T>): T {
        val identity = identityProvider.get()
        return auditAwareAggregateRepository.getById(stream, aggregateId, type, identity)
    }
    
    override fun <T : AggregateRoot> getByIds(ids: List<String>, type: Class<T>): Map<String, T> {
        val identity = identityProvider.get()
        return auditAwareAggregateRepository.getByIds(ids, type, identity)
    }
}