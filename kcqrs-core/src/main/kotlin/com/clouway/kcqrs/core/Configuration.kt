package com.clouway.kcqrs.core

/**
 * Configuration is representing the configuration of the KCQRS.
 *
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
interface Configuration {
    /**
     * Determines the topic name of the AggregateRoot.
     */
    fun topicName(aggregate: AggregateRoot): String
}