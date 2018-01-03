package com.clouway.kcqrs.core

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
interface CommandHandler<in T : Command> {

    /**
     * Handle the command
     *
     * @param command
     * @throws EventCollisionException
     * @throws HydrationException
     * @throws AggregateNotFoundException
     */
    @Throws(EventCollisionException::class, HydrationException::class, AggregateNotFoundException::class)
    fun handle(command: T)
}