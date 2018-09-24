package com.clouway.kcqrs.core

/**
 * Base class for handling a specific event that will be published through MessageBus.
 * Ensure that all classes that extend this class implement a constructor
 * that receives a single parameter of the event's type
 *
 * Code Example of required constructor:
 * ```
 * class EmployeeDeactivatedEventHandler(event:  EmployeeDeactivated) : EventHandler<EmployeeDeactivated>(event)
 * ```
 *
 * @param <T>
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
interface EventHandler<in T> {
    fun handle(event: T)
}
