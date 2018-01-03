package com.clouway.kcqrs.core

import java.util.*

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
/**
 * Simple interface to an aggregate root
 */
interface AggregateRoot {
  /**
   * get the Id
   *
   * @return
   */
  fun getId(): UUID?

  /**
   * Gets all change events since the
   * original hydration. If there are no
   * changes then null is returned
   *
   * @return
   */
  fun getUncommittedChanges(): Iterable<Event>

  /**
   * Mark all changes a committed
   */
  fun markChangesAsCommitted()

  /**
   * load the aggregate root
   *
   * @param history
   * @throws HydrationException
   */
  fun loadFromHistory(history: Iterable<Event>)

  /**
   * Returns the version of the aggregate when it was hydrated
   * @return
   */
  fun getExpectedVersion(): Int

}