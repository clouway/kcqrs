package com.clouway.kcqrs.core

import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.util.*

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
abstract class AggregateRootBase private constructor(protected @JvmField var uuid: UUID?) : AggregateRoot {

  private var changes: ArrayList<Event> = ArrayList<Event>()
  private var version: Int = 0

  constructor() : this(null)

  override fun getId(): UUID? {
    return uuid
  }

  override fun markChangesAsCommitted() {
    changes.clear()
  }

  override fun getExpectedVersion(): Int {
    return version
  }

  override fun getUncommittedChanges(): Iterable<Event> {
    return if (changes.isEmpty()) listOf() else changes
  }

  override fun loadFromHistory(history: Iterable<Event>) {
    for (event in history) {
      applyChange(event, false)
      version++
    }
  }

  /**
   * Apply the event assuming it is new
   *
   * @param event
   * @throws HydrationException
   */
  protected fun applyChange(event: Event) {
    applyChange(event, true)
  }

  /**
   * Apply the change by invoking the inherited members apply method that fits the signature of the event passed
   *
   * @param event
   * @param isNew
   * @throws HydrationException
   */
  private fun applyChange(event: Event, isNew: Boolean) {

    var method: Method? = null

    try {
      method = this::class.java.getDeclaredMethod("apply", event::class.java)
    } catch (e: NoSuchMethodException) {
      //do nothing. This just means that the method signature wasn't found and
      //the aggregate doesn't need to apply any state changes since it wasn't
      //implemented.
    }

    if (method != null) {
      method.isAccessible = true
      try {
        method.invoke(this, event)
      } catch (e: IllegalAccessException) {
        throw IllegalStateException(e)
      } catch (e: IllegalArgumentException) {
        throw IllegalStateException(e)
      } catch (e: InvocationTargetException) {
        throw IllegalStateException(e)
      }
    }

    if (isNew) {
      changes.add(event)
    }
  }
}