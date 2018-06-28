package com.clouway.kcqrs.core

import com.google.gson.Gson
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.util.ArrayList


/**
 * AggregateRootBase is a Base class of all aggregate roots.
 *
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
abstract class AggregateRootBase private constructor(@JvmField protected var aggregateId: String? = "") : AggregateRoot {

    private var changes: ArrayList<Event> = ArrayList()
    private var version = 0L

    constructor() : this(null)

    override fun getId(): String? {
        return aggregateId
    }

    override fun <T : AggregateRoot> fromSnapshot(snapshotData: String, snapshotVersion: Long): T {
        val fromSnapshot = getSnapshotMapper().fromSnapshot(snapshotData, snapshotVersion)
        val newInstance = this@AggregateRootBase::class.java.newInstance()
        setFields(fromSnapshot, newInstance)
        return newInstance as T
    }

    override fun getSnapshotMapper(): SnapshotMapper<AggregateRoot> {
        //TODO(V.Mitov) Pass a serializer so that type adapters could be used
        return object : SnapshotMapper<AggregateRoot> {
            val gson = Gson()
            override fun toSnapshot(data: AggregateRoot): Snapshot {
                return Snapshot(0, Binary(gson.toJson(data)))
            }

            override fun fromSnapshot(snapshot: String, snapshotVersion: Long): AggregateRoot {
                return gson.fromJson(snapshot, this@AggregateRootBase::class.java)
            }
        }
    }

    override fun markChangesAsCommitted() {
        changes.clear()
    }

    override fun getExpectedVersion(): Long {
        return version
    }

    override fun getUncommittedChanges(): List<Event> {
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

    private fun setFields(from: Any, to: Any) {
        from.javaClass.declaredFields.forEach { field ->
            try {
                val fieldFrom = from.javaClass.getDeclaredField(field.name)
                fieldFrom.isAccessible = true
                val value = fieldFrom.get(from)
                val declaredField = to.javaClass.getDeclaredField(field.name)
                declaredField.isAccessible = true
                declaredField.set(to, value)
            } catch (e: IllegalAccessException) {
                throw IllegalStateException(e)
            } catch (e: NoSuchFieldException) {
                throw IllegalStateException(e)
            }
        }

        //Grabbing the aggregateId from the superClass
        val aggregateIdFieldFrom = from.javaClass.superclass.getDeclaredField("aggregateId")
        aggregateIdFieldFrom.isAccessible = true
        val value = aggregateIdFieldFrom.get(from)
        val declaredField = to.javaClass.superclass.getDeclaredField("aggregateId")
        declaredField.isAccessible = true
        declaredField.set(to, value)
    }
}