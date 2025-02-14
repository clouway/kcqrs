package com.clouway.kcqrs.core

import com.clouway.kcqrs.core.messages.MessageFormat
import com.clouway.kcqrs.core.messages.TypeLookup
import java.io.ByteArrayInputStream
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method

/**
 * AggregateRootBase is a Base class of all aggregate roots.
 *
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
abstract class AggregateRootBase private constructor(
    @JvmField protected var aggregateId: String? = "",
    @JvmField protected var version: Long = 0L,
) : AggregateRoot {
    private var changes: ArrayList<Any> = ArrayList()

    constructor() : this(null)

    override fun getId(): String? = aggregateId

    @Suppress("UNCHECKED_CAST")
    final override fun <T : AggregateRoot> fromSnapshot(
        snapshotData: ByteArray,
        snapshotVersion: Long,
        messageFormat: MessageFormat,
    ): T {
        val snapshotRootBase = getSnapshotMapper().fromSnapshot(snapshotData, snapshotVersion, messageFormat)
        val newInstance = this@AggregateRootBase::class.java.newInstance()
        setFields(snapshotRootBase, newInstance)
        newInstance.version = snapshotVersion
        return newInstance as T
    }

    override fun getSnapshotMapper(): SnapshotMapper<AggregateRoot> =
        object : SnapshotMapper<AggregateRoot> {
            override fun toSnapshot(
                data: AggregateRoot,
                messageFormat: MessageFormat,
            ): Snapshot = Snapshot(data.getExpectedVersion(), Binary(messageFormat.formatToBytes(data)))

            override fun fromSnapshot(
                snapshot: ByteArray,
                snapshotVersion: Long,
                messageFormat: MessageFormat,
            ): AggregateRoot =
                messageFormat.parse(
                    ByteArrayInputStream(snapshot),
                    this@AggregateRootBase::class.java.simpleName,
                    object : TypeLookup {
                        override fun lookup(kind: String): Class<*>? = getSnapshotDataType()
                    },
                )
        }

    override fun getSnapshotDataType(): Class<*>? = null

    override fun markChangesAsCommitted() {
        changes.clear()
    }

    override fun getExpectedVersion(): Long = version

    override fun getUncommittedChanges(): List<Any> = if (changes.isEmpty()) listOf() else changes

    override fun loadFromHistory(
        history: Iterable<Any>,
        version: Long,
    ) {
        this.version = version
        for (event in history) {
            applyChange(event, false)
        }
    }

    /**
     * Apply the event assuming it is new
     *
     * @param event
     * @throws HydrationException
     */
    protected fun applyChange(event: Any) {
        applyChange(event, true)
    }

    /**
     * Apply the change by invoking the inherited members apply method that fits the signature of the event passed
     *
     * @param event
     * @param isNew
     * @throws HydrationException
     */
    private fun applyChange(
        event: Any,
        isNew: Boolean,
    ) {
        var method: Method? = null

        try {
            method = this::class.java.getDeclaredMethod("apply", event::class.java)
        } catch (e: NoSuchMethodException) {
            // do nothing. This just means that the method signature wasn't found and
            // the aggregate doesn't need to apply any state changes since it wasn't
            // implemented.
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

    private fun setFields(
        from: Any,
        to: Any,
    ) {
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

        // Grabbing the aggregateId from the superClass
        val aggregateIdFieldFrom = from.javaClass.superclass.getDeclaredField("aggregateId")
        aggregateIdFieldFrom.isAccessible = true
        val value = aggregateIdFieldFrom.get(from)
        val declaredField = to.javaClass.superclass.getDeclaredField("aggregateId")
        declaredField.isAccessible = true
        declaredField.set(to, value)
    }
}
