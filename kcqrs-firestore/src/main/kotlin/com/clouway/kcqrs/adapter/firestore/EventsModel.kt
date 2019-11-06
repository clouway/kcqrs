package com.clouway.kcqrs.adapter.firestore

/**
 * EventsModel is a model class that represents the events structure in the firestore document.
 *
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
internal data class EventsModel(@JvmField val events: List<EventModel>) {

    /**
     * Removes last N events from list.
     */
    fun removeLastN(count: Int): EventsModel {
        val lastEventIndex = events.size - count
        val updatedEvents = events.filterIndexed { index, _ -> index < lastEventIndex }
        return EventsModel(updatedEvents)
    }
}