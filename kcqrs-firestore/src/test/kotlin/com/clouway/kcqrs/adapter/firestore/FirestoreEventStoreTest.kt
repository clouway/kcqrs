package com.clouway.kcqrs.adapter.firestore

import com.clouway.kcqrs.core.EventStore
import com.clouway.kcqrs.core.IdGenerators
import com.clouway.kcqrs.testing.EventStoreContract
import com.clouway.kcqrs.testing.TestMessageFormat
import com.clouway.testing.firestore.FirestoreCleaner
import com.clouway.testing.firestore.FirestoreRule
import org.junit.ClassRule
import org.junit.Rule

/**
 * FirestoreEventStoreTest is a testing class used to verify the behaviour of events stored in Firestore.
 *
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class FirestoreEventStoreTest : EventStoreContract() {

    companion object {
        @ClassRule
        @JvmField
        val firestoreRule = FirestoreRule()
    }

    @Rule
    @JvmField
    val firestoreCleaner = FirestoreCleaner(firestoreRule.firestore, listOf("streams", "snapshots", "stream_indexes"))

    override fun createEventStore(): EventStore {
        return FirestoreEventStore(firestoreRule.firestore, TestMessageFormat(), IdGenerators.snowflake())
    }
}