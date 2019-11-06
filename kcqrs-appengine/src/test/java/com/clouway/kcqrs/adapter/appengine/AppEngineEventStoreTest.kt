package com.clouway.kcqrs.adapter.appengine

import com.clouway.kcqrs.core.EventStore
import com.clouway.kcqrs.core.IdGenerators
import com.clouway.kcqrs.testing.EventStoreContract
import com.clouway.kcqrs.testing.TestMessageFormat
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig
import com.google.appengine.tools.development.testing.LocalServiceTestHelper
import org.junit.After
import org.junit.Before


/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class AppEngineEventStoreTest : EventStoreContract() {

    private val helper = LocalServiceTestHelper(LocalDatastoreServiceTestConfig()
            .setDefaultHighRepJobPolicyUnappliedJobPercentage(0f))

    @Before
    fun tearUp() {
        helper.setUp()
    }

    @After
    fun tearDown() {
        helper.tearDown()
    }
    
    override fun createEventStore(): EventStore {
        return AppEngineEventStore("Event", TestMessageFormat(), IdGenerators.snowflake())
    }
}

