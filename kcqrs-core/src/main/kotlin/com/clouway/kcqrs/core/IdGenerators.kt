package com.clouway.kcqrs.core

import java.time.LocalDateTime

/**
 * IdGenerators ia a factory class used to create different kind of generators used for Events.
 *
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
object IdGenerators {
    
    /**
     * Creates a new Distributed Sequence Generator inspired by Twitter snowflake:
     * https://github.com/twitter/snowflake/tree/snowflake-2010
     *
     * **Note:** A single instance of the sequence generator needs to be used.
     *
     * @param nodeId the nodeId which will be used as disciminator. A nodeId will be generated automatically if none is provided.
     * @param epoch the epoch time which will be used
     */
    fun snowflake(nodeId: String? = null, epoch: LocalDateTime = LocalDateTime.of(2015, 1, 1, 0, 0, 0)): IdGenerator {
        // Use passed nodeId as descriminator as it will ensure
        // uniqueness of the generated Ids when multiple app instances
        // are created
        if (nodeId != null) {
            // Make sure that nodeId not exceeds it's size
            return SnowflakeIdGenerator(Math.abs(nodeId.hashCode() % 1023), epoch)
        }
        
        return SnowflakeIdGenerator()
    }
}