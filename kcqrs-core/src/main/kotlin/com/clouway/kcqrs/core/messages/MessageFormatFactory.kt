package com.clouway.kcqrs.core.messages

/**
 * JsonFactory is an factory class which
 * 
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
interface MessageFormatFactory {
    /**
     * Creates a new MessageFormat. 
     */
    fun createMessageFormat(): MessageFormat

}