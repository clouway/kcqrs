package com.clouway.kcqrs.client.gson

import com.clouway.kcqrs.core.messages.MessageFormat
import com.clouway.kcqrs.core.messages.MessageFormatFactory

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class GsonMessageFormatFactory : MessageFormatFactory {

    override fun createMessageFormat(): MessageFormat {
        return GsonMessageFormat()
    }

}