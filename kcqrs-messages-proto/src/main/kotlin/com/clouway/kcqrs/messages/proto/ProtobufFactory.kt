package com.clouway.kcqrs.messages.proto

import com.clouway.kcqrs.core.messages.MessageFormat
import com.clouway.kcqrs.core.messages.MessageFormatFactory

/**
 * ProtoMessageFactory is a factory class for that creates [ProtoMessageFormat] instances for formatting
 * of Event messages as Protocol Buffers.
 *
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class ProtoMessageFactory(private vararg val params: EventAdapter<*, *>) : MessageFormatFactory {
	
	override fun createMessageFormat(): MessageFormat {
		return ProtoMessageFormat(*params)
	}
}

