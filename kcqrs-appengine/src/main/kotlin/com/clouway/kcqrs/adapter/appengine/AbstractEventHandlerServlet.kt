package com.clouway.kcqrs.adapter.appengine

import com.clouway.kcqrs.core.Event
import com.clouway.kcqrs.core.MessageBus
import java.io.ByteArrayInputStream
import java.io.InputStream
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
abstract class AbstractEventHandlerServlet : HttpServlet() {

    override fun doPost(req: HttpServletRequest, resp: HttpServletResponse) {
        val type = req.getParameter("type")
        val payload = req.getParameter("payload")

        val clazz = Class.forName(type)
        val event = decode(ByteArrayInputStream(payload.toByteArray(Charsets.UTF_8)), clazz)

        messageBus().handle(event)
    }

    abstract fun decode(inputStream: InputStream, type: Class<*>): Event

    abstract fun messageBus(): MessageBus
}