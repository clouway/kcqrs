package com.clouway.kcqrs.adapter.appengine

import com.clouway.kcqrs.core.EventWithPayload
import com.clouway.kcqrs.core.MessageBus
import java.io.ByteArrayInputStream
import java.io.InputStream
import java.util.logging.Level
import java.util.logging.Logger
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
abstract class AbstractEventHandlerServlet : HttpServlet() {
    private val logger = Logger.getLogger(AbstractEventHandlerServlet::class.java.name)

    override fun doPost(req: HttpServletRequest, resp: HttpServletResponse) {
        val type = req.getParameter("type")
        val payload = req.getParameter("payload")
        
        val clazz = Class.forName(type)
        try {
            val event = decode(ByteArrayInputStream(payload.toByteArray(Charsets.UTF_8)), clazz)
            messageBus().handle(EventWithPayload(event, payload))
        } catch (ex: Exception) {
            logger.log(Level.SEVERE, "Could not handle the received event", ex)
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST)
        }
    }

    abstract fun decode(inputStream: InputStream, type: Class<*>): Any

    abstract fun messageBus(): MessageBus

}