package com.clouway.kcqrs.example

import com.clouway.kcqrs.core.Interceptor
import com.clouway.kcqrs.example.adapter.http.ChangeProductNameHandler
import com.clouway.kcqrs.example.adapter.http.GetProductHandler
import com.clouway.kcqrs.example.adapter.http.RegisterProductHandler
import com.clouway.kcqrs.example.adapter.http.Transports.json
import com.clouway.kcqrs.example.commands.RegisterProductCommand
import com.clouway.kcqrs.example.commands.RegisterProductCommandHandler
import com.clouway.kcqrs.example.domain.ProductRegisteredEvent
import com.clouway.kcqrs.example.handler.ProductRegisteredEventHandler
import spark.Spark.*
import spark.servlet.SparkApplication

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
@Suppress("UNUSED")
class AppBootstrap : SparkApplication {
    
    override fun init() {
        val messageBus = CQRSContext.messageBus()

        val eventRepository = CQRSContext.eventRepository()

        messageBus.registerInterceptor(object : Interceptor {
            override fun intercept(chain: Interceptor.Chain) {
                chain.proceed(chain.event())
            }
        })
        
        messageBus.registerCommandHandler(RegisterProductCommand::class.java, RegisterProductCommandHandler(eventRepository))
        messageBus.registerEventHandler(ProductRegisteredEvent::class.java, ProductRegisteredEventHandler())

        post("/v1/products", RegisterProductHandler(messageBus), json())
        get("/v1/products/:id", GetProductHandler(eventRepository), json())
        put("/v1/products/:id", ChangeProductNameHandler(eventRepository), json())

        after("/v1/*", { _, response ->
            response.header("Content-Type", "application/json")
        })
    }

}