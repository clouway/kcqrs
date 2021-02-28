package com.clouway.kcqrs.example

import com.clouway.kcqrs.core.Interceptor
import com.clouway.kcqrs.example.adapter.http.ChangeProductNameHandler
import com.clouway.kcqrs.example.adapter.http.GetProductHandler
import com.clouway.kcqrs.example.adapter.http.RegisterProductHandler
import com.clouway.kcqrs.example.adapter.http.Transports.json
import com.clouway.kcqrs.example.commands.RegisterProductCommand
import com.clouway.kcqrs.example.commands.RegisterProductCommandHandler
import spark.Spark

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
fun main() {
	val messageBus = CQRSContext.messageBus()
	
	val eventRepository = CQRSContext.eventRepository()
	
	messageBus.registerInterceptor(object : Interceptor {
		override fun intercept(chain: Interceptor.Chain) {
			chain.proceed(chain.event())
		}
	})
	
	messageBus.registerCommandHandler(
		RegisterProductCommand::class.java,
		RegisterProductCommandHandler(eventRepository)
	)
	
	Spark.port(8080)
	
	Spark.post("/v1/products", RegisterProductHandler(messageBus), json())
	Spark.get("/v1/products/:id", GetProductHandler(eventRepository), json())
	Spark.put("/v1/products/:id", ChangeProductNameHandler(eventRepository), json())
	
	Spark.after("/v1/*") { _, response ->
		response.header("Content-Type", "application/json")
	}
	
}

//curl --header "Content-Type: application/json" --request POST --data '{"name":"Product1"}' http://localhost:8080/v1/products