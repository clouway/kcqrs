package com.clouway.kcqrs.example.handler

import com.clouway.kcqrs.core.EventHandler
import com.clouway.kcqrs.example.domain.ProductRegisteredEvent
import com.google.appengine.api.datastore.DatastoreServiceFactory
import com.google.appengine.api.datastore.Entity
import com.google.appengine.api.datastore.EntityNotFoundException
import com.google.appengine.api.datastore.KeyFactory

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class ProductRegisteredEventHandler : EventHandler<ProductRegisteredEvent> {
    override fun handle(event: ProductRegisteredEvent) {
        println("got new ProductRegisteredEvent: $event")
        val dss = DatastoreServiceFactory.getDatastoreService()

        val key = KeyFactory.createKey("Product", event.uuid.toString())
        try {
            dss.get(key)
            println("nothing to be done as product already exists")
            // do nothing as this product was already added
        } catch (ex: EntityNotFoundException) {
            println("registering a new product")
            val e = Entity(key)
            e.setIndexedProperty("name", event.name)

            dss.put(e)
        }
    }

}