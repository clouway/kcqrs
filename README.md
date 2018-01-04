## kcqrs

CQRS library build in Kotlin for better development experience.

#### Handling Commands 

```kotlin
val messageBus = cqrs.messageBus()

// On App Initialization 
messageBus.registerCommandHandler(ChangeCustomerName::class.java, ChangeCustomerNameHandler())

// Dispatch User Action in Servlet or REST Api Call 
fun registerNewCustomer(req: HttpServletRequest, HttpServletResponse) {     
  msgBus.send(RegisterNewCustomer(req.getParameter("name")))      
} 

```

#### Handling Events 

```kotlin

val messageBus = cqrs.messageBus()

// On App Initialization 
msgBus.registerEventHandler(CustomerRegisteredEvent::class.java, CustomerRegisteredEventHandler(InMemoryCustomerRepository()))

// Handle registration event      
msgBus.handle(CustomerRegisteredEvent("Any Customer Name"))       

```
#### Interceptors
Interceptors are a powerful mechanism that can monitor, rewrite, and retry calls. Here's a simple interceptor that
publishes received event. 

```kotlin

val msgBus = cqrs.messageBus()
val pubSubPublisher = object : Interceptor {
   override fun intercept(chain: Interceptor.Chain) {     
     // make sure that event is processed by other handlers                                 
     chain.proceed(chain.event())
     
     val pubSubEvent = adapt(chain.event())     
     pubSubPublisher.publish(pubSubEvent)
  }
}
msgBus.registerInterceptor(pubSubPublisher)

// ....
msgBus.handle(event)                

```

#### App Engine Adapter

Registering Event Handler
```kotlin
class KCqrsEventHandler : AbstractEventHandlerServlet() {
    private val gson = Gson()

    override fun decode(inputStream: InputStream, type: Class<*>): Event {
        val event = gson.fromJson(InputStreamReader(inputStream, "UTF-8"), type)
        return event as Event
    }

    override fun messageBus(): MessageBus {
        return Kcqrs.messageBus()
    }
    
}
```

Adding it to web.xml
```xml
  <servlet>
    <servlet-name>kcqrsEventHandler</servlet-name>
    <servlet-class>com.clouway.kcqrs.example.KCqrsEventHandler</servlet-class>
  </servlet>

  <servlet-mapping>
    <servlet-name>kcqrsEventHandler</servlet-name>
    <url-pattern>/worker/kcqrs</url-pattern>
  </servlet-mapping>

  <security-constraint>
    <web-resource-collection>
      <web-resource-name>worker</web-resource-name>
      <url-pattern>/worker/*</url-pattern>
    </web-resource-collection>
    <auth-constraint>
      <role-name>admin</role-name>
    </auth-constraint>
  </security-constraint>
```

```kotlin
// Initialize KCQRS for GAE by using name of Entity to which events are going to be stored and event handling url
val cqrs = AppEngineKCqrs("Events", "/worker/kcqrs")

// Accessing message
cqrs.messageBus()
// Accessing repository 
cqrs.repository()
```

#### TODO
 * MongoDB Adapter 
 
### License
Copyright 2018 clouWay ood.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   https://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.