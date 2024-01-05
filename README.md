Simple Camel 4.0.3 LTS application that restarts a route every 500ms.

Let it run for a couple of minutes and take a heap dump: 

SedaConsumer, CamelInternalProcessor, FallbackErrorHandler, RoutePipeline, LogProcessor and DefaultChannel accumulate in memory in org.apache.camel.impl.engine.DefaultRoute.services. 