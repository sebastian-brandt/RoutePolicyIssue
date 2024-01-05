package org.example;

import org.apache.camel.builder.endpoint.EndpointRouteBuilder;

import static org.apache.camel.LoggingLevel.ERROR;

public class MyRouteBuilder extends EndpointRouteBuilder {

    @Override
    public final void configure() {
		from(seda("test")).routeId("TestRoute")
			.routePolicy(new RestartRoutePolicy())
			.onException(Exception.class)
				.handled(true)
				.log(ERROR, "test", "Error: ${exception.message}")
			.end()
			.log("Received: ${body}")
			;
    }
}
