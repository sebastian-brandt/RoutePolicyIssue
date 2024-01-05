package org.example;

import org.apache.camel.builder.endpoint.EndpointRouteBuilder;
import org.springframework.stereotype.Component;

import static org.apache.camel.LoggingLevel.ERROR;

@Component
public class MyRouteBuilder extends EndpointRouteBuilder {

    @Override
    public final void configure() {
        // @formatter:off

		from(seda("test")).routeId("TestRoute")
			.routePolicy(new RestartRoutePolicy())
			.onException(Exception.class)
				.handled(true)
				.log(ERROR, "test", "Error while consuming feed: ${exception.message}")
			.end()
			.log("Received: ${body}")
			;

		// @formatter:on
    }
}
