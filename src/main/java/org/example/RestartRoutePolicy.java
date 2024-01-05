package org.example;

import org.apache.camel.CamelContext;
import org.apache.camel.CamelContextAware;
import org.apache.camel.Route;
import org.apache.camel.support.RoutePolicySupport;
import org.apache.camel.util.ObjectHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.synchronizedSet;

/**
 * Restarts controlled routes every 500ms.
 */
public class RestartRoutePolicy extends RoutePolicySupport implements CamelContextAware  {

    private static final Logger LOG = LoggerFactory.getLogger(RestartRoutePolicy.class);
    private static final long RESTART_INTERVAL_MILLIS = 500;

    private final Set<String> routeIds = synchronizedSet(new HashSet<>());
    private CamelContext camelContext;
    private ScheduledExecutorService executorService;


    @Override
    public void setCamelContext(CamelContext camelContext) {
        this.camelContext = camelContext;
    }

    @Override
    public CamelContext getCamelContext() {
        return camelContext;
    }

    @Override
    protected void doStart() {
        ObjectHelper.notNull(camelContext, "camelContext", this);
        executorService = camelContext.getExecutorServiceManager().newSingleThreadScheduledExecutor(this, RestartRoutePolicy.class.getSimpleName());
        scheduleRestart();
    }

    @Override
    protected void doStop() {
        if (camelContext != null && executorService != null) {
            camelContext.getExecutorServiceManager().shutdownNow(executorService);
        }
    }

    @Override
    public void onInit(Route route) {
        super.onInit(route);
        routeIds.add(route.getId());
    }

    private void scheduleRestart() {
        executorService.scheduleWithFixedDelay(this::restartRoutes,
            RESTART_INTERVAL_MILLIS, RESTART_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
    }

    private void restartRoutes() {
        routeIds.forEach(this::restartRoute);
    }

    private void restartRoute(String routeId) {
        if (!isRunAllowed()) {
            LOG.warn("Cannot restart route {}, route policy not running!", routeId);
            return;
        }
        stopRoute(routeId);
        startRoute(routeId);
    }

    private void stopRoute(String routeId) {
        try {
            camelContext.getRouteController().stopRoute(routeId, 10, TimeUnit.SECONDS);
            LOG.info("Stopped route: {}", routeId);
        }
        catch (Exception e) {
            System.exit(1);
        }
    }

    private void startRoute(String routeId) {
        try {
            camelContext.getRouteController().startRoute(routeId);
            LOG.info("Started route: {}", routeId);
        }
        catch (Exception e) {
            System.exit(1);
        }
    }
}
