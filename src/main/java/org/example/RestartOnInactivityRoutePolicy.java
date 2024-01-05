package org.example;

import org.apache.camel.CamelContext;
import org.apache.camel.CamelContextAware;
import org.apache.camel.Exchange;
import org.apache.camel.Route;
import org.apache.camel.support.RoutePolicySupport;
import org.apache.camel.util.ObjectHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.synchronizedSet;

/**
 * Route policy that restarts a route when no new exchanges arrive within a certain timeout.
 */
public class RestartOnInactivityRoutePolicy extends RoutePolicySupport implements CamelContextAware  {

    private static final Logger LOG = LoggerFactory.getLogger(RestartOnInactivityRoutePolicy.class);

    /** When no messaged where received after this timeout in millis, the consuming route will be restarted */
    private static final long INACTIVE_TIMEOUT_MILLIS = 10_000;

    /** Interval in millis for checking if messages were received */
    private static final long CHECK_INTERVAL_MILLIS = 1_000;

    /** Timeout in millis for stopping a route */
    private static final long STOP_ROUTE_TIMEOUT_MILLIS = 10_000;


    /** Route IDs of routes that are managed by this policy */
    private final Set<String> routeIds = synchronizedSet(new HashSet<>());

    /** Epoch millis timestamp of when the last message was received on a route */
    private final ConcurrentMap<String, Long> routeId2LastMessageMillis = new ConcurrentHashMap<>();

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
        executorService = camelContext.getExecutorServiceManager().newSingleThreadScheduledExecutor(this, RestartOnInactivityRoutePolicy.class.getSimpleName());
        scheduleActivityCheck();
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

        String routeId = route.getId();
        routeIds.add(routeId);
        updateTimestamp(routeId);
    }

    @Override
    public void onExchangeBegin(Route route, Exchange exchange) {
        String routeId = route.getId();
        updateTimestamp(routeId);
    }

    private void scheduleActivityCheck() {
        executorService.scheduleWithFixedDelay(this::checkActivityAll,
            CHECK_INTERVAL_MILLIS, CHECK_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
    }

    private void checkActivityAll() {
        routeIds.forEach(this::checkActivity);
    }

    private void checkActivity(String routeId) {
        boolean isRouteStarted = camelContext.getRouteController().getRouteStatus(routeId).isStarted();
        if (isRouteStarted) {
            if (isReceivingMessages(routeId)) {
                LOG.debug("Route: {} is receiving messages", routeId);
            }
            else {
                LOG.warn("Route: {} is not receiving messages. Restarting...", routeId);
                restartRoute(routeId);
                LOG.info("Route: {} restarted.", routeId);
            }
        }
    }

    private void restartRoute(String routeId) {
        if (!isRunAllowed()) {
            LOG.warn("Cannot restart route {}, route policy not running!", routeId);
            return;
        }
        stopRoute(routeId);
        startRoute(routeId);
    }

    private void updateTimestamp(String routeId) {
        long lastMessageMillis = System.currentTimeMillis();
        routeId2LastMessageMillis.put(routeId, lastMessageMillis);
    }

    private boolean isReceivingMessages(String routeId) {
        long lastMessageMillis = routeId2LastMessageMillis.getOrDefault(routeId, -1L);
        long timeSinceLastMessageMillis = System.currentTimeMillis() - lastMessageMillis;
        return timeSinceLastMessageMillis < INACTIVE_TIMEOUT_MILLIS;
    }

    private void stopRoute(String routeId) {
        try {
            LOG.debug("Stopping route {}...", routeId);
            camelContext.getRouteController().stopRoute(routeId, STOP_ROUTE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            LOG.debug("Stopped route {}", routeId);
        }
        catch (Exception e) {
            LOG.error("Error stopping route {} -- shutting down application now!", routeId);
            System.exit(1);
        }
    }

    private void startRoute(String routeId) {
        try {
            LOG.debug("Starting route {}...", routeId);
            camelContext.getRouteController().startRoute(routeId);
            LOG.debug("Started route {}", routeId);
        }
        catch (Exception e) {
            LOG.error("Error starting route {} -- shutting down application now!", routeId);
            System.exit(1);
        }
    }
}
