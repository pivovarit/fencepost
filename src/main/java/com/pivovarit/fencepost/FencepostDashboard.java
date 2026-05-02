package com.pivovarit.fencepost;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.postgresql.PGConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public final class FencepostDashboard {

    private static final Logger logger = LoggerFactory.getLogger(FencepostDashboard.class);
    static final String DASHBOARD_CHANNEL = "fencepost_dashboard";
    private static final int DEFAULT_PORT = 3388;
    private static final String DEFAULT_LOCKS_TABLE = "fencepost_locks";
    private static final String DEFAULT_QUEUE_TABLE = "fencepost_queue";
    private static final int LISTEN_TIMEOUT_MS = 5000;
    private static final byte[] REFRESH_EVENT = "data: refresh\n\n".getBytes(StandardCharsets.UTF_8);
    private static final byte[] DASHBOARD_HTML = loadDashboardHtml();

    private final DashboardApi api;
    private final ListenerConnection listener;
    private final List<OutputStream> sseClients = new CopyOnWriteArrayList<>();
    private HttpServer server;
    private volatile Thread listenerThread;

    public FencepostDashboard(DataSource dataSource) {
        this(dataSource, DEFAULT_LOCKS_TABLE, DEFAULT_QUEUE_TABLE);
    }

    public FencepostDashboard(DataSource dataSource, String locksTable, String queueTable) {
        this.api = new DashboardApi(dataSource, locksTable, queueTable);
        this.listener = new ListenerConnection(dataSource, DASHBOARD_CHANNEL);
    }

    public void start() throws IOException {
        start(DEFAULT_PORT);
    }

    public void start(int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/", this::handle);
        server.start();
        try {
            listener.ensure();
        } catch (SQLException e) {
            server.stop(0);
            throw new IOException("failed to start dashboard listener", e);
        }
        startListener();
    }

    public void stop() {
        stopListener();
        for (OutputStream out : sseClients) {
            try { out.close(); } catch (IOException e) { logger.trace("failed to close SSE client output stream", e); }
        }
        sseClients.clear();
        if (server != null) {
            server.stop(0);
        }
    }

    public int getPort() {
        if (server == null) {
            throw new IllegalStateException("Server not started");
        }
        return server.getAddress().getPort();
    }

    private void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String path = exchange.getRequestURI().getPath();

        if (!"GET".equalsIgnoreCase(method)) {
            sendJsonResponse(exchange, 404, "{\"error\":\"not found\"}");
            return;
        }

        try {
            if ("/".equals(path)) {
                serveDashboardHtml(exchange);
            } else if ("/api/events".equals(path)) {
                handleSse(exchange);
                return;
            } else if ("/api/status".equals(path)) {
                String json = api.status();
                sendJsonResponse(exchange, 200, json);
            } else if ("/api/locks".equals(path)) {
                String json = api.locks();
                sendJsonResponse(exchange, 200, json);
            } else if (path.startsWith("/api/locks/") && path.length() > "/api/locks/".length()) {
                String name = URLDecoder.decode(path.substring("/api/locks/".length()), StandardCharsets.UTF_8);
                String json = api.lock(name);
                if ("null".equals(json)) {
                    sendJsonResponse(exchange, 404, "{\"error\":\"not found\"}");
                } else {
                    sendJsonResponse(exchange, 200, json);
                }
            } else if ("/api/queues".equals(path)) {
                String json = api.queues();
                sendJsonResponse(exchange, 200, json);
            } else if (path.startsWith("/api/queues/") && path.contains("/messages/")) {
                String sub = path.substring("/api/queues/".length());
                int msgIdx = sub.indexOf("/messages/");
                String queueName = URLDecoder.decode(sub.substring(0, msgIdx), StandardCharsets.UTF_8);
                long messageId = Long.parseLong(sub.substring(msgIdx + "/messages/".length()));
                String json = api.message(queueName, messageId);
                if ("null".equals(json)) {
                    sendJsonResponse(exchange, 404, "{\"error\":\"not found\"}");
                } else {
                    sendJsonResponse(exchange, 200, json);
                }
            } else if (path.startsWith("/api/queues/") && path.length() > "/api/queues/".length()) {
                String name = URLDecoder.decode(path.substring("/api/queues/".length()), StandardCharsets.UTF_8);
                String json = api.queue(name);
                sendJsonResponse(exchange, 200, json);
            } else {
                sendJsonResponse(exchange, 404, "{\"error\":\"not found\"}");
            }
        } catch (Exception e) {
            sendJsonResponse(exchange, 500, "{\"error\":\"internal server error\"}");
        }
    }

    private void serveDashboardHtml(HttpExchange exchange) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "text/html; charset=UTF-8");
        exchange.sendResponseHeaders(200, DASHBOARD_HTML.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(DASHBOARD_HTML);
        }
    }

    private static byte[] loadDashboardHtml() {
        String resourcePath = "/com/pivovarit/fencepost/dashboard/dashboard.html";
        try (InputStream is = FencepostDashboard.class.getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new IllegalStateException("dashboard.html not found on classpath");
            }
            return is.readAllBytes();
        } catch (IOException e) {
            throw new IllegalStateException("failed to load dashboard.html", e);
        }
    }

    private static void sendJsonResponse(HttpExchange exchange, int status, String json) throws IOException {
        byte[] body = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    private void handleSse(HttpExchange exchange) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "text/event-stream");
        exchange.getResponseHeaders().set("Cache-Control", "no-cache");
        exchange.sendResponseHeaders(200, 0);
        OutputStream out = exchange.getResponseBody();
        sseClients.add(out);
        out.write("data: connected\n\n".getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    private void broadcastRefresh() {
        for (OutputStream out : sseClients) {
            try {
                out.write(REFRESH_EVENT);
                out.flush();
            } catch (IOException e) {
                sseClients.remove(out);
            }
        }
    }

    private void startListener() {
        listenerThread = new Thread(() -> {
            try {
                while (!listener.isStopped() && !Thread.currentThread().isInterrupted()) {
                    try {
                        var conn = listener.ensure();
                        var pgConn = conn.unwrap(PGConnection.class);
                        var notifications = pgConn.getNotifications(LISTEN_TIMEOUT_MS);
                        if (notifications != null && notifications.length > 0) {
                            broadcastRefresh();
                        }
                    } catch (Exception e) {
                        listener.close();
                        if (!listener.isStopped() && !Thread.currentThread().isInterrupted()) {
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    }
                }
            } finally {
                listener.close();
            }
        });
        listenerThread.setDaemon(true);
        listenerThread.setName("fencepost-dashboard-listener");
        listenerThread.start();
    }

    private void stopListener() {
        Thread thread = listenerThread;
        if (thread != null) {
            thread.interrupt();
            listener.stop();
            try {
                thread.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            listenerThread = null;
        } else {
            listener.stop();
        }
    }

}
