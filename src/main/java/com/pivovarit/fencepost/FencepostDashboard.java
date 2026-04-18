package com.pivovarit.fencepost;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

public final class FencepostDashboard {

    private static final int DEFAULT_PORT = 3388;
    private static final String DEFAULT_LOCKS_TABLE = "fencepost_locks";
    private static final String DEFAULT_QUEUE_TABLE = "fencepost_queue";

    private final DashboardApi api;
    private HttpServer server;

    public FencepostDashboard(DataSource dataSource) {
        this(dataSource, DEFAULT_LOCKS_TABLE, DEFAULT_QUEUE_TABLE);
    }

    public FencepostDashboard(DataSource dataSource, String locksTable, String queueTable) {
        this.api = new DashboardApi(dataSource, locksTable, queueTable);
    }

    public void start() throws IOException {
        start(DEFAULT_PORT);
    }

    public void start(int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/", this::handle);
        server.start();
    }

    public void stop() {
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
        String resourcePath = "/com/pivovarit/fencepost/dashboard/dashboard.html";
        InputStream is = FencepostDashboard.class.getResourceAsStream(resourcePath);
        if (is == null) {
            sendJsonResponse(exchange, 500, "{\"error\":\"internal server error\"}");
            return;
        }
        byte[] body = is.readAllBytes();
        exchange.getResponseHeaders().set("Content-Type", "text/html; charset=UTF-8");
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
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

}
