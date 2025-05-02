package com.rafalohaki.statsexporter.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.rafalohaki.statsexporter.StatsExporterPlugin;
import okhttp3.*;
import org.bukkit.Bukkit;
import org.jetbrains.annotations.NotNull;
// Import PluginMeta if needed, though often implicitly available via Paper API dependency
// import io.papermc.paper.plugin.configuration.PluginMeta;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

public class HttpUtils {

    private final StatsExporterPlugin plugin;
    private final OkHttpClient httpClient;
    private final Gson gson;
    private final String apiUrl;
    private final String apiKey;
    private final int batchMaxSize;
    private final long batchMaxDelayMillis;
    private final String userAgent; // Store user agent string

    private final Queue<Map<String, Object>> batchQueue = new ConcurrentLinkedQueue<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "StatsExporter-HttpUtils-Scheduler")
    );
    private ScheduledFuture<?> scheduledSendTask;
    private final AtomicBoolean sendLock = new AtomicBoolean(false);
    private final AtomicLong lastSendTime = new AtomicLong(System.currentTimeMillis());

    public HttpUtils(StatsExporterPlugin plugin) {
        this.plugin = plugin;
        this.httpClient = plugin.getHttpClient();
        this.gson = new GsonBuilder().create();

        this.apiUrl = plugin.getPluginConfig().getString("api.url");
        this.apiKey = plugin.getPluginConfig().getString("api.key");
        this.batchMaxSize = plugin.getPluginConfig().getInt("batch.maxSize", 50);
        this.batchMaxDelayMillis = plugin.getPluginConfig().getLong("batch.maxDelaySeconds", 15) * 1000;

        // Use plugin.getPluginMeta() instead of plugin.getDescription()
        this.userAgent = "StatsExporterPlugin/" + plugin.getPluginMeta().getVersion();

        if (batchMaxDelayMillis > 0) {
           startDelayedSendTask();
        }
    }

    public void queuePlayerData(Map<String, Object> playerDataMap) {
        if (playerDataMap == null || playerDataMap.isEmpty()) {
            return;
        }
        if (batchQueue.offer(playerDataMap)) {
            plugin.debug("Queued player data. Queue size: " + batchQueue.size());
        } else {
            plugin.log(Level.WARNING, "Failed to add player data to the queue (queue might be full or restricted).");
            return;
        }

        if (batchQueue.size() >= batchMaxSize) {
            plugin.debug("Batch size reached (" + batchQueue.size() + "/" + batchMaxSize + "). Triggering send.");
            cancelScheduledSendTask();
            Bukkit.getScheduler().runTaskAsynchronously(plugin, this::sendBatchAsync);
        } else if (batchMaxDelayMillis > 0 && scheduledSendTask == null) {
             startDelayedSendTask();
        }
    }

    private void startDelayedSendTask() {
         if (batchMaxDelayMillis <= 0 || scheduledSendTask != null || scheduler.isShutdown()) {
             return;
         }
         plugin.debug("Scheduling batch send task with delay: " + batchMaxDelayMillis + "ms");
         try {
            scheduledSendTask = scheduler.schedule(() -> {
                 plugin.debug("Scheduled delay elapsed. Triggering async batch send via Bukkit Scheduler.");
                 Bukkit.getScheduler().runTaskAsynchronously(plugin, this::sendBatchAsync);
                 scheduledSendTask = null;
            }, batchMaxDelayMillis, TimeUnit.MILLISECONDS);
         } catch (RejectedExecutionException e) {
             plugin.log(Level.WARNING, "Could not schedule batch send task (scheduler likely shutdown): " + e.getMessage());
         }
    }

    private void cancelScheduledSendTask() {
         if (scheduledSendTask != null) {
             plugin.debug("Cancelling scheduled batch send task.");
             scheduledSendTask.cancel(false);
             scheduledSendTask = null;
         }
    }

    public void sendBatchAsync() {
        if (!sendLock.compareAndSet(false, true)) {
            plugin.debug("Send batch already in progress. Skipping.");
            return;
        }

        if (batchQueue.isEmpty()) {
            plugin.debug("Batch queue is empty after acquiring lock. Nothing to send.");
            sendLock.set(false);
            return;
        }

        List<Map<String, Object>> batchToSend = new ArrayList<>();
        int count = 0;
        while (!batchQueue.isEmpty() && count < batchMaxSize) {
            Map<String, Object> data = batchQueue.poll();
            if (data != null) {
                batchToSend.add(data);
                count++;
            } else {
                break;
            }
        }

        if (batchToSend.isEmpty()) {
            plugin.debug("Batch queue became empty during polling. Nothing to send.");
            sendLock.set(false);
            return;
        }

        plugin.debug("Sending batch of " + batchToSend.size() + " player data entries.");

        String jsonPayload;
        try {
            jsonPayload = gson.toJson(batchToSend);
        } catch (Exception e) {
            plugin.log(Level.SEVERE, "Failed to serialize batch data to JSON: " + e.getMessage(), e);
            sendLock.set(false);
            return;
        }

        RequestBody body = RequestBody.create(jsonPayload, MediaType.get("application/json; charset=utf-8"));
        Request request = new Request.Builder()
                .url(this.apiUrl)
                .header("Authorization", "Bearer " + this.apiKey)
                .header("User-Agent", this.userAgent) // Use stored user agent
                .post(body)
                .build();

        httpClient.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(@NotNull Call call, @NotNull IOException e) {
                try {
                    plugin.log(Level.WARNING, "Failed to send stats batch to API: " + e.getMessage());
                    // Consider adding retry logic or saving failed batches here
                } finally {
                     sendLock.set(false);
                }
            }

            @Override
            public void onResponse(@NotNull Call call, @NotNull Response response) {
                try (ResponseBody responseBody = response.body()) {
                    if (response.isSuccessful()) {
                        plugin.debug("Successfully sent stats batch. Response code: " + response.code());
                    } else {
                        String responseBodyString = responseBody != null ? responseBody.string() : "[No Response Body]";
                        plugin.log(Level.WARNING,"API endpoint returned an error. Code: " + response.code() + ", Response: " + responseBodyString.substring(0, Math.min(responseBodyString.length(), 500)));
                    }
                } catch (IOException e) {
                     plugin.log(Level.WARNING, "IOException while reading API response body: " + e.getMessage());
                } finally {
                     sendLock.set(false);
                     lastSendTime.set(System.currentTimeMillis());
                     if (!batchQueue.isEmpty() && batchMaxDelayMillis > 0) {
                        startDelayedSendTask();
                     }
                }
            }
        });
    }

    public void flushBatchSync() {
        plugin.log(Level.INFO, "Flushing remaining data synchronously...");

        if (!sendLock.compareAndSet(false, true)) {
            plugin.log(Level.WARNING, "Could not acquire send lock during sync flush. An async send might be in progress. Data might not be flushed.");
            try { Thread.sleep(500); } catch (InterruptedException ignored) {}
            if (!sendLock.compareAndSet(false, true)) {
                plugin.log(Level.SEVERE, "Send lock still held during sync flush. Aborting synchronous send.");
                return;
            }
        }

        try {
            List<Map<String, Object>> batchToSend = new ArrayList<>();
            Map<String, Object> data;
            while ((data = batchQueue.poll()) != null) {
                batchToSend.add(data);
            }

            if (batchToSend.isEmpty()) {
                plugin.log(Level.INFO, "No data in queue to flush.");
                return;
            }

             plugin.log(Level.INFO, "Attempting to send " + batchToSend.size() + " remaining entries synchronously.");

            String jsonPayload;
            try {
                jsonPayload = gson.toJson(batchToSend);
            } catch (Exception e) {
                plugin.log(Level.SEVERE, "Failed to serialize final batch data to JSON: " + e.getMessage(), e);
                return;
            }

            RequestBody body = RequestBody.create(jsonPayload, MediaType.get("application/json; charset=utf-8"));
            Request request = new Request.Builder()
                    .url(this.apiUrl)
                    .header("Authorization", "Bearer " + this.apiKey)
                    // Use stored user agent + suffix
                    .header("User-Agent", this.userAgent + " (SyncFlush)")
                    .post(body)
                    .build();

            try (Response response = httpClient.newCall(request).execute()) {
                if (response.isSuccessful()) {
                    plugin.log(Level.INFO, "Successfully sent final batch synchronously. Code: " + response.code());
                } else {
                    try (ResponseBody responseBody = response.body()) {
                        String responseBodyString = responseBody != null ? responseBody.string() : "[No Response Body]";
                        plugin.log(Level.WARNING, "API endpoint returned an error during sync flush. Code: " + response.code() + ", Response: " + responseBodyString.substring(0, Math.min(responseBodyString.length(), 500)));
                    }
                }
            } catch (IOException e) {
                plugin.log(Level.SEVERE, "Failed to send final batch synchronously: " + e.getMessage(), e);
            }
        } finally {
             sendLock.set(false);
        }
    }

    public void shutdown() {
        plugin.log(Level.INFO, "Shutting down HttpUtils scheduler...");
        cancelScheduledSendTask();
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
                plugin.log(Level.WARNING, "HttpUtils scheduler did not terminate gracefully.");
            } else {
                 plugin.log(Level.INFO, "HttpUtils scheduler shutdown complete.");
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public int getQueueSize() {
        return batchQueue.size();
    }
}