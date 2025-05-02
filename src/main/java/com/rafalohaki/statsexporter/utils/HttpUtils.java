package com.rafalohaki.statsexporter.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.rafalohaki.statsexporter.StatsExporterPlugin;
import okhttp3.*;
import org.bukkit.Bukkit;
import org.jetbrains.annotations.NotNull;

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
    private final String userAgent;

    private final Queue<Map<String, Object>> batchQueue = new ConcurrentLinkedQueue<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "StatsExporter-HttpUtils-Scheduler")
    );
    private ScheduledFuture<?> scheduledSendTask;
    private final AtomicBoolean sendLock = new AtomicBoolean(false);
    private final AtomicLong lastSendTime = new AtomicLong(System.currentTimeMillis());

    // Constructor now initializes the OkHttpClient based on config
    public HttpUtils(StatsExporterPlugin plugin) {
        this.plugin = plugin;
        this.gson = new GsonBuilder().create();

        this.apiUrl = plugin.getPluginConfig().getString("api.url");
        this.apiKey = plugin.getPluginConfig().getString("api.key");
        this.batchMaxSize = plugin.getPluginConfig().getInt("batch.maxSize", 50);
        this.batchMaxDelayMillis = plugin.getPluginConfig().getLong("batch.maxDelaySeconds", 15) * 1000;
        this.userAgent = "StatsExporterPlugin/" + plugin.getPluginMeta().getVersion();

        // Initialize OkHttpClient here
        int timeoutSeconds = plugin.getPluginConfig().getInt("api.timeoutSeconds", 10);
        OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder()
                .connectTimeout(timeoutSeconds, TimeUnit.SECONDS)
                .readTimeout(timeoutSeconds, TimeUnit.SECONDS)
                .writeTimeout(timeoutSeconds, TimeUnit.SECONDS);
                // Add other configurations like connection pool if needed

        // Add the RetryInterceptor if maxRetries > 0
        int maxRetriesConfig = plugin.getPluginConfig().getInt("api.maxRetries", 3);
        if (maxRetriesConfig > 0) {
             clientBuilder.addInterceptor(new RetryInterceptor(plugin));
             plugin.debug("Added RetryInterceptor with max " + maxRetriesConfig + " retries.");
        }

        this.httpClient = clientBuilder.build();
        plugin.debug("OkHttpClient initialized with " + timeoutSeconds + "s timeout.");

        if (batchMaxDelayMillis > 0) {
           startDelayedSendTask(); // Will log debug message internally
        }
    }

    /**
     * Returns the OkHttpClient instance used by this utility.
     * This client is configured based on the plugin's config during HttpUtils initialization.
     *
     * @return The OkHttpClient instance
     */
    public OkHttpClient getHttpClient() {
        return this.httpClient;
    }


    public void queuePlayerData(Map<String, Object> playerDataMap) {
        if (playerDataMap == null || playerDataMap.isEmpty()) {
            return;
        }
        if (batchQueue.offer(playerDataMap)) {
            plugin.debug("Queued player data. Queue size: " + batchQueue.size()); // Use debug
        } else {
            plugin.log(Level.WARNING, "Failed to add player data to the queue (queue might be full or restricted)."); // Keep WARNING
            return;
        }

        // Trigger send if size reached OR if delay not running (first item after idle)
        boolean shouldTriggerBySize = batchQueue.size() >= batchMaxSize;
        boolean shouldTriggerByTime = batchMaxDelayMillis > 0 && scheduledSendTask == null;

        if (shouldTriggerBySize) {
            plugin.debug("Batch size reached (" + batchQueue.size() + "/" + batchMaxSize + "). Triggering send."); // Use debug
            cancelScheduledSendTask(); // Will log debug internally if needed
            // Run async via Bukkit scheduler to avoid blocking caller thread
            Bukkit.getScheduler().runTaskAsynchronously(plugin, this::sendBatchAsync);
        } else if (shouldTriggerByTime && !batchQueue.isEmpty()) {
             // Only start delay task if size not met AND queue is not empty AND task not already running
             startDelayedSendTask(); // Will log debug internally
        }
    }


    private void startDelayedSendTask() {
         if (batchMaxDelayMillis <= 0 || scheduledSendTask != null || scheduler.isShutdown()) {
             return;
         }
         // Ensure there's something to send before scheduling
         if (!batchQueue.isEmpty()) {
             plugin.debug("Scheduling batch send task with delay: " + batchMaxDelayMillis + "ms"); // Use debug
             try {
                scheduledSendTask = scheduler.schedule(() -> {
                     plugin.debug("Scheduled delay elapsed. Triggering async batch send via Bukkit Scheduler."); // Use debug
                     Bukkit.getScheduler().runTaskAsynchronously(plugin, this::sendBatchAsync);
                     scheduledSendTask = null; // Clear task ref after execution starts
                }, batchMaxDelayMillis, TimeUnit.MILLISECONDS);
             } catch (RejectedExecutionException e) {
                 plugin.log(Level.WARNING, "Could not schedule batch send task (scheduler likely shutdown): " + e.getMessage()); // Keep WARNING
             }
         } else {
              plugin.debug("startDelayedSendTask called but queue is empty, not scheduling."); // Use debug
         }
    }

    private void cancelScheduledSendTask() {
         if (scheduledSendTask != null) {
             plugin.debug("Cancelling scheduled batch send task."); // Use debug
             scheduledSendTask.cancel(false); // Don't interrupt if already running
             scheduledSendTask = null;
         }
    }

    public void sendBatchAsync() {
        if (!sendLock.compareAndSet(false, true)) {
            plugin.debug("Send batch already in progress. Skipping."); // Use debug
            return;
        }

        // Drain the queue up to max size AFTER acquiring the lock
        List<Map<String, Object>> batchToSend = new ArrayList<>();
        int count = 0;
        while (!batchQueue.isEmpty() && count < batchMaxSize) {
            Map<String, Object> data = batchQueue.poll();
            if (data != null) {
                batchToSend.add(data);
                count++;
            } else {
                break; // Should not happen with ConcurrentLinkedQueue, but safe practice
            }
        }

        // If nothing was actually drained (e.g., another thread drained it between lock check and poll)
        if (batchToSend.isEmpty()) {
            plugin.debug("Batch queue became empty before sending. Releasing lock."); // Use debug
            sendLock.set(false);
             // Check if more data arrived after draining and restart delayed task if needed
            if (!batchQueue.isEmpty()) startDelayedSendTask();
            return;
        }

        plugin.debug("Sending batch of " + batchToSend.size() + " player data entries."); // Use debug

        String jsonPayload;
        try {
            jsonPayload = gson.toJson(batchToSend);
        } catch (Exception e) {
            plugin.log(Level.SEVERE, "Failed to serialize batch data to JSON: " + e.getMessage(), e); // Keep SEVERE
            sendLock.set(false); // Release lock on error
             // Consider requeueing or saving failed batch here (though retry interceptor helps)
            return;
        }

        RequestBody body = RequestBody.create(jsonPayload, MediaType.get("application/json; charset=utf-8"));
        Request request = new Request.Builder()
                .url(this.apiUrl)
                .header("Authorization", "Bearer " + this.apiKey)
                .header("User-Agent", this.userAgent)
                .post(body)
                .build();

        httpClient.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(@NotNull Call call, @NotNull IOException e) {
                try {
                    // Keep SEVERE for final network/API failures AFTER retries handled by interceptor
                    plugin.log(Level.SEVERE, "Failed to send stats batch to API after retries: " + e.getMessage());
                    // Data in batchToSend is lost after all retries failed.
                } finally {
                     sendLock.set(false); // Release lock even on failure
                     // Check if more data arrived and restart delayed task if needed
                     if (!batchQueue.isEmpty()) startDelayedSendTask();
                }
            }

            @Override
            public void onResponse(@NotNull Call call, @NotNull Response response) {
                try (ResponseBody responseBody = response.body()) {
                    if (response.isSuccessful()) {
                        plugin.debug("Successfully sent stats batch. Response code: " + response.code()); // Use debug
                    } else if (RetryInterceptor.RETRYABLE_CODES.contains(response.code())) {
                         // This case should ideally not be reached if the interceptor worked, but log defensively
                         plugin.log(Level.SEVERE, "API endpoint returned a retryable error code " + response.code() + " unexpectedly after interceptor handling.");
                    } else {
                        // Keep WARNING for non-retryable API-side errors AFTER retries
                        String responseBodyString = responseBody != null ? responseBody.string() : "[No Response Body]";
                        plugin.log(Level.WARNING,"API endpoint returned an error. Code: " + response.code() + ", Response: " + responseBodyString.substring(0, Math.min(responseBodyString.length(), 500)));
                        // Data in batchToSend is considered sent, even if API reports error.
                    }
                } catch (IOException e) {
                     // Keep WARNING for issues reading response
                     plugin.log(Level.WARNING, "IOException while reading API response body: " + e.getMessage());
                } finally {
                     sendLock.set(false); // Release lock after handling response
                     lastSendTime.set(System.currentTimeMillis());
                     // Check if more data arrived and restart delayed task if needed
                     if (!batchQueue.isEmpty()) startDelayedSendTask();
                }
            }
        });
    }

    // Keep INFO/WARNING/SEVERE for synchronous flush operations as they are significant (shutdown)
    public void flushBatchSync() {
        plugin.log(Level.INFO, "Flushing remaining data synchronously...");

        if (!sendLock.compareAndSet(false, true)) {
            plugin.log(Level.WARNING, "Could not acquire send lock during sync flush. An async send might be in progress. Trying again shortly...");
            try { Thread.sleep(500); } catch (InterruptedException ignored) {}
            if (!sendLock.compareAndSet(false, true)) {
                plugin.log(Level.SEVERE, "Send lock still held during sync flush. Aborting synchronous send. Some data may be lost.");
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
                // Data is lost if serialization fails here
                return;
            }

            RequestBody body = RequestBody.create(jsonPayload, MediaType.get("application/json; charset=utf-8"));
            Request request = new Request.Builder()
                    .url(this.apiUrl)
                    .header("Authorization", "Bearer " + this.apiKey)
                    .header("User-Agent", this.userAgent + " (SyncFlush)")
                    .post(body)
                    .build();

            // Note: The RetryInterceptor WILL still apply to this synchronous call
            try (Response response = httpClient.newCall(request).execute()) { // Synchronous execution
                if (response.isSuccessful()) {
                    plugin.log(Level.INFO, "Successfully sent final batch synchronously. Code: " + response.code());
                } else {
                    try (ResponseBody responseBody = response.body()) {
                        String responseBodyString = responseBody != null ? responseBody.string() : "[No Response Body]";
                        plugin.log(Level.WARNING, "API endpoint returned an error during sync flush. Code: " + response.code() + ", Response: " + responseBodyString.substring(0, Math.min(responseBodyString.length(), 500)));
                    }
                }
            } catch (IOException e) {
                plugin.log(Level.SEVERE, "Failed to send final batch synchronously (after potential retries): " + e.getMessage(), e);
                // Data is lost if sync send fails after retries
            }
        } finally {
             sendLock.set(false); // Release lock
        }
    }

    // Keep INFO/WARNING for shutdown sequence
    public void shutdown() {
        plugin.log(Level.INFO, "Shutting down HttpUtils scheduler...");
        cancelScheduledSendTask(); // Logs debug internally if needed
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
            plugin.log(Level.WARNING, "Interrupted while waiting for HttpUtils scheduler shutdown.");
        }

        // Also shut down the OkHttpClient dispatcher gracefully
        if (httpClient != null) {
            plugin.log(Level.INFO, "Shutting down OkHttpClient dispatcher...");
            httpClient.dispatcher().executorService().shutdown();
            httpClient.connectionPool().evictAll();
            try {
                if (!httpClient.dispatcher().executorService().awaitTermination(5, TimeUnit.SECONDS)) {
                     plugin.log(Level.WARNING, "OkHttpClient dispatcher did not shut down gracefully within 5 seconds. Forcing shutdown...");
                    httpClient.dispatcher().executorService().shutdownNow();
                } else {
                     plugin.log(Level.INFO, "OkHttpClient dispatcher shut down.");
                }
                // Close the cache if one was configured
                if (httpClient.cache() != null) {
                     httpClient.cache().close();
                }
            } catch (InterruptedException e) {
                 Thread.currentThread().interrupt();
                 plugin.log(Level.WARNING, "Interrupted while shutting down OkHttpClient dispatcher. Forcing shutdown.");
                 httpClient.dispatcher().executorService().shutdownNow();
            } catch (Exception e) { // Catch potential IOException from cache().close()
                 plugin.log(Level.WARNING, "Error closing OkHttpClient cache during shutdown.", e);
            }
        }
    }

    public int getQueueSize() {
        return batchQueue.size();
    }
}