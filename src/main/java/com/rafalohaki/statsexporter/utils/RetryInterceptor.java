package com.rafalohaki.statsexporter.utils;

import com.rafalohaki.statsexporter.StatsExporterPlugin;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;

public class RetryInterceptor implements Interceptor {
    private final StatsExporterPlugin plugin;
    private final int maxRetries;
    private final long retryDelayMillis;
    // List of HTTP status codes that should trigger a retry
    public static final List<Integer> RETRYABLE_CODES = Arrays.asList(
            408, // Request Timeout
            429, // Too Many Requests
            500, // Internal Server Error
            502, // Bad Gateway
            503, // Service Unavailable
            504  // Gateway Timeout
    );

    public RetryInterceptor(StatsExporterPlugin plugin) {
        this.plugin = plugin;
        // Read retry configuration
        this.maxRetries = plugin.getPluginConfig().getInt("api.maxRetries", 3);
        this.retryDelayMillis = plugin.getPluginConfig().getLong("api.retryDelaySeconds", 10) * 1000;
    }

    @NotNull
    @Override
    public Response intercept(@NotNull Chain chain) throws IOException {
        Request request = chain.request();
        Response response = null;
        IOException exception = null;
        int tryCount = 0;

        while (tryCount <= maxRetries) {
            if (tryCount > 0) {
                // --- Retry Delay Implementation ---
                // Calculate delay (simple exponential backoff)
                long delay = (long) (retryDelayMillis * Math.pow(2, tryCount - 1));
                plugin.debug("Retry attempt #" + tryCount + " for request to " + request.url() + ". Waiting " + delay + "ms...");

                // Use Thread.sleep for the delay.
                // NOTE: This blocks the current OkHttp Dispatcher thread during the sleep period.
                // OkHttp uses a pool of threads, so other requests can proceed concurrently.
                // However, if many requests require long retries simultaneously, it could
                // temporarily exhaust the dispatcher pool. For typical stats exporting,
                // this simple approach is usually acceptable and significantly less complex
                // than fully asynchronous delay handling within the interceptor chain.
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    plugin.log(Level.WARNING, "Retry delay interrupted for request to " + request.url(), e);
                    throw new IOException("Retry interrupted", e);
                }
                // --- End Retry Delay ---
            }

            // Close previous response body before retrying to avoid resource leaks
            if (response != null) {
                response.close();
            }

            try {
                // Proceed with the request (or retry)
                response = chain.proceed(request);

                // If successful, return the response immediately
                if (response.isSuccessful()) {
                    return response;
                }

                // Check if the status code is retryable
                if (RETRYABLE_CODES.contains(response.code())) {
                     plugin.log(Level.WARNING, "Request to " + request.url() + " failed with retryable code " + response.code() + ". Attempt " + tryCount + "/" + maxRetries);
                     tryCount++;
                     exception = null; // Reset exception since we got a response code
                     continue; // Continue to the next iteration for retry
                } else {
                     // Non-retryable HTTP error code, return the response
                     plugin.debug("Request to " + request.url() + " failed with non-retryable code " + response.code());
                     return response;
                }

            } catch (IOException e) {
                 // IOException (e.g., connection timeout, reset) is generally retryable
                 plugin.log(Level.WARNING, "Request to " + request.url() + " failed with IOException: " + e.getMessage() + ". Attempt " + tryCount + "/" + maxRetries);
                 exception = e; // Store the exception
                 tryCount++;
                 continue; // Continue to the next iteration for retry
            }
        }

        // If loop finishes because max retries were exceeded
        plugin.log(Level.SEVERE, "Request to " + request.url() + " failed after " + maxRetries + " retries.");
        if (exception != null) {
            throw exception; // Throw the last encountered IOException if that was the last failure reason
        } else if (response != null) {
            return response; // Return the last received response (which was an unsuccessful, retryable code)
        } else {
            // Should not happen in theory, but provides a fallback
            throw new IOException("Request failed after " + maxRetries + " retries with no response or exception recorded.");
        }
    }
}