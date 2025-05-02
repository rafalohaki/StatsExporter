package com.rafalohaki.statsexporter;

import com.rafalohaki.statsexporter.commands.StatsExporterCommand;
import com.rafalohaki.statsexporter.listeners.PlayerListener;
import com.rafalohaki.statsexporter.tasks.StatsSyncTask;
import com.rafalohaki.statsexporter.utils.HttpUtils;
import com.rafalohaki.statsexporter.utils.StatsFileReader;
import okhttp3.OkHttpClient;
import org.bukkit.Bukkit;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitTask;

// import java.util.concurrent.TimeUnit; // Removed unused import
import java.util.logging.Level;

public final class StatsExporterPlugin extends JavaPlugin {

    private static StatsExporterPlugin instance;
    private FileConfiguration pluginConfig;
    // private OkHttpClient httpClient; // Removed unused field
    private HttpUtils httpUtils;
    private StatsFileReader statsFileReader;
    private BukkitTask periodicSyncTask;
    private StatsSyncTask bulkImportRunnable;
    private BukkitTask bulkImportBukkitTask;


    @Override
    public void onEnable() {
        instance = this;
        log(Level.INFO, "Loading configuration..."); // Keep INFO for major steps
        saveDefaultConfig();
        pluginConfig = getConfig();
        reloadConfig(); // Load initial config values correctly
        pluginConfig = getConfig(); // Refresh reference after reloadConfig()

        log(Level.INFO, "Validating configuration..."); // Keep INFO for major steps
        String apiUrl = pluginConfig.getString("api.url", "");
        String apiKey = pluginConfig.getString("api.key", "");

        if (apiUrl == null || apiUrl.isEmpty() || apiUrl.isBlank()) {
            log(Level.SEVERE, "API URL is not configured in config.yml! Disabling plugin."); // Keep SEVERE
            getServer().getPluginManager().disablePlugin(this);
            return;
        }
        if (apiKey == null || apiKey.isEmpty() || apiKey.isBlank() || apiKey.equals("YOUR_SECRET_API_KEY_HERE")) {
             log(Level.SEVERE, "API Key is not configured or is set to the default placeholder in config.yml! Disabling plugin."); // Keep SEVERE
             getServer().getPluginManager().disablePlugin(this);
             return;
        }
        log(Level.INFO, "Configuration loaded and validated successfully."); // Keep INFO

        log(Level.INFO, "Initializing components..."); // Keep INFO
        // HttpUtils now initializes OkHttpClient internally based on config
        this.httpUtils = new HttpUtils(this);
        // this.httpClient = this.httpUtils.getHttpClient(); // Removed assignment to unused field
        this.statsFileReader = new StatsFileReader(this);

        log(Level.INFO, "Components initialized."); // Keep INFO

        log(Level.INFO, "Registering event listeners..."); // Keep INFO
        getServer().getPluginManager().registerEvents(new PlayerListener(this), this);
        log(Level.INFO, "Event listeners registered."); // Keep INFO

        log(Level.INFO, "Registering commands..."); // Keep INFO
        try {
            getCommand("statsexporter").setExecutor(new StatsExporterCommand(this));
        } catch (NullPointerException e) {
             log(Level.SEVERE, "Failed to register command 'statsexporter'. Is it defined correctly in plugin.yml?"); // Keep SEVERE
        }
        log(Level.INFO, "Commands registered."); // Keep INFO

        if (pluginConfig.getBoolean("sync.periodicOnlineSync", true)) {
            long intervalMinutes = pluginConfig.getLong("sync.periodicOnlineSyncIntervalMinutes", 20);
            if (intervalMinutes > 0) {
                long intervalTicks = intervalMinutes * 60 * 20;
                // Keep INFO for scheduling summary
                log(Level.INFO, "Scheduling periodic online player sync every " + intervalMinutes + " minutes (" + intervalTicks + " ticks).");

                 try {
                    StatsSyncTask syncTaskRunnable = new StatsSyncTask(this);
                    Runnable syncTaskAsRunnable = syncTaskRunnable;
                    this.periodicSyncTask = getServer().getScheduler().runTaskTimerAsynchronously(
                            this,
                            syncTaskAsRunnable,
                            1200L, // Start after 1 minute
                            intervalTicks); // Repeat every interval

                 } catch (Exception e) {
                    log(Level.SEVERE, "Failed to schedule periodic sync task", e); // Keep SEVERE
                 }

            } else {
                 log(Level.INFO, "Periodic online player sync interval is <= 0, task disabled."); // Keep INFO
            }
        } else {
            log(Level.INFO, "Periodic online player sync is disabled in config."); // Keep INFO
        }

        log(Level.INFO, "StatsExporter has been enabled successfully!"); // Keep INFO
    }

    @Override
    public void onDisable() {
        log(Level.INFO, "Disabling StatsExporter..."); // Keep INFO

        log(Level.INFO, "Cancelling scheduled tasks..."); // Keep INFO
        if (periodicSyncTask != null && !periodicSyncTask.isCancelled()) {
            periodicSyncTask.cancel();
            log(Level.INFO, "Cancelled periodic sync task."); // Keep INFO
        }
        cancelBulkImportTask(); // Internal logs are INFO/debug

        if (httpUtils != null) {
            log(Level.INFO, "Flushing remaining batched data and shutting down HttpUtils..."); // Keep INFO
            httpUtils.flushBatchSync(); // Uses INFO/WARNING internally // Should ideally handle retry queue too
            httpUtils.shutdown(); // Uses INFO/WARNING internally
            log(Level.INFO, "HttpUtils flushed and shut down."); // Keep INFO
        } else {
             log(Level.INFO, "HttpUtils was not initialized, skipping flush/shutdown."); // Keep INFO
        }

        // HTTP Client shutdown is now managed within HttpUtils shutdown if necessary.
        // Explicit shutdown here is usually not needed if HttpUtils owns the client lifecycle.

        log(Level.INFO, "StatsExporter has been disabled."); // Keep INFO
        instance = null;
    }

    // --- Getter methods ---

    public static StatsExporterPlugin getInstance() {
        return instance;
    }

    @Override // Override to ensure latest config is always returned
    public FileConfiguration getConfig() {
        // Ensure pluginConfig is initialized or reloaded if needed
        if (this.pluginConfig == null) {
             super.reloadConfig(); // Use super's reloadConfig which re-reads from disk
             this.pluginConfig = super.getConfig(); // Update internal reference AFTER reloading
        }
        return this.pluginConfig;
    }

    /**
     * Gets the current plugin configuration.
     * This is a convenience method that returns the same as getConfig()
     * but used for consistent naming across the plugin.
     * Use this method internally for clarity.
     *
     * @return The current plugin configuration
     */
    public FileConfiguration getPluginConfig() {
        return this.getConfig(); // Just call the overridden getConfig()
    }

    @Override // Override reloadConfig to update the internal reference
    public void reloadConfig() {
         super.reloadConfig();
         this.pluginConfig = super.getConfig(); // Update internal reference
         // Optionally re-initialize components that depend on config changes here
         // For HttpUtils, critical changes like URL/Key/Timeout/Retry might need a restart/re-initialization
         // But things like batch size, delays could potentially be updated.
         if (this.httpUtils != null) {
              log(Level.INFO, "Configuration reloaded. HttpUtils may need plugin restart for API URL/Key/Timeout/Retry changes.");
              // Consider adding an update method to HttpUtils if needed for non-critical settings
              // Example: httpUtils.updateConfig(this.pluginConfig);
         }
         if (this.statsFileReader != null) {
             // statsFileReader depends on world location, less likely to change config dynamically
         }
    }


    public OkHttpClient getHttpClient() {
        // Preferably get from HttpUtils if it manages the client lifecycle
        // Ensure HttpUtils is initialized before calling its getter
        return (httpUtils != null) ? httpUtils.getHttpClient() : null; // Now valid after HttpUtils fix, return null if httpUtils isn't ready
    }

    public HttpUtils getHttpUtils() {
        return httpUtils;
    }

    public StatsFileReader getStatsFileReader() {
        return statsFileReader;
    }

     // --- Task Management ---

     public void setBulkImportTask(StatsSyncTask runnable, BukkitTask task) {
        if (this.bulkImportBukkitTask != null && !this.bulkImportBukkitTask.isCancelled()) {
            this.bulkImportBukkitTask.cancel();
        }
        this.bulkImportRunnable = runnable;
        this.bulkImportBukkitTask = task;
    }

    public void clearBulkImportTaskReferences() {
         this.bulkImportRunnable = null;
         this.bulkImportBukkitTask = null;
         debug("Cleared bulk import task references."); // Use debug
     }

     public void cancelBulkImportTask() {
         if (isBulkImportRunning()) {
             log(Level.INFO, "Attempting to cancel bulk import task..."); // Keep INFO for user action
             if (bulkImportRunnable != null) {
                 try {
                     bulkImportRunnable.cancel(); // Calls internal cancel/cleanup
                 } catch (IllegalStateException e) {
                     debug("Bulk import task runnable already cancelled or finished."); // Use debug
                     if (bulkImportBukkitTask != null && !bulkImportBukkitTask.isCancelled()) {
                         bulkImportBukkitTask.cancel();
                     }
                     clearBulkImportTaskReferences();
                 }
             } else if (bulkImportBukkitTask != null && !bulkImportBukkitTask.isCancelled()) {
                 log(Level.WARNING, "Bulk import runnable reference was null, cancelling BukkitTask directly."); // Keep WARNING
                 bulkImportBukkitTask.cancel();
                 clearBulkImportTaskReferences();
             }
         } else {
             debug("No active bulk import task to cancel."); // Use debug
         }
     }

     public boolean isBulkImportRunning() {
         // Check the runnable's internal state first, then the BukkitTask
         // Added check for active running using Bukkit scheduler state
         return this.bulkImportRunnable != null
                && this.bulkImportRunnable.isTaskRunning()
                && this.bulkImportBukkitTask != null
                && !this.bulkImportBukkitTask.isCancelled()
                // Check if scheduler thinks the task is actively running or queued
                && (Bukkit.getScheduler().isCurrentlyRunning(this.bulkImportBukkitTask.getTaskId()) || Bukkit.getScheduler().isQueued(this.bulkImportBukkitTask.getTaskId())); // Modified check
     }

    // --- Logging Utilities (Refactored) ---

    /**
     * Logs a message at the specified level using the plugin's logger.
     * Use this for standard logging (INFO, WARNING, SEVERE).
     * @param level The severity level (e.g., Level.INFO).
     * @param message The message to log.
     */
    public void log(Level level, String message) {
        getLogger().log(level, message);
    }

    /**
     * Logs a message and throwable at the specified level using the plugin's logger.
     * Use this for standard logging with exceptions.
     * @param level The severity level.
     * @param message The message to log.
     * @param thrown The associated Throwable.
     */
     public void log(Level level, String message, Throwable thrown) {
         getLogger().log(level, message, thrown);
     }

    /**
     * Logs a detailed debug message *only* if 'debug' is enabled in config.yml.
     * Uses the FINE logging level internally.
     * @param message The debug message to log.
     */
    public void debug(String message) {
         // Check the config setting before logging
         FileConfiguration currentConfig = getPluginConfig(); // Use the getter
         if (currentConfig != null && currentConfig.getBoolean("debug", false)) {
             // Log using FINE level
             getLogger().log(Level.FINE, "[Debug] " + message);
         }
    }
}