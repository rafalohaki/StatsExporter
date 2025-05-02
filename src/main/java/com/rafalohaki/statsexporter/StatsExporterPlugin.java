package com.rafalohaki.statsexporter;

import com.rafalohaki.statsexporter.commands.StatsExporterCommand;
import com.rafalohaki.statsexporter.listeners.PlayerListener;
import com.rafalohaki.statsexporter.tasks.StatsSyncTask; // Import the task
import com.rafalohaki.statsexporter.utils.HttpUtils;
import com.rafalohaki.statsexporter.utils.StatsFileReader;
import okhttp3.OkHttpClient;
import org.bukkit.Bukkit;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitTask;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public final class StatsExporterPlugin extends JavaPlugin {

    private static StatsExporterPlugin instance;
    private FileConfiguration pluginConfig;
    private OkHttpClient httpClient;
    private HttpUtils httpUtils;
    private StatsFileReader statsFileReader;
    private BukkitTask periodicSyncTask;
    // Reference to the bulk import task runnable for status checking
    private StatsSyncTask bulkImportRunnable; // Use the task class type
    private BukkitTask bulkImportBukkitTask; // Keep BukkitTask ref for cancellation


    @Override
    public void onEnable() {
        instance = this;
        // 1. Load Configuration
        getLogger().info("Loading configuration...");
        saveDefaultConfig();
        pluginConfig = getConfig();
        reloadConfig(); // Ensure latest config is loaded after potential creation
        pluginConfig = getConfig();

        // 2. Validate Configuration
        getLogger().info("Validating configuration...");
        String apiUrl = pluginConfig.getString("api.url", "");
        String apiKey = pluginConfig.getString("api.key", "");

        if (apiUrl == null || apiUrl.isEmpty() || apiUrl.isBlank()) {
            getLogger().severe("API URL is not configured in config.yml! Disabling plugin.");
            getServer().getPluginManager().disablePlugin(this);
            return;
        }
        if (apiKey == null || apiKey.isEmpty() || apiKey.isBlank() || apiKey.equals("YOUR_SECRET_API_KEY_HERE")) {
             getLogger().severe("API Key is not configured or is set to the default placeholder in config.yml! Disabling plugin.");
             getServer().getPluginManager().disablePlugin(this);
             return;
        }
        getLogger().info("Configuration loaded and validated successfully.");

        // 3. Initialize Components
        getLogger().info("Initializing components...");
        long timeout = pluginConfig.getLong("api.timeoutSeconds", 10);
        this.httpClient = new OkHttpClient.Builder()
                .connectTimeout(timeout, TimeUnit.SECONDS)
                .readTimeout(timeout, TimeUnit.SECONDS)
                .writeTimeout(timeout, TimeUnit.SECONDS)
                .build();

        this.httpUtils = new HttpUtils(this);
        this.statsFileReader = new StatsFileReader(this);

        getLogger().info("Components initialized.");

        // 4. Register Event Listeners
        getLogger().info("Registering event listeners...");
        getServer().getPluginManager().registerEvents(new PlayerListener(this), this);
        getLogger().info("Event listeners registered.");

        // 5. Register Commands
        getLogger().info("Registering commands...");
        try {
            getCommand("statsexporter").setExecutor(new StatsExporterCommand(this));
        } catch (NullPointerException e) {
             getLogger().severe("Failed to register command 'statsexporter'. Is it defined correctly in plugin.yml?");
        }
        getLogger().info("Commands registered.");

        // 6. Schedule Repeating Tasks (Periodic Sync) - Corrected
        if (pluginConfig.getBoolean("sync.periodicOnlineSync", true)) {
            long intervalMinutes = pluginConfig.getLong("sync.periodicOnlineSyncIntervalMinutes", 20);
            if (intervalMinutes > 0) {
                long intervalTicks = intervalMinutes * 60 * 20;
                getLogger().info("Scheduling periodic online player sync every " + intervalMinutes + " minutes (" + intervalTicks + " ticks).");

                 try {
                    // Create the task instance for periodic sync
                    StatsSyncTask syncTaskRunnable = new StatsSyncTask(this); // Use periodic constructor
                    // Schedule the task to run asynchronously
                    this.periodicSyncTask = getServer().getScheduler().runTaskTimerAsynchronously(
                            this,
                            syncTaskRunnable,
                            1200L, // Start after 1 minute
                            intervalTicks); // Repeat every interval
                 } catch (Exception e) {
                    getLogger().log(Level.SEVERE, "Failed to schedule periodic sync task", e);
                 }

            } else {
                 getLogger().info("Periodic online player sync interval is <= 0, task disabled.");
            }
        } else {
            getLogger().info("Periodic online player sync is disabled in config.");
        }


        getLogger().info("StatsExporter has been enabled successfully!");
    }

    @Override
    public void onDisable() {
        getLogger().info("Disabling StatsExporter...");

        // 1. Cancel scheduled Bukkit tasks
        getLogger().info("Cancelling scheduled tasks...");
        if (periodicSyncTask != null && !periodicSyncTask.isCancelled()) {
            periodicSyncTask.cancel();
            getLogger().info("Cancelled periodic sync task.");
        }
        // Use the dedicated cancel method for bulk import
        cancelBulkImportTask(); // This logs its own message if it cancels anything

        // 2. Process remaining data and shutdown HttpUtils
        if (httpUtils != null) {
            getLogger().info("Flushing remaining batched data and shutting down HttpUtils...");
            httpUtils.flushBatchSync();
            httpUtils.shutdown();
            getLogger().info("HttpUtils flushed and shut down.");
        } else {
             getLogger().info("HttpUtils was not initialized, skipping flush/shutdown.");
        }


        // 3. Clean up OkHttp Client resources
        if (httpClient != null) {
             getLogger().info("Shutting down HTTP client...");
            httpClient.dispatcher().executorService().shutdown();
            try {
                if (!httpClient.dispatcher().executorService().awaitTermination(5, TimeUnit.SECONDS)) {
                    getLogger().warning("HTTP client dispatcher did not shut down cleanly within 5 seconds. Forcing shutdown...");
                    httpClient.dispatcher().executorService().shutdownNow();
                } else {
                     getLogger().info("HTTP client dispatcher shut down.");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                getLogger().warning("Interrupted while shutting down HTTP client dispatcher. Forcing shutdown.");
                httpClient.dispatcher().executorService().shutdownNow();
            }
        } else {
             getLogger().info("HTTP Client was not initialized, skipping shutdown.");
        }

        getLogger().info("StatsExporter has been disabled.");
        instance = null;
    }

    // --- Getter methods ---

    public static StatsExporterPlugin getInstance() {
        return instance;
    }

    public FileConfiguration getPluginConfig() {
        return pluginConfig;
    }

    public OkHttpClient getHttpClient() {
        return httpClient;
    }

    public HttpUtils getHttpUtils() {
        return httpUtils;
    }

    public StatsFileReader getStatsFileReader() {
        return statsFileReader;
    }

     // --- Task Management ---

     // Method for the command to set the running bulk import task
     public void setBulkImportTask(StatsSyncTask runnable, BukkitTask task) {
        if (this.bulkImportBukkitTask != null && !this.bulkImportBukkitTask.isCancelled()) {
            this.bulkImportBukkitTask.cancel();
        }
        this.bulkImportRunnable = runnable;
        this.bulkImportBukkitTask = task;
    }

    // Clear references when bulk import finishes or is cancelled
    public void clearBulkImportTaskReferences() {
         this.bulkImportRunnable = null;
         this.bulkImportBukkitTask = null;
         debug("Cleared bulk import task references.");
     }

     // Cancel the bulk import task if it's running
     public void cancelBulkImportTask() {
         if (isBulkImportRunning()) { // Use the status check method
             getLogger().info("Attempting to cancel bulk import task...");
             if (bulkImportRunnable != null) {
                 try {
                     bulkImportRunnable.cancel(); // This should trigger cleanup in the task
                     // References are cleared within the task's cleanup logic
                 } catch (IllegalStateException e) {
                     debug("Bulk import task runnable already cancelled or finished.");
                     // Ensure refs are cleared if cancel throws exception
                     if (bulkImportBukkitTask != null && !bulkImportBukkitTask.isCancelled()) {
                         bulkImportBukkitTask.cancel();
                     }
                     clearBulkImportTaskReferences();
                 }
             } else if (bulkImportBukkitTask != null && !bulkImportBukkitTask.isCancelled()) {
                 // Fallback if runnable ref is somehow null but BukkitTask exists
                 getLogger().warning("Bulk import runnable reference was null, cancelling BukkitTask directly.");
                 bulkImportBukkitTask.cancel();
                 clearBulkImportTaskReferences(); // Clear refs here since task cleanup won't run
             }
         } else {
             debug("No active bulk import task to cancel.");
         }
     }


     // Check if bulk import is running using the flag in the task runnable
     public boolean isBulkImportRunning() {
         // Check both the runnable flag and ensure the BukkitTask itself hasn't been cancelled
         return this.bulkImportRunnable != null
                && this.bulkImportRunnable.isTaskRunning()
                && this.bulkImportBukkitTask != null
                && !this.bulkImportBukkitTask.isCancelled();
     }

    // --- Logging Utilities ---

    public void log(Level level, String message) {
        if (level == Level.WARNING || level == Level.SEVERE) {
             getLogger().log(level, message);
        } else {
            if (pluginConfig != null && pluginConfig.getBoolean("debug", false)) {
                 getLogger().log(Level.INFO,"[Debug] " + message);
            } else if (level == Level.INFO) {
                 getLogger().log(level, message);
            }
        }
    }

     public void log(Level level, String message, Throwable thrown) {
         if (level == Level.SEVERE || level == Level.WARNING || (pluginConfig != null && pluginConfig.getBoolean("debug", false))) {
             getLogger().log(level, message, thrown);
         } else {
             log(level, message);
         }
     }

    public void debug(String message) {
         log(Level.INFO, message); // Uses the main log method
    }
}