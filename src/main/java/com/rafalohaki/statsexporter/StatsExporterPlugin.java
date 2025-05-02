package com.rafalohaki.statsexporter;

import com.rafalohaki.statsexporter.commands.StatsExporterCommand;
import com.rafalohaki.statsexporter.listeners.PlayerListener;
import com.rafalohaki.statsexporter.tasks.StatsSyncTask;
import com.rafalohaki.statsexporter.utils.HttpUtils;
import com.rafalohaki.statsexporter.utils.StatsFileReader;
import okhttp3.OkHttpClient;
// Unused import org.bukkit.Bukkit; removed
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
    private StatsSyncTask bulkImportRunnable;
    private BukkitTask bulkImportBukkitTask;


    @Override
    public void onEnable() {
        instance = this;
        getLogger().info("Loading configuration...");
        saveDefaultConfig();
        pluginConfig = getConfig();
        reloadConfig();
        pluginConfig = getConfig();

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

        getLogger().info("Registering event listeners...");
        getServer().getPluginManager().registerEvents(new PlayerListener(this), this);
        getLogger().info("Event listeners registered.");

        getLogger().info("Registering commands...");
        try {
            getCommand("statsexporter").setExecutor(new StatsExporterCommand(this));
        } catch (NullPointerException e) {
             getLogger().severe("Failed to register command 'statsexporter'. Is it defined correctly in plugin.yml?");
        }
        getLogger().info("Commands registered.");

        if (pluginConfig.getBoolean("sync.periodicOnlineSync", true)) {
            long intervalMinutes = pluginConfig.getLong("sync.periodicOnlineSyncIntervalMinutes", 20);
            if (intervalMinutes > 0) {
                long intervalTicks = intervalMinutes * 60 * 20;
                getLogger().info("Scheduling periodic online player sync every " + intervalMinutes + " minutes (" + intervalTicks + " ticks).");

                 try {
                    StatsSyncTask syncTaskRunnable = new StatsSyncTask(this);

                    // Use explicit Runnable to silence deprecation warning
                    Runnable syncTaskAsRunnable = syncTaskRunnable;
                    this.periodicSyncTask = getServer().getScheduler().runTaskTimerAsynchronously(
                            this,
                            syncTaskAsRunnable, // Pass the Runnable
                            1200L,              // Start after 1 minute
                            intervalTicks);     // Repeat every interval

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

        getLogger().info("Cancelling scheduled tasks...");
        if (periodicSyncTask != null && !periodicSyncTask.isCancelled()) {
            periodicSyncTask.cancel();
            getLogger().info("Cancelled periodic sync task.");
        }
        cancelBulkImportTask();

        if (httpUtils != null) {
            getLogger().info("Flushing remaining batched data and shutting down HttpUtils...");
            httpUtils.flushBatchSync();
            httpUtils.shutdown();
            getLogger().info("HttpUtils flushed and shut down.");
        } else {
             getLogger().info("HttpUtils was not initialized, skipping flush/shutdown.");
        }

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
         debug("Cleared bulk import task references.");
     }

     public void cancelBulkImportTask() {
         if (isBulkImportRunning()) {
             getLogger().info("Attempting to cancel bulk import task...");
             if (bulkImportRunnable != null) {
                 try {
                     bulkImportRunnable.cancel();
                 } catch (IllegalStateException e) {
                     debug("Bulk import task runnable already cancelled or finished.");
                     if (bulkImportBukkitTask != null && !bulkImportBukkitTask.isCancelled()) {
                         bulkImportBukkitTask.cancel();
                     }
                     clearBulkImportTaskReferences();
                 }
             } else if (bulkImportBukkitTask != null && !bulkImportBukkitTask.isCancelled()) {
                 getLogger().warning("Bulk import runnable reference was null, cancelling BukkitTask directly.");
                 bulkImportBukkitTask.cancel();
                 clearBulkImportTaskReferences();
             }
         } else {
             debug("No active bulk import task to cancel.");
         }
     }

     public boolean isBulkImportRunning() {
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
         log(Level.INFO, message);
    }
}