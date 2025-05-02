package com.rafalohaki.statsexporter.tasks;

import com.rafalohaki.statsexporter.StatsExporterPlugin;
import com.rafalohaki.statsexporter.utils.HttpUtils;
import com.rafalohaki.statsexporter.utils.StatsFileReader;
import org.bukkit.Bukkit;
import org.bukkit.OfflinePlayer;
import org.bukkit.entity.Player;
import org.bukkit.scheduler.BukkitRunnable;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor; // Import Executor
import java.util.concurrent.atomic.AtomicBoolean; // Import AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public class StatsSyncTask extends BukkitRunnable {

    private final StatsExporterPlugin plugin;
    private final HttpUtils httpUtils;
    private final StatsFileReader statsFileReader;
    private final Executor bukkitExecutor;

    // Task Mode specifics
    private final boolean isBulkImport;
    private Iterator<UUID> bulkImportIterator;
    private final int playersPerTick;
    private final int totalToProcess;
    private final AtomicInteger processedCount = new AtomicInteger(0);
    private Instant importStartTime;
    private final AtomicBoolean isRunning = new AtomicBoolean(false); // Flag for task status


    // Constructor for Periodic Sync mode
    public StatsSyncTask(StatsExporterPlugin plugin) {
        this.plugin = plugin;
        this.httpUtils = plugin.getHttpUtils();
        this.statsFileReader = plugin.getStatsFileReader();
        this.isBulkImport = false;
        this.playersPerTick = 0;
        this.totalToProcess = 0;
        this.bulkImportIterator = null;
        this.bukkitExecutor = runnable -> Bukkit.getScheduler().runTaskAsynchronously(plugin, runnable);
    }

    // Constructor for Bulk Import mode
    public StatsSyncTask(StatsExporterPlugin plugin, List<UUID> uuidsToImport) {
        this.plugin = plugin;
        this.httpUtils = plugin.getHttpUtils();
        this.statsFileReader = plugin.getStatsFileReader();
        this.isBulkImport = true;
        this.playersPerTick = plugin.getPluginConfig().getInt("import.playersPerTick", 2);
        this.totalToProcess = uuidsToImport.size();
        this.bulkImportIterator = uuidsToImport.iterator();
        this.importStartTime = Instant.now();
        this.bukkitExecutor = runnable -> Bukkit.getScheduler().runTaskAsynchronously(plugin, runnable);
    }


    @Override
    public void run() {
        // Prevent running if cancelled externally, especially for bulk import
        if (isCancelled()) {
            if (isBulkImport) {
                 cleanupBulkImport(false); // Mark as not naturally finished
            }
            return;
        }

        if (isBulkImport) {
            // Set running flag only for bulk import mode
            if (!isRunning.get()) {
                isRunning.set(true);
                plugin.debug("Bulk import task thread started execution.");
            }
            runBulkImportTick();
        } else {
            runPeriodicSync();
        }
    }

    // Override cancel method to ensure state is cleaned up
    @Override
    public synchronized void cancel() throws IllegalStateException {
        super.cancel();
        if (isBulkImport) {
            cleanupBulkImport(false); // Assume cancellation means not finished normally
        }
    }


    private void runBulkImportTick() {
        if (bulkImportIterator == null || !bulkImportIterator.hasNext()) {
            cleanupBulkImport(true); // Mark as finished normally
            return;
        }

        // Check again for external cancellation before processing batch
        if (isCancelled()) {
             cleanupBulkImport(false);
             return;
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < playersPerTick && bulkImportIterator.hasNext(); i++) {
            UUID uuid = bulkImportIterator.next();
            OfflinePlayer offlinePlayer = Bukkit.getOfflinePlayer(uuid);

            CompletableFuture<Void> future = statsFileReader.getPlayerDataMapAsync(offlinePlayer)
                .thenAcceptAsync(playerDataMap -> {
                    if (playerDataMap != null && !playerDataMap.isEmpty()) {
                        httpUtils.queuePlayerData(playerDataMap);
                    }
                    int currentCount = processedCount.incrementAndGet();
                    if (currentCount % 100 == 0 || currentCount == totalToProcess) {
                        plugin.log(Level.INFO, "Bulk import progress: " + currentCount + "/" + totalToProcess);
                    }
                }, bukkitExecutor)
                 .exceptionally(ex -> {
                    plugin.log(Level.WARNING, "Error processing stats during bulk import for UUID " + uuid + ": " + ex.getMessage()); // Reduced severity, don't print stack usually
                    processedCount.incrementAndGet(); // Still count as processed
                    return null;
                 });
            futures.add(future);
        }
    }

    private void runPeriodicSync() {
        // Check if cancelled before proceeding
         if (isCancelled()) {
             return;
         }

        plugin.debug("Running periodic online player sync...");
        Collection<? extends Player> onlinePlayers = Bukkit.getOnlinePlayers();

        if (onlinePlayers.isEmpty()) {
            plugin.debug("No players online for periodic sync.");
            return;
        }

        AtomicInteger syncCount = new AtomicInteger(0);
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (Player player : onlinePlayers) {
             // Added check for cancellation inside loop for long-running syncs
             if (isCancelled()) break;

            plugin.debug("Queueing periodic sync for online player: " + player.getName());
             CompletableFuture<Void> future = statsFileReader.getPlayerDataMapAsync(player)
                .thenAcceptAsync(playerDataMap -> {
                    if (playerDataMap != null && !playerDataMap.isEmpty()) {
                        httpUtils.queuePlayerData(playerDataMap);
                        syncCount.incrementAndGet();
                    }
                }, bukkitExecutor)
                 .exceptionally(ex -> {
                     plugin.log(Level.WARNING, "Error processing periodic sync stats for player " + player.getName() + ": " + ex.getMessage());
                     return null;
                 });
             futures.add(future);
        }

         // Only log completion if not cancelled prematurely
         if (!isCancelled()) {
             CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).whenComplete((unused, throwable) -> {
                 // Check cancellation again before logging completion message
                 if (!isCancelled()) {
                     if (throwable == null) {
                         plugin.debug("Periodic sync cycle completed. Successfully queued data for " + syncCount.get() + " online players.");
                     } else {
                         plugin.log(Level.WARNING, "Periodic sync cycle finished with errors.", throwable);
                     }
                 }
             });
         }
    }

    // Centralized cleanup logic for bulk import task
    private void cleanupBulkImport(boolean finishedNaturally) {
         if (isRunning.compareAndSet(true, false)) { // Ensure cleanup runs only once
             if (finishedNaturally) {
                 Instant endTime = Instant.now();
                 Duration duration = Duration.between(importStartTime, endTime);
                 plugin.log(Level.INFO, "Bulk import finished processing " + processedCount.get() + "/" + totalToProcess + " players in " + formatDuration(duration) + ".");
             } else {
                 plugin.log(Level.INFO, "Bulk import task stopped or cancelled after processing " + processedCount.get() + "/" + totalToProcess + " players.");
             }
             plugin.clearBulkImportTaskReferences(); // Clear references in main plugin
             // Cancel BukkitRunnable explicitly if not already cancelled
             if (!isCancelled()) {
                 try {
                     cancel();
                 } catch (IllegalStateException e) { /* ignore if already cancelled */ }
             }
         }
    }


    // Public method for command/plugin to check status
    public boolean isTaskRunning() {
        return isRunning.get();
    }


    private String formatDuration(Duration duration) {
        long seconds = duration.getSeconds();
        long absSeconds = Math.abs(seconds);
        String positive = String.format(
                "%dh %02dm %02ds",
                absSeconds / 3600,
                (absSeconds % 3600) / 60,
                absSeconds % 60);
        return seconds < 0 ? "-" + positive : positive;
    }
}