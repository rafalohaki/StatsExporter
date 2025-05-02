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
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public class StatsSyncTask extends BukkitRunnable {

    private final StatsExporterPlugin plugin;
    private final HttpUtils httpUtils;
    private final StatsFileReader statsFileReader;
    private final Executor bukkitExecutor; // Bukkit's async executor

    // Task Mode specifics
    private final boolean isBulkImport;
    private Iterator<UUID> bulkImportIterator; // Iterator for UUIDs in bulk mode
    private final int playersPerTick;          // How many players to process per tick in bulk mode
    private final int totalToProcess;          // Total UUIDs in the bulk import list
    private final AtomicInteger processedCount = new AtomicInteger(0); // Counter for processed players in bulk mode
    private Instant importStartTime;           // Start time for bulk import duration calculation
    private final AtomicBoolean isRunning = new AtomicBoolean(false); // Tracks if the task *logic* is actively running (especially for bulk import)


    // Constructor for Periodic Sync mode (online players)
    public StatsSyncTask(StatsExporterPlugin plugin) {
        this.plugin = plugin;
        this.httpUtils = plugin.getHttpUtils();
        this.statsFileReader = plugin.getStatsFileReader();
        this.isBulkImport = false;
        this.playersPerTick = 0; // Not used in periodic mode
        this.totalToProcess = 0; // Not used in periodic mode
        this.bulkImportIterator = null; // Not used in periodic mode
        this.bukkitExecutor = runnable -> Bukkit.getScheduler().runTaskAsynchronously(plugin, runnable);
        // isRunning flag is primarily for bulk import state, not strictly needed for periodic
    }

    // Constructor for Bulk Import mode (all player files)
    public StatsSyncTask(StatsExporterPlugin plugin, List<UUID> uuidsToImport) {
        this.plugin = plugin;
        this.httpUtils = plugin.getHttpUtils();
        this.statsFileReader = plugin.getStatsFileReader();
        this.isBulkImport = true;
        // Read config values within the constructor or pass them if needed
        this.playersPerTick = Math.max(1, plugin.getPluginConfig().getInt("import.playersPerTick", 2)); // Ensure at least 1
        this.totalToProcess = uuidsToImport.size();
        this.bulkImportIterator = uuidsToImport.iterator();
        this.importStartTime = Instant.now();
        this.bukkitExecutor = runnable -> Bukkit.getScheduler().runTaskAsynchronously(plugin, runnable);
        // isRunning starts as false, set to true when run() first executes in bulk mode
    }


    @Override
    public void run() {
        // --- Robust Cancellation Check at Start ---
        // Check Bukkit's cancellation status first. This is the most likely place
        // for IllegalStateException if the task was cancelled externally between ticks.
        try {
            if (super.isCancelled()) {
                plugin.debug("Task run() called but Bukkit reports already cancelled.");
                // If it was a bulk import that was running, ensure cleanup happens
                if (isBulkImport && isRunning.get()) {
                    cleanupBulkImport(false); // Mark as not finished naturally
                }
                return; // Do not proceed
            }
        } catch (IllegalStateException e) {
            // This catch block handles the specific error reported by the user.
            // It means the scheduler is executing this 'run' even though it
            // considers the task finished or cancelled internally.
            plugin.debug("IllegalStateException checking cancellation status at start of run(), task likely already unscheduled/finished.");
            // Ensure cleanup runs if it was a bulk import that got interrupted mid-process
             if (isBulkImport && isRunning.get()) {
                 cleanupBulkImport(false); // Mark as not finished naturally
             }
            return; // Stop execution as the task state is invalid
        }
        // --- End Robust Cancellation Check ---

        // --- Execute Task Logic based on Mode ---
        if (isBulkImport) {
            // Set running flag only for bulk import mode when it first starts
            if (!isRunning.get() && bulkImportIterator != null && bulkImportIterator.hasNext()) {
                isRunning.set(true);
                plugin.debug("Bulk import task thread started execution.");
            }

            // Perform one tick's worth of bulk import work
            runBulkImportTick(); // This method might call cleanupBulkImport if it finishes

            // --- Explicit Bukkit Cancellation on Completion ---
            // After the tick runs, check if the *logic* is now stopped (isRunning is false).
            // If it is, explicitly tell the Bukkit scheduler to cancel this repeating task.
            if (!isRunning.get() && bulkImportIterator != null /* Check if it was initialized */) {
                plugin.debug("Bulk import finished or was stopped, cancelling Bukkit task from run() method.");
                try {
                    this.cancel(); // Tell Bukkit scheduler THIS task is done and should not run again.
                } catch (IllegalStateException e) {
                    // This might happen if the task was cancelled externally *exactly* between
                    // runBulkImportTick finishing and this check. It's safe to ignore.
                    plugin.debug("Attempted to self-cancel task from run(), but it was already cancelled/finished.");
                }
            }
            // --- End Explicit Bukkit Cancellation ---

        } else { // Periodic Sync Mode
            runPeriodicSync();
            // Periodic tasks don't typically self-cancel; they run until the plugin disables
            // or they are explicitly cancelled externally.
        }
    }

    // Override BukkitRunnable's cancel method to ensure internal state is cleaned up
    // This is called by external cancellations (e.g., command, plugin disable)
    // or by the self-cancellation in the run() method.
    @Override
    public synchronized void cancel() throws IllegalStateException {
        plugin.debug("StatsSyncTask cancel() method invoked.");
        // First, try to cancel via Bukkit's scheduler. This might throw if already cancelled.
        try {
            super.cancel();
        } catch (IllegalStateException e) {
            plugin.debug("IllegalStateException calling super.cancel(), task likely already cancelled/finished. Proceeding with internal cleanup.");
        } finally {
            // Regardless of Bukkit's state, ensure our internal state is cleaned up,
            // especially for bulk imports.
            if (isBulkImport) {
                // Call cleanup, ensuring it only runs once via compareAndSet
                cleanupBulkImport(false); // Assume external cancel means not finished normally
            }
        }
    }


    // --- Bulk Import Logic for a Single Tick ---
    private void runBulkImportTick() {
        // Check if the iterator is valid and has more elements
        if (bulkImportIterator == null || !bulkImportIterator.hasNext()) {
            plugin.debug("Bulk import iterator is null or empty, initiating cleanup.");
            cleanupBulkImport(true); // Mark as finished normally
            // run() method will detect !isRunning and cancel the Bukkit task
            return;
        }

        // --- Check for external cancellation before processing this tick's batch ---
        try {
            if (super.isCancelled()) {
                 plugin.debug("Bulk import tick detected external cancellation, initiating cleanup.");
                 cleanupBulkImport(false);
                 return; // Stop processing this tick
            }
        } catch (IllegalStateException e) {
             plugin.debug("IllegalStateException checking cancellation status in runBulkImportTick(), task likely already cancelled/finished.");
             cleanupBulkImport(false); // Assume cancelled if state is invalid
             return;
        }
        // --- End Cancellation Check ---

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        int processedThisTick = 0;
        boolean checkCancelMidTick = false; // Flag if cancelled during the loop

        for (int i = 0; i < playersPerTick && bulkImportIterator.hasNext(); i++) {
             // --- Add check inside the loop for responsiveness ---
             try {
                if (super.isCancelled()) {
                    checkCancelMidTick = true; // Mark that we should check cancellation status again
                    break; // Stop processing this tick's batch
                }
             } catch (IllegalStateException e) {
                 plugin.debug("IllegalStateException checking cancellation status mid-bulk-import-loop.");
                 checkCancelMidTick = true;
                 break;
             }
             // --- End of added check ---

            UUID uuid = bulkImportIterator.next();
            OfflinePlayer offlinePlayer = Bukkit.getOfflinePlayer(uuid);

            CompletableFuture<Void> future = statsFileReader.getPlayerDataMapAsync(offlinePlayer)
                .thenAcceptAsync(playerDataMap -> { // Process the result asynchronously
                    if (playerDataMap != null && !playerDataMap.isEmpty()) {
                        httpUtils.queuePlayerData(playerDataMap);
                    }
                    // Increment count and log progress periodically
                    int currentCount = processedCount.incrementAndGet();
                    if (currentCount % 100 == 0 || currentCount == totalToProcess) {
                        // Avoid spamming console, log every 100 or on the last one
                        plugin.log(Level.INFO, "Bulk import progress: " + currentCount + "/" + totalToProcess);
                    }
                }, bukkitExecutor) // Ensure this runs on Bukkit's async pool
                 .exceptionally(ex -> { // Handle errors for individual player processing
                    plugin.log(Level.WARNING, "Error processing stats during bulk import for UUID " + uuid + ": " + ex.getMessage()); // Log warning, not full stack usually
                    processedCount.incrementAndGet(); // Still count as processed *attempt* for progress
                    return null; // Required for exceptionally stage
                 });
            futures.add(future);
            processedThisTick++;
        }

        plugin.debug("Processed " + processedThisTick + " players this tick.");

        // If cancelled during the loop, trigger cleanup immediately and exit tick
        if (checkCancelMidTick) {
             plugin.debug("Cancellation detected mid-tick, initiating cleanup.");
             cleanupBulkImport(false);
             return;
        }

        // Check if the iterator is now empty AFTER processing the loop's batch
        if (!bulkImportIterator.hasNext()) {
             plugin.debug("Bulk import iterator finished. Waiting for last batch futures.");
             CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).whenComplete((unused, throwable) -> {
                // This ensures the cleanup happens after the last batch is processed/queued
                 boolean stillNotCancelled;
                 try {
                     stillNotCancelled = !super.isCancelled();
                 } catch (IllegalStateException e) {
                     stillNotCancelled = false;
                 }

                 if(stillNotCancelled) {
                    plugin.debug("Last batch futures complete. Initiating final cleanup.");
                    cleanupBulkImport(true); // Mark as finished naturally
                    // The run() method will then detect !isRunning and self-cancel the BukkitTask.
                 } else {
                    plugin.debug("Task was cancelled while waiting for last batch futures. Cleanup likely already run.");
                    // Cleanup might have already been triggered by the cancel() override or run() check
                     if (isRunning.get()) { // Double-check if cleanup is needed
                        cleanupBulkImport(false);
                     }
                 }
             });
        }
    }

    // --- Periodic Sync Logic (Online Players) ---
    private void runPeriodicSync() {
        // --- Check if cancelled before proceeding ---
        try {
            if (super.isCancelled()) {
                 plugin.debug("Periodic sync cancelled before starting.");
                 return;
             }
        } catch (IllegalStateException e) {
             plugin.debug("IllegalStateException checking cancellation status in runPeriodicSync(), task likely already cancelled/finished.");
             return; // Stop if state is invalid
        }
        // --- End Cancellation Check ---

        plugin.debug("Running periodic online player sync...");
        Collection<? extends Player> onlinePlayers = Bukkit.getOnlinePlayers();

        if (onlinePlayers.isEmpty()) {
            plugin.debug("No players online for periodic sync.");
            return;
        }

        AtomicInteger syncCount = new AtomicInteger(0);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        boolean checkCancelMidLoop = false;

        for (Player player : onlinePlayers) {
             // --- Added check for cancellation inside loop ---
             try {
                if (super.isCancelled()) {
                    checkCancelMidLoop = true;
                    break; // Stop processing more players if task was cancelled
                }
             } catch (IllegalStateException e) {
                 plugin.debug("IllegalStateException checking cancellation status mid-periodic-sync-loop. Breaking loop.");
                 checkCancelMidLoop = true;
                 break; // Stop processing more players if task state is weird
             }
             // --- End Cancellation Check ---

             plugin.debug("Queueing periodic sync for online player: " + player.getName());
             CompletableFuture<Void> future = statsFileReader.getPlayerDataMapAsync(player)
                .thenAcceptAsync(playerDataMap -> {
                    if (playerDataMap != null && !playerDataMap.isEmpty()) {
                        httpUtils.queuePlayerData(playerDataMap);
                        syncCount.incrementAndGet();
                    }
                }, bukkitExecutor) // Run on Bukkit's async pool
                 .exceptionally(ex -> { // Handle individual player errors
                     plugin.log(Level.WARNING, "Error processing periodic sync stats for player " + player.getName() + ": " + ex.getMessage());
                     return null; // Allow Promise.all to complete
                 });
             futures.add(future);
        }

         // If cancelled mid-loop, don't wait for futures or log completion
         if (checkCancelMidLoop) {
             plugin.debug("Periodic sync cancelled mid-loop. Not waiting for futures.");
             return;
         }

         // Wait for all futures and log completion status
         CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).whenComplete((unused, throwable) -> {
             // Check cancellation status *again* before logging completion message
             boolean stillNotCancelled;
             try {
                 stillNotCancelled = !super.isCancelled();
             } catch (IllegalStateException e) {
                 stillNotCancelled = false; // Assume cancelled if state is invalid
             }

             if (stillNotCancelled) {
                 if (throwable == null) {
                     plugin.debug("Periodic sync cycle completed. Successfully queued data for " + syncCount.get() + " online players.");
                 } else {
                     // Log general error if CompletableFuture.allOf failed (unlikely with exceptionally blocks)
                     plugin.log(Level.WARNING, "Periodic sync cycle finished, but some errors occurred during processing.", throwable);
                 }
             } else {
                  plugin.debug("Periodic sync task was cancelled while waiting for player futures.");
             }
         });
    }

    // --- Centralized Cleanup Logic for Bulk Import Task ---
    // This method now focuses *only* on internal state cleanup and logging.
    // It sets isRunning to false, which signals the run() method to cancel the Bukkit task.
    private void cleanupBulkImport(boolean finishedNaturally) {
         // Use compareAndSet to ensure this cleanup logic runs only once per task instance
         if (isRunning.compareAndSet(true, false)) {
             plugin.debug("CleanupBulkImport running (finishedNaturally=" + finishedNaturally + ")");
             if (finishedNaturally) {
                 Instant endTime = Instant.now();
                 // Ensure importStartTime was initialized
                 if (importStartTime != null) {
                    Duration duration = Duration.between(importStartTime, endTime);
                    plugin.log(Level.INFO, "Bulk import finished processing " + processedCount.get() + "/" + totalToProcess + " players in " + formatDuration(duration) + ".");
                 } else {
                     plugin.log(Level.INFO, "Bulk import finished processing " + processedCount.get() + "/" + totalToProcess + " players. (Start time unavailable)");
                 }
             } else {
                 plugin.log(Level.INFO, "Bulk import task stopped or cancelled after processing " + processedCount.get() + "/" + totalToProcess + " players.");
             }
             // Clear the references in the main plugin class to allow a new import task
             plugin.clearBulkImportTaskReferences();

             // --- REMOVED REDUNDANT/PROBLEMATIC SELF-CANCEL ---
             // The run() method is now responsible for cancelling the BukkitTask
             // after it detects that isRunning has become false.
         } else {
             plugin.debug("CleanupBulkImport called but task was already marked as not running.");
         }
    }


    // --- Public method for commands/plugin to check logical status ---
    // Note: This reflects the internal 'isRunning' flag, not Bukkit's scheduler status directly.
    public boolean isTaskRunning() {
        return isRunning.get();
    }


    // --- Utility to format Duration ---
    private String formatDuration(Duration duration) {
        if (duration == null) return "N/A";
        long seconds = duration.getSeconds();
        long absSeconds = Math.abs(seconds);
        String positive = String.format(
                "%dh %02dm %02ds",
                absSeconds / 3600,
                (absSeconds % 3600) / 60,
                absSeconds % 60);
        // Handle potential negative duration if clock adjustments occur, though unlikely here
        return seconds < 0 ? "-" + positive : positive;
    }
}