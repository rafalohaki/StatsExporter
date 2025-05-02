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
        // Check Bukkit's cancellation status first using our helper method.
        if (isBukkitTaskCancelled()) {
            plugin.debug("Task run() called but Bukkit reports already cancelled (checked via helper).");
            // If it was a bulk import that was running, ensure cleanup happens
            if (isBulkImport && isRunning.get()) {
                cleanupBulkImport(false); // Mark as not finished naturally
            }
            return; // Do not proceed
        }
        // --- End Robust Cancellation Check ---

        // --- Execute Task Logic based on Mode ---
        if (isBulkImport) {
            // Set running flag only for bulk import mode when it first starts
            // and ensure the iterator is valid
            if (!isRunning.get() && bulkImportIterator != null && bulkImportIterator.hasNext()) {
                isRunning.set(true);
                plugin.debug("Bulk import task thread started execution.");
            }

            // Perform one tick's worth of bulk import work using the refactored method
            runBulkImportTick(); // This method might trigger cleanup if it finishes/cancels

            // --- Explicit Bukkit Cancellation on Completion ---
            // After the tick runs, check if the *logic* is now stopped (isRunning is false).
            // If it is, explicitly tell the Bukkit scheduler to cancel this repeating task.
            // Also check if the iterator was initialized to avoid issues if it was never started.
            if (!isRunning.get() && bulkImportIterator != null) {
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
            // No specific cleanup needed for periodic sync beyond stopping the task
        }
    }

    // --- Refactored Bulk Import Logic ---

    /**
     * Executes one tick of the bulk import process. Coordinates checking state,
     * processing a batch, and handling completion or cancellation.
     * Cognitive Complexity is significantly reduced.
     */
    private void runBulkImportTick() {
        // 1. Initial State Check: Is the iterator valid and has elements?
        if (bulkImportIterator == null || !bulkImportIterator.hasNext()) {
            plugin.debug("Bulk import iterator is null or empty at start of tick, initiating cleanup.");
            cleanupBulkImport(true); // Mark as finished (or already finished)
            // run() will see !isRunning and cancel the Bukkit task.
            return;
        }

        // 2. Pre-Batch Cancellation Check (using helper)
        if (isBukkitTaskCancelled()) {
             plugin.debug("Bulk import tick detected cancellation before processing batch, initiating cleanup.");
             cleanupBulkImport(false); // Cancelled
             // run() will see !isRunning and cancel the Bukkit task.
             return;
        }

        // 3. Process the Batch
        List<CompletableFuture<Void>> batchFutures = processPlayerBatch();

        // 4. Post-Batch Cancellation Check (if processPlayerBatch detected cancellation)
        // processPlayerBatch returns null if cancelled mid-batch
        if (batchFutures == null) {
             plugin.debug("Batch processing indicated cancellation, initiating cleanup.");
             cleanupBulkImport(false); // Cancelled during batch
             // run() will see !isRunning and cancel the Bukkit task.
             return;
        }

        // 5. Check if Import is Complete (Iterator finished?)
        if (!bulkImportIterator.hasNext()) {
             handleFinalBatchCompletion(batchFutures);
             // Note: cleanupBulkImport is called asynchronously within handleFinalBatchCompletion.
             // The run() method will eventually detect !isRunning and cancel the Bukkit task.
        }
        // If the iterator still has elements, the task continues to the next tick automatically.
    }

    /**
     * Processes a batch of players from the iterator for the current tick.
     * Handles mid-tick cancellation checks using the helper method.
     *
     * @return A list of CompletableFuture representing the processing of each player in the batch,
     *         or null if the task was cancelled mid-batch.
     */
    private List<CompletableFuture<Void>> processPlayerBatch() {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        int processedThisTick = 0;

        for (int i = 0; i < playersPerTick && bulkImportIterator.hasNext(); i++) {
             // Check for cancellation *inside* the loop for better responsiveness
             if (isBukkitTaskCancelled()) {
                 plugin.debug("Cancellation detected mid-batch processing loop.");
                 return null; // Signal cancellation happened during the batch
             }

            UUID uuid = bulkImportIterator.next();
            futures.add(processSinglePlayerStats(uuid));
            processedThisTick++;
        }

        plugin.debug("Scheduled processing for " + processedThisTick + " players this tick.");
        return futures;
    }

    /**
     * Asynchronously processes the statistics for a single player UUID.
     * Reads stats, queues data, updates progress, and handles errors.
     *
     * @param uuid The UUID of the player to process.
     * @return A CompletableFuture representing the completion of this player's processing.
     */
    private CompletableFuture<Void> processSinglePlayerStats(UUID uuid) {
        OfflinePlayer offlinePlayer = Bukkit.getOfflinePlayer(uuid);

        return statsFileReader.getPlayerDataMapAsync(offlinePlayer)
            .thenAcceptAsync(playerDataMap -> { // Process the result asynchronously
                if (playerDataMap != null && !playerDataMap.isEmpty()) {
                    httpUtils.queuePlayerData(playerDataMap);
                }
                // Increment count and log progress periodically
                int currentCount = processedCount.incrementAndGet();
                logProgressIfNeeded(currentCount);
            }, bukkitExecutor) // Ensure this runs on Bukkit's async pool
             .exceptionally(ex -> { // Handle errors for individual player processing
                handlePlayerProcessingError(uuid, ex);
                return null; // Required for exceptionally stage
             });
    }

    /**
     * Waits for the final batch of futures to complete and initiates cleanup.
     * Checks for cancellation using the helper method before marking as finished naturally.
     *
     * @param lastBatchFutures The list of futures from the last processed batch.
     */
    private void handleFinalBatchCompletion(List<CompletableFuture<Void>> lastBatchFutures) {
         plugin.debug("Bulk import iterator finished. Waiting for last batch futures.");
         CompletableFuture.allOf(lastBatchFutures.toArray(new CompletableFuture[0]))
            .whenCompleteAsync((unused, throwable) -> {
                 // Ensure cleanup happens after the last futures complete
                 // Check cancellation status *after* waiting
                 if (!isBukkitTaskCancelled()) {
                    plugin.debug("Last batch futures complete. Initiating final cleanup (finished naturally).");
                    cleanupBulkImport(true); // Mark as finished naturally
                 } else {
                     plugin.debug("Task was cancelled while waiting for last batch futures. Cleanup likely already run or will be run by cancel().");
                     // Cleanup might have already been triggered by cancel() override or run() check.
                     // Ensure cleanup runs if somehow missed.
                     if (isRunning.get()) { // Double-check if cleanup is needed
                        cleanupBulkImport(false); // Cancelled
                     }
                 }
                 // The run() method should detect !isRunning and self-cancel the BukkitTask.
             }, bukkitExecutor); // Use executor to avoid blocking scheduler thread if waiting is long
    }

    /**
     * Logs bulk import progress periodically.
     * @param currentCount The current number of processed players.
     */
    private void logProgressIfNeeded(int currentCount) {
         // Avoid spamming console, log every 100 or on the last one
         if (currentCount % 100 == 0 || currentCount == totalToProcess) {
            plugin.log(Level.INFO, "Bulk import progress: " + currentCount + "/" + totalToProcess);
         }
    }

    /**
     * Handles and logs errors encountered during single player processing in bulk import.
     * Also ensures the progress counter is incremented even on error.
     * @param uuid The UUID of the player that failed.
     * @param ex The exception thrown.
     * @return null (for CompletableFuture.exceptionally)
     */
    private Void handlePlayerProcessingError(UUID uuid, Throwable ex) {
        plugin.log(Level.WARNING, "Error processing stats during bulk import for UUID " + uuid + ": " + ex.getMessage()); // Log warning
        // Still count as processed *attempt* for progress tracking
        int currentCountOnError = processedCount.incrementAndGet();
        logProgressIfNeeded(currentCountOnError); // Log progress even on error if it hits a milestone
        return null;
    }

    // --- Periodic Sync Logic (Online Players) ---
    private void runPeriodicSync() {
        // --- Check if cancelled before proceeding (using helper) ---
        if (isBukkitTaskCancelled()) {
             plugin.debug("Periodic sync cancelled before starting.");
             return;
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
             // --- Added check for cancellation inside loop (using helper) ---
             if (isBukkitTaskCancelled()) {
                 checkCancelMidLoop = true;
                 plugin.debug("Periodic sync cancelled mid-loop.");
                 break; // Stop processing more players if task was cancelled
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
                     // Check cancellation again inside exceptionally block to avoid logging if cancelled
                     if (!isBukkitTaskCancelled()) {
                        plugin.log(Level.WARNING, "Error processing periodic sync stats for player " + player.getName() + ": " + ex.getMessage());
                     }
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
             // Check cancellation status *again* before logging completion message (using helper)
             if (!isBukkitTaskCancelled()) {
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
                 // Log message depends slightly on whether it was cancelled externally or finished partially
                 plugin.log(Level.INFO, "Bulk import task stopped or cancelled after processing " + processedCount.get() + "/" + totalToProcess + " players.");
             }
             // Clear the references in the main plugin class to allow a new import task
             plugin.clearBulkImportTaskReferences();

             // The run() method is responsible for cancelling the BukkitTask
             // after it detects that isRunning has become false.
             plugin.debug("isRunning flag set to false by cleanupBulkImport.");
         } else {
             plugin.debug("CleanupBulkImport called but task was already marked as not running.");
         }
    }


    /**
     * Checks if the Bukkit task is cancelled, handling potential IllegalStateException.
     * This prevents errors if checking a task that the scheduler already considers finished.
     * @return true if the task is cancelled or in an invalid state, false otherwise.
     */
    private boolean isBukkitTaskCancelled() {
        try {
            // Check the BukkitRunnable's cancelled status
            if (super.isCancelled()) {
                 // Don't log excessively here, let the caller decide context
                 // plugin.debug("Task cancellation detected by isBukkitTaskCancelled().");
                 return true;
            }
            return false;
        } catch (IllegalStateException e) {
            // This exception means the task is no longer scheduled (finished or cancelled)
            // according to the Bukkit scheduler. Treat it as cancelled for our logic.
            plugin.debug("IllegalStateException caught in isBukkitTaskCancelled(), task likely already finished/cancelled by scheduler.");
            return true;
        }
    }


    // --- Public method for commands/plugin to check logical status ---
    // Note: This reflects the internal 'isRunning' flag, not Bukkit's scheduler status directly.
    public boolean isTaskRunning() {
        // Check both the atomic boolean AND the iterator status for bulk import
        // to handle cases where the task might be scheduled but hasn't started processing yet.
        if (isBulkImport) {
            return isRunning.get() && bulkImportIterator != null && bulkImportIterator.hasNext();
        } else {
            // For periodic sync, we rely solely on Bukkit's scheduler status,
            // so this method isn't the primary way to check.
            // However, returning false seems reasonable if not bulk import.
            return false;
        }
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