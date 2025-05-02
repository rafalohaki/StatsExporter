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

/**
 * Handles synchronization of player statistics, operating in two modes:
 * 1. Periodic Sync: Regularly syncs stats for currently online players.
 * 2. Bulk Import: Processes all player stat files from the stats directory, typically triggered by a command.
 */
public class StatsSyncTask extends BukkitRunnable {

    private final StatsExporterPlugin plugin;
    private final HttpUtils httpUtils;
    private final StatsFileReader statsFileReader;
    private final Executor bukkitExecutor; // Executor using Bukkit's async scheduler

    // Task Mode specifics
    private final boolean isBulkImport;
    private final Iterator<UUID> bulkImportIterator; // Iterator for UUIDs in bulk mode
    private final int playersPerTick;          // How many players to process per tick in bulk mode
    private final int totalToProcess;          // Total UUIDs in the bulk import list
    private final AtomicInteger processedCount = new AtomicInteger(0); // Counter for processed players in bulk mode
    private final Instant importStartTime;           // Start time for bulk import duration calculation
    private final AtomicBoolean isRunning = new AtomicBoolean(false); // Tracks if the bulk import *logic* is actively running
    private final AtomicInteger runCounter = new AtomicInteger(0); // Counter for run() calls for debugging

    /**
     * Constructor for Periodic Sync mode (online players).
     * @param plugin The main plugin instance.
     */
    public StatsSyncTask(StatsExporterPlugin plugin) {
        this.plugin = plugin;
        this.httpUtils = plugin.getHttpUtils();
        this.statsFileReader = plugin.getStatsFileReader();
        this.isBulkImport = false;
        this.playersPerTick = 0; // Not used
        this.totalToProcess = 0; // Not used
        this.bulkImportIterator = null; // Not used
        this.importStartTime = null; // Not used
        this.bukkitExecutor = runnable -> Bukkit.getScheduler().runTaskAsynchronously(plugin, runnable);
        // Optional: Log creation if needed
        // plugin.debug("StatsSyncTask created for Periodic Sync.");
    }

    /**
     * Constructor for Bulk Import mode (all player files).
     * @param plugin The main plugin instance.
     * @param uuidsToImport List of player UUIDs to process.
     */
    public StatsSyncTask(StatsExporterPlugin plugin, List<UUID> uuidsToImport) {
        this.plugin = plugin;
        this.httpUtils = plugin.getHttpUtils();
        this.statsFileReader = plugin.getStatsFileReader();
        this.isBulkImport = true;
        this.playersPerTick = Math.max(1, plugin.getPluginConfig().getInt("import.playersPerTick", 2)); // Ensure at least 1
        this.totalToProcess = (uuidsToImport != null) ? uuidsToImport.size() : 0;
        this.bulkImportIterator = (uuidsToImport != null) ? uuidsToImport.iterator() : Collections.emptyIterator();
        this.importStartTime = Instant.now();
        this.bukkitExecutor = runnable -> Bukkit.getScheduler().runTaskAsynchronously(plugin, runnable);
        plugin.debug("StatsSyncTask created for Bulk Import (" + totalToProcess + " players, " + playersPerTick + " per tick).");
    }

    /**
     * Main execution logic called by the Bukkit scheduler.
     */
    @Override
    public void run() {
        int currentRunCount = runCounter.incrementAndGet();
        int taskId = -1; // Default Task ID if retrieval fails
        try {
             // Try to get ID for logging, but don't rely on it for critical logic early on
             taskId = getTaskId();
        } catch (IllegalStateException e) {
            // Log only on the first run if ID isn't available yet
            if (currentRunCount == 1) {
                plugin.log(Level.WARNING, "!!! StatsSyncTask (ID unknown) run() #1 entered but couldn't get Task ID immediately. isBulkImport=" + isBulkImport);
            }
            // For subsequent runs, this would be highly unusual unless the task was externally cancelled and somehow restarted by Bukkit.
        }
        final int finalTaskId = taskId; // Make task ID effectively final for use in lambdas/logging

        if (finalTaskId != -1) {
             plugin.log(Level.INFO, "!!! StatsSyncTask (" + finalTaskId + ") run() method ENTERED #" + currentRunCount + ". isBulkImport=" + isBulkImport);
        }

        // --- Primary Cancellation Check ---
        if (isBukkitTaskCancelled(finalTaskId)) { // Use the fixed helper method
            plugin.log(Level.WARNING,"!!! StatsSyncTask (" + finalTaskId + ") run() ENTERED but task is cancelled.");
            if (isBulkImport && isRunning.get()) {
                cleanupBulkImport(false, finalTaskId); // Cancelled externally
            }
            return; // Do not proceed
        }

        // --- Execute Task Logic based on Mode ---
        try {
            if (isBulkImport) {
                runBulkImportTickWrapper(finalTaskId);
            } else {
                runPeriodicSync(finalTaskId);
            }
        } catch (Throwable t) {
            // Catch unexpected errors within the sync logic itself
            plugin.log(Level.SEVERE, "!!! Uncaught exception in StatsSyncTask (" + finalTaskId + ") run() method !!! - Task will likely stop.", t);
            if (isBulkImport) {
                cleanupBulkImport(false, finalTaskId); // Ensure cleanup on unexpected error
            }
            // Cancel the task as it's in an unknown state
            try {
                cancel();
            } catch (Exception e) {
                plugin.log(Level.SEVERE, "Failed to cancel task (" + finalTaskId + ") after uncaught exception.", e);
            }
        }
         plugin.debug("!!! StatsSyncTask (" + finalTaskId + ") run() method EXITING #" + currentRunCount);
    }

    /**
     * Wrapper for bulk import logic within the run() method's try-catch.
     * Handles setting the initial running state.
     * @param taskId The task ID for logging purposes (-1 if unavailable).
     */
    private void runBulkImportTickWrapper(int taskId) {
        plugin.debug("--- StatsSyncTask (" + taskId + ") run() - Entering Bulk Import Logic ---");

        // Mark as running on the first execution tick if not already running
        if (!isRunning.get() && bulkImportIterator != null && bulkImportIterator.hasNext()) {
            if (isRunning.compareAndSet(false, true)) {
                plugin.log(Level.INFO, "--- StatsSyncTask (" + taskId + ") Bulk import task commencing execution. ---");
            } else {
                // This might indicate a race condition or logic error if reached
                plugin.log(Level.WARNING, "--- StatsSyncTask (" + taskId + ") Bulk import run() entered, but isRunning was already true?");
            }
        } else if (!isRunning.get()) {
            // If run() is called but we are no longer marked as running (likely due to prior cleanup)
            plugin.log(Level.WARNING, "--- StatsSyncTask (" + taskId + ") Bulk import run() called but isRunning is false. Iterator hasNext: " +
                    (bulkImportIterator != null && bulkImportIterator.hasNext()) + ". Likely already cleaned up.");
            // Ensure the task is actually cancelled in Bukkit's scheduler if it reached this state unexpectedly
            if (!isBukkitTaskCancelled(taskId)) {
                try { cancel(); } catch (Exception ignored) {}
            }
            return; // Stop execution for this tick
        }

        // Perform one tick's worth of bulk import work
        runBulkImportTick(taskId);
    }


    /**
     * Processes a batch of players for the bulk import during a single tick.
     * @param taskId The task ID for logging purposes (-1 if unavailable).
     */
    private void runBulkImportTick(int taskId) {
        plugin.debug(">>> StatsSyncTask (" + taskId + ") runBulkImportTick START");

        // 1. Check if iterator has items (completion condition)
        if (!bulkImportIterator.hasNext()) {
            plugin.debug("--- runBulkImportTick (" + taskId + "): Iterator is empty, initiating natural cleanup.");
            cleanupBulkImport(true, taskId); // Finished naturally
            return;
        }

        // 2. Check for external cancellation *before* processing the batch
        if (isBukkitTaskCancelled(taskId)) {
             plugin.log(Level.INFO, "--- runBulkImportTick (" + taskId + "): Cancellation detected before processing batch. Cleaning up.");
             cleanupBulkImport(false, taskId); // Cancelled externally
             return;
        }

        // 3. Process the Batch
        plugin.debug(">>> runBulkImportTick (" + taskId + "): Processing player batch...");
        List<CompletableFuture<Void>> batchFutures = processPlayerBatch(taskId); // Returns null if cancelled mid-batch

        // 4. Handle potential cancellation *during* batch processing
        if (batchFutures == null) {
             plugin.log(Level.INFO, "--- runBulkImportTick (" + taskId + "): processPlayerBatch indicated cancellation mid-batch. Cleaning up.");
             cleanupBulkImport(false, taskId); // Cancelled externally/mid-batch
             return;
        }
        plugin.debug("<<< runBulkImportTick (" + taskId + "): Batch processing scheduled for " + batchFutures.size() + " players.");

        // 5. Check if this was the *last* batch
        if (!bulkImportIterator.hasNext()) {
             plugin.debug("--- runBulkImportTick (" + taskId + "): Iterator is now empty. Handling final batch completion...");
             handleFinalBatchCompletion(batchFutures, taskId);
        }

         plugin.debug("<<< StatsSyncTask (" + taskId + ") runBulkImportTick END");
    }

    /**
     * Processes up to `playersPerTick` players from the iterator.
     * @param taskId The task ID for logging purposes (-1 if unavailable).
     * @return A List of CompletableFuture for the processed players, or null if cancellation was detected mid-processing.
     */
    private List<CompletableFuture<Void>> processPlayerBatch(int taskId) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        int processedThisTick = 0;

        for (int i = 0; i < playersPerTick && bulkImportIterator.hasNext(); i++) {
             // Check cancellation *inside* the loop for responsiveness
             if (isBukkitTaskCancelled(taskId)) {
                 plugin.debug("--- processPlayerBatch (" + taskId + "): Cancellation detected mid-loop.");
                 return null; // Signal cancellation
             }

            UUID uuid = bulkImportIterator.next();
            futures.add(processSinglePlayerStats(uuid, taskId));
            processedThisTick++;
        }
        plugin.debug("--- processPlayerBatch (" + taskId + "): Scheduled processing for " + processedThisTick + " players this tick.");
        return futures;
    }

    /**
     * Creates a CompletableFuture to read, map, and queue stats for a single player UUID.
     * @param uuid The player's UUID.
     * @param taskId The task ID for logging purposes (-1 if unavailable).
     * @return A CompletableFuture representing the async operation.
     */
    private CompletableFuture<Void> processSinglePlayerStats(UUID uuid, int taskId) {
        // Getting OfflinePlayer can involve I/O, do it async if needed, though usually fast
        OfflinePlayer offlinePlayer = Bukkit.getOfflinePlayer(uuid);
        String playerNameHint = offlinePlayer.getName(); // Get name for logging

        plugin.debug("--- processing (" + taskId + "): Scheduling stats read for UUID: " + uuid + (playerNameHint != null ? " (" + playerNameHint + ")" : ""));

        return statsFileReader.getPlayerDataMapAsync(offlinePlayer)
            .thenAcceptAsync(playerDataMap -> { // Runs after stats are read and mapped
                // Check cancellation *again* before queueing
                if (isBukkitTaskCancelled(taskId)) {
                    plugin.debug("--- processing (" + taskId + "): Cancelled before queueing data for " + uuid);
                    return;
                }

                if (playerDataMap != null && !playerDataMap.isEmpty()) {
                    plugin.debug("--- processing (" + taskId + "): Successfully got data for " + uuid + ", queueing.");
                    httpUtils.queuePlayerData(playerDataMap);
                } else {
                    plugin.debug("--- processing (" + taskId + "): No stats data found/read for " + uuid + ", skipping queue.");
                }
                // Increment and log progress *after* attempting to queue (or skipping)
                logProgressIfNeeded(processedCount.incrementAndGet(), taskId);

            }, bukkitExecutor) // Ensure this accept stage also runs async
             .exceptionally(ex -> { // Handle errors during the stats reading/mapping phase
                 if (!isBukkitTaskCancelled(taskId)) { // Log only if not cancelled
                    plugin.log(Level.WARNING, "!!! Error processing stats during bulk import for UUID " + uuid + (playerNameHint != null ? " (" + playerNameHint + ")" : "") + ": " + ex.getMessage());
                 }
                 // Still increment count and log progress even if this player failed
                 logProgressIfNeeded(processedCount.incrementAndGet(), taskId);
                 return null; // Necessary for exceptionally block
             });
    }

    /**
     * Waits for the final batch of futures to complete and then triggers cleanup.
     * @param lastBatchFutures The list of futures from the last processing tick.
     * @param taskId The task ID for logging purposes (-1 if unavailable).
     */
    private void handleFinalBatchCompletion(List<CompletableFuture<Void>> lastBatchFutures, int taskId) {
         plugin.debug("--- handleFinalBatchCompletion (" + taskId + "): Waiting for last " + lastBatchFutures.size() + " player futures...");

         CompletableFuture.allOf(lastBatchFutures.toArray(new CompletableFuture[0]))
            .whenCompleteAsync((unused, throwable) -> { // Runs after all futures in the batch complete
                 plugin.debug("--- handleFinalBatchCompletion (" + taskId + "): Final batch futures completed.");
                 if (throwable != null && !isBukkitTaskCancelled(taskId)) {
                     // Log errors from the last batch if any occurred and we weren't cancelled
                     plugin.log(Level.WARNING, "--- handleFinalBatchCompletion (" + taskId + "): Errors occurred in the final batch.", throwable);
                 }

                 // Double-check cancellation status before declaring natural finish
                 if (!isBukkitTaskCancelled(taskId)) {
                    plugin.debug("--- handleFinalBatchCompletion (" + taskId + "): Task not cancelled, initiating natural cleanup.");
                    cleanupBulkImport(true, taskId); // Finished naturally
                 } else {
                    plugin.log(Level.INFO, "--- handleFinalBatchCompletion (" + taskId + "): Task was cancelled while waiting for final batch. Cleanup should already be in progress/finished.");
                    // Ensure cleanup runs if somehow missed
                     if (isRunning.get()) {
                        cleanupBulkImport(false, taskId); // Cancelled
                     }
                 }
             }, bukkitExecutor); // Run completion logic async
    }

    /**
     * Logs the bulk import progress periodically.
     * @param currentCount The number of players processed so far.
     * @param taskId The task ID for logging purposes (-1 if unavailable).
     */
    private void logProgressIfNeeded(int currentCount, int taskId) {
         // Log every 100 players or on the very last player
         if (totalToProcess > 0 && (currentCount % 100 == 0 || currentCount == totalToProcess)) {
            plugin.log(Level.INFO, "Bulk import progress (" + taskId + "): " + currentCount + "/" + totalToProcess);
         }
    }


    /**
     * Runs the periodic synchronization for online players.
     * @param taskId The task ID for logging purposes (-1 if unavailable).
     */
    private void runPeriodicSync(int taskId) {
        if (isBukkitTaskCancelled(taskId)) {
             plugin.debug("Periodic sync (" + taskId + ") cancelled before starting.");
             return;
         }

        plugin.debug("--- Running Periodic Sync (" + taskId + ") ---");
        Collection<? extends Player> onlinePlayers = Bukkit.getOnlinePlayers();

        if (onlinePlayers.isEmpty()) {
            plugin.debug("Periodic sync (" + taskId + "): No players online.");
            return;
        }

        plugin.debug("Periodic sync (" + taskId + "): Found " + onlinePlayers.size() + " online players.");
        AtomicInteger syncCount = new AtomicInteger(0);
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (Player player : onlinePlayers) {
             // Check cancellation inside the loop
             if (isBukkitTaskCancelled(taskId)) {
                 plugin.debug("Periodic sync (" + taskId + ") cancelled mid-loop.");
                 break; // Stop processing more players for this cycle
             }

             plugin.debug("Periodic sync (" + taskId + "): Queueing sync for online player: " + player.getName());
             CompletableFuture<Void> future = statsFileReader.getPlayerDataMapAsync(player)
                .thenAcceptAsync(playerDataMap -> {
                    // Check cancellation before queueing
                     if (isBukkitTaskCancelled(taskId)) return;

                    if (playerDataMap != null && !playerDataMap.isEmpty()) {
                        httpUtils.queuePlayerData(playerDataMap);
                        syncCount.incrementAndGet();
                         plugin.debug("Periodic sync (" + taskId + "): Queued data for " + player.getName());
                    } else {
                         plugin.debug("Periodic sync (" + taskId + "): No data found for " + player.getName());
                    }
                }, bukkitExecutor)
                 .exceptionally(ex -> {
                     if (!isBukkitTaskCancelled(taskId)) {
                        plugin.log(Level.WARNING, "!!! Error in periodic sync for player " + player.getName() + ": " + ex.getMessage());
                     }
                     return null;
                 });
             futures.add(future);
        }

        // Wait for all scheduled futures for this cycle to complete (unless cancelled mid-loop)
         if (!isBukkitTaskCancelled(taskId) && !futures.isEmpty()) {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).whenComplete((unused, throwable) -> {
                if (!isBukkitTaskCancelled(taskId)) {
                    plugin.debug("--- Periodic sync cycle (" + taskId + ") completed. Successfully queued data for " + syncCount.get() + " online players. ---");
                    if (throwable != null) {
                        plugin.log(Level.WARNING, "Periodic sync cycle (" + taskId + ") finished, but some errors occurred during processing.", throwable);
                    }
                } else {
                    plugin.debug("Periodic sync (" + taskId + ") was cancelled while waiting for player futures.");
                }
            });
         } else if (isBukkitTaskCancelled(taskId)) {
              plugin.debug("Periodic sync (" + taskId + ") cycle aborted due to cancellation.");
         }
    }


    /**
     * Centralized cleanup logic for the bulk import task. Ensures it runs only once.
     * Sets the running state to false, logs the final status, clears plugin references,
     * and attempts to cancel the Bukkit task itself.
     *
     * @param finishedNaturally True if the task completed all items, false if cancelled/stopped early.
     * @param taskId The task ID for logging purposes (-1 if unavailable).
     */
    private void cleanupBulkImport(boolean finishedNaturally, int taskId) {
         // Use compareAndSet to ensure this cleanup logic runs only once per task instance
         if (isRunning.compareAndSet(true, false)) {
             plugin.log(Level.INFO, "--- StatsSyncTask (" + taskId + ") cleanupBulkImport running (finishedNaturally=" + finishedNaturally + ") ---");

             // Log final status based on completion type
             if (finishedNaturally) {
                 Instant endTime = Instant.now();
                 Duration duration = (importStartTime != null) ? Duration.between(importStartTime, endTime) : null;
                 plugin.log(Level.INFO, "Bulk import (" + taskId + ") finished processing " + processedCount.get() + "/" + totalToProcess + " players" + (duration != null ? " in " + formatDuration(duration) : "."));
             } else {
                 plugin.log(Level.INFO, "Bulk import (" + taskId + ") stopped or cancelled after processing " + processedCount.get() + "/" + totalToProcess + " players.");
             }

             // Clear the references in the main plugin class to allow GC and prevent stale state
             plugin.clearBulkImportTaskReferences();

             // Attempt to explicitly cancel the Bukkit task to stop the timer
             try {
                 plugin.debug("--- StatsSyncTask (" + taskId + ") cleanupBulkImport attempting self-cancel...");
                 this.cancel(); // Calls BukkitRunnable.cancel() -> which calls our override
                 plugin.debug("--- StatsSyncTask (" + taskId + ") cleanupBulkImport self-cancel called.");
             } catch (IllegalStateException e) {
                 // This is expected if cancel() was already called externally or if the task finished very recently.
                 plugin.debug("--- StatsSyncTask (" + taskId + ") cleanupBulkImport ignoring IllegalStateException during self-cancel, task likely already cancelled/finished by Bukkit.");
             } catch (Exception e) {
                 // Catch any other unexpected errors during cancellation
                 plugin.log(Level.SEVERE, "--- StatsSyncTask (" + taskId + ") cleanupBulkImport Unexpected error during self-cancel!", e);
             }
             plugin.log(Level.INFO, "--- StatsSyncTask (" + taskId + ") cleanupBulkImport finished. ---");
         } else {
             // This might happen if cancel() is called rapidly multiple times or race conditions
             plugin.debug("--- StatsSyncTask (" + taskId + ") cleanupBulkImport called but task was already marked as not running (isRunning=false). ---");
         }
    }

    /**
     * Override BukkitRunnable's cancel method to ensure our cleanup logic is triggered
     * when cancelled externally (e.g., by command or plugin disable).
     */
    @Override
    public synchronized void cancel() throws IllegalStateException {
        int taskId = -1; try { taskId = getTaskId(); } catch (IllegalStateException ignored) {} // Safe get ID for logging
        plugin.debug("StatsSyncTask (" + taskId + ") cancel() method invoked.");

        // Variable 'wasCancelled' removed as it was unused.

        try {
            // Check if Bukkit already considers it cancelled BEFORE trying to cancel again
            if (!super.isCancelled()) {
                super.cancel(); // Attempt to cancel via Bukkit first
                 plugin.debug("StatsSyncTask (" + taskId + ") cancel(): Called super.cancel().");
            } else {
                 plugin.debug("StatsSyncTask (" + taskId + ") cancel(): super.isCancelled() was already true.");
            }
        } catch (IllegalStateException e) {
            plugin.debug("StatsSyncTask (" + taskId + ") cancel(): Caught IllegalStateException from super.isCancelled/cancel(), task likely already finished/cancelled by Bukkit.");
            // No action needed here regarding the removed variable.
        } finally {
            // Crucially, ensure internal cleanup runs if this was a bulk import task,
            // regardless of whether we called super.cancel() or it was already cancelled.
            if (isBulkImport) {
                plugin.debug("StatsSyncTask (" + taskId + ") cancel(): Triggering cleanupBulkImport(false).");
                cleanupBulkImport(false, taskId); // Mark as cancelled externally
            }
        }
    }

    /**
     * Checks if the Bukkit task associated with this runnable is cancelled.
     * Handles potential IllegalStateException if the task is already finished.
     * @param taskId The task ID for logging purposes (-1 if unavailable).
     * @return true if the task is cancelled or finished, false otherwise.
     */
    private boolean isBukkitTaskCancelled(int taskId) {
        try {
            // Rely solely on BukkitRunnable's isCancelled() method.
            boolean cancelled = super.isCancelled();
            // if (cancelled) {
            //     plugin.debug("isBukkitTaskCancelled (" + taskId + "): super.isCancelled() returned true.");
            // }
            return cancelled;
        } catch (IllegalStateException e) {
            // This catch block handles cases where the task state is inconsistent
            // or checked too early/late according to Bukkit internals.
            plugin.log(Level.WARNING,"isBukkitTaskCancelled (" + taskId + "): Caught IllegalStateException from super.isCancelled(). Task is likely finished or already cancelled.");
            return true; // Treat as cancelled/finished if state is illegal
        }
    }


    /**
     * Checks if the bulk import task *believes* it should be running.
     * @return True if the bulk import is active, false otherwise or if not in bulk mode.
     */
    public boolean isTaskRunning() {
        // This reflects the internal state flag, primarily for the bulk import.
        // It might briefly be true even if cancellation is in progress but cleanup hasn't finished.
        return isBulkImport && isRunning.get();
    }

    /**
     * Formats a Duration into a human-readable string (Hh MMm SSs).
     * @param duration The duration to format.
     * @return Formatted string or "N/A".
     */
    private String formatDuration(Duration duration) {
        if (duration == null) return "N/A";
        long seconds = duration.toSeconds(); // Use toSeconds for simplicity
        long absSeconds = Math.abs(seconds);
        String positive = String.format(
                "%dh %02dm %02ds",
                absSeconds / 3600,
                (absSeconds % 3600) / 60,
                absSeconds % 60);
        return seconds < 0 ? "-" + positive : positive;
    }
}