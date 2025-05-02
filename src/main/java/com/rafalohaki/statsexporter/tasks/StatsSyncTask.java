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
    private final Executor bukkitExecutor;

    // Task Mode specifics
    private final boolean isBulkImport;
    private final Iterator<UUID> bulkImportIterator;
    private final int playersPerTick;
    private final int totalToProcess;
    private final AtomicInteger processedCount = new AtomicInteger(0);
    private final Instant importStartTime;
    private final AtomicBoolean isRunning = new AtomicBoolean(false); // Use AtomicBoolean for thread-safety
    private final AtomicInteger runCounter = new AtomicInteger(0); // Debug counter

    /** Constructor for Periodic Sync mode */
    public StatsSyncTask(StatsExporterPlugin plugin) {
        this.plugin = plugin;
        this.httpUtils = plugin.getHttpUtils();
        this.statsFileReader = plugin.getStatsFileReader();
        this.isBulkImport = false;
        this.playersPerTick = 0;
        this.totalToProcess = 0;
        this.bulkImportIterator = null;
        this.importStartTime = null;
        this.bukkitExecutor = runnable -> Bukkit.getScheduler().runTaskAsynchronously(plugin, runnable);
        plugin.debug("StatsSyncTask created for Periodic Sync."); // Use debug
    }

    /** Constructor for Bulk Import mode */
    public StatsSyncTask(StatsExporterPlugin plugin, List<UUID> uuidsToImport) {
        this.plugin = plugin;
        this.httpUtils = plugin.getHttpUtils();
        this.statsFileReader = plugin.getStatsFileReader();
        this.isBulkImport = true;
        this.playersPerTick = Math.max(1, plugin.getPluginConfig().getInt("import.playersPerTick", 2));
        this.totalToProcess = (uuidsToImport != null) ? uuidsToImport.size() : 0;
        this.bulkImportIterator = (uuidsToImport != null) ? uuidsToImport.iterator() : Collections.emptyIterator();
        this.importStartTime = Instant.now();
        this.bukkitExecutor = runnable -> Bukkit.getScheduler().runTaskAsynchronously(plugin, runnable);
        // Use debug for detailed creation log
        plugin.debug("StatsSyncTask created for Bulk Import (" + totalToProcess + " players, " + playersPerTick + " per tick).");
    }

    @Override
    public void run() {
        int currentRunCount = runCounter.incrementAndGet();
        int taskId = -1;
        try {
             taskId = getTaskId();
        } catch (IllegalStateException e) {
            // Log only on the first run if ID isn't available yet
            if (currentRunCount == 1) {
                // Keep warning for scheduler state issue
                plugin.log(Level.WARNING, "StatsSyncTask (ID unknown) run() #" + currentRunCount + " entered but couldn't get Task ID immediately. isBulkImport=" + isBulkImport);
            }
        }
        final int finalTaskId = taskId;

        plugin.debug("StatsSyncTask (" + finalTaskId + ") run() ENTERED #" + currentRunCount + ". isBulkImport=" + isBulkImport); // Use debug

        if (isBukkitTaskCancelled(finalTaskId)) {
            // Keep warning for cancellation detection
            plugin.log(Level.WARNING,"StatsSyncTask (" + finalTaskId + ") run() ENTERED but task is cancelled.");
            if (isBulkImport && isRunning.get()) {
                cleanupBulkImport(false, finalTaskId); // Cancelled externally
            }
            return;
        }

        try {
            if (isBulkImport) {
                runBulkImportTickWrapper(finalTaskId);
            } else {
                runPeriodicSync(finalTaskId);
            }
        } catch (Throwable t) {
            plugin.log(Level.SEVERE, "Uncaught exception in StatsSyncTask (" + finalTaskId + ") run() method! Task likely stopped.", t); // Keep SEVERE
            if (isBulkImport) {
                cleanupBulkImport(false, finalTaskId);
            }
            try {
                cancel(); // Attempt self-cancellation on fatal error
            } catch (Exception e) {
                plugin.log(Level.SEVERE, "Failed to cancel task (" + finalTaskId + ") after uncaught exception.", e); // Keep SEVERE
            }
        }
         plugin.debug("StatsSyncTask (" + finalTaskId + ") run() EXITING #" + currentRunCount); // Use debug
    }

    private void runBulkImportTickWrapper(int taskId) {
        plugin.debug("StatsSyncTask (" + taskId + ") run() - Entering Bulk Import Logic"); // Use debug

        if (!isRunning.get() && bulkImportIterator != null && bulkImportIterator.hasNext()) {
            if (isRunning.compareAndSet(false, true)) {
                // Keep INFO for important state change (start)
                plugin.log(Level.INFO, "StatsSyncTask (" + taskId + ") Bulk import task commencing execution.");
            } else {
                // Keep WARNING for unexpected state
                plugin.log(Level.WARNING, "StatsSyncTask (" + taskId + ") Bulk import run() entered, but isRunning was already true?");
            }
        } else if (!isRunning.get()) {
            // Keep WARNING for potentially incorrect state
            plugin.log(Level.WARNING, "StatsSyncTask (" + taskId + ") Bulk import run() called but isRunning is false. Iterator hasNext: " +
                    (bulkImportIterator != null && bulkImportIterator.hasNext()) + ". Likely already cleaned up.");
            if (!isBukkitTaskCancelled(taskId)) {
                try { cancel(); } catch (Exception ignored) {}
            }
            return;
        }
        runBulkImportTick(taskId);
    }

    private void runBulkImportTick(int taskId) {
        plugin.debug("StatsSyncTask (" + taskId + ") runBulkImportTick START"); // Use debug

        if (bulkImportIterator == null || !bulkImportIterator.hasNext()) {
            plugin.debug("runBulkImportTick (" + taskId + "): Iterator is empty or null, initiating natural cleanup."); // Use debug
            cleanupBulkImport(true, taskId);
            return;
        }

        if (isBukkitTaskCancelled(taskId)) {
             plugin.debug("runBulkImportTick (" + taskId + "): Cancellation detected before processing batch. Cleaning up."); // Use debug
             cleanupBulkImport(false, taskId);
             return;
        }

        plugin.debug("runBulkImportTick (" + taskId + "): Processing player batch..."); // Use debug
        List<CompletableFuture<Void>> batchFutures = processPlayerBatch(taskId);

        if (batchFutures == null) {
             plugin.debug("runBulkImportTick (" + taskId + "): processPlayerBatch indicated cancellation mid-batch. Cleaning up."); // Use debug
             cleanupBulkImport(false, taskId);
             return;
        }
        plugin.debug("runBulkImportTick (" + taskId + "): Batch processing scheduled for " + batchFutures.size() + " players."); // Use debug

        // Check if the iterator became empty *after* processing the batch
        if (!bulkImportIterator.hasNext()) {
             plugin.debug("runBulkImportTick (" + taskId + "): Iterator is now empty. Handling final batch completion..."); // Use debug
             handleFinalBatchCompletion(batchFutures, taskId);
        }

         plugin.debug("StatsSyncTask (" + taskId + ") runBulkImportTick END"); // Use debug
    }

    private List<CompletableFuture<Void>> processPlayerBatch(int taskId) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        int processedThisTick = 0;

        if (bulkImportIterator == null) return futures; // Safety check

        for (int i = 0; i < playersPerTick && bulkImportIterator.hasNext(); i++) {
             if (isBukkitTaskCancelled(taskId)) {
                 plugin.debug("processPlayerBatch (" + taskId + "): Cancellation detected mid-loop."); // Use debug
                 return null;
             }

            UUID uuid = bulkImportIterator.next();
            if (uuid != null) {
                futures.add(processSinglePlayerStats(uuid, taskId));
                processedThisTick++;
            }
        }
        plugin.debug("processPlayerBatch (" + taskId + "): Scheduled processing for " + processedThisTick + " players this tick."); // Use debug
        return futures;
    }

    private CompletableFuture<Void> processSinglePlayerStats(UUID uuid, int taskId) {
        OfflinePlayer offlinePlayer = Bukkit.getOfflinePlayer(uuid);
        String playerNameHint = offlinePlayer.getName();

        plugin.debug("processing (" + taskId + "): Scheduling stats read for UUID: " + uuid + (playerNameHint != null ? " (" + playerNameHint + ")" : "")); // Use debug

        return statsFileReader.getPlayerDataMapAsync(offlinePlayer)
            .thenAcceptAsync(playerDataMap -> {
                if (isBukkitTaskCancelled(taskId)) {
                    plugin.debug("processing (" + taskId + "): Cancelled before queueing data for " + uuid); // Use debug
                    return;
                }

                if (playerDataMap != null && !playerDataMap.isEmpty()) {
                    plugin.debug("processing (" + taskId + "): Successfully got data for " + uuid + ", queueing."); // Use debug
                    httpUtils.queuePlayerData(playerDataMap);
                } else {
                    plugin.debug("processing (" + taskId + "): No stats data found/read for " + uuid + ", skipping queue."); // Use debug
                }
                logProgressIfNeeded(processedCount.incrementAndGet(), taskId); // Calls INFO internally if needed

            }, bukkitExecutor)
             .exceptionally(ex -> {
                 if (!isBukkitTaskCancelled(taskId)) {
                    // Keep WARNING for player-specific processing errors
                    plugin.log(Level.WARNING, "Error processing stats during bulk import for UUID " + uuid + (playerNameHint != null ? " (" + playerNameHint + ")" : "") + ": " + ex.getMessage());
                 }
                 logProgressIfNeeded(processedCount.incrementAndGet(), taskId); // Still count as processed (attempted)
                 return null;
             });
    }

    private void handleFinalBatchCompletion(List<CompletableFuture<Void>> lastBatchFutures, int taskId) {
         plugin.debug("handleFinalBatchCompletion (" + taskId + "): Waiting for last " + lastBatchFutures.size() + " player futures..."); // Use debug

         CompletableFuture.allOf(lastBatchFutures.toArray(new CompletableFuture[0]))
            .whenCompleteAsync((unused, throwable) -> {
                 plugin.debug("handleFinalBatchCompletion (" + taskId + "): Final batch futures completed."); // Use debug
                 if (throwable != null && !isBukkitTaskCancelled(taskId)) {
                     // Keep WARNING if errors occurred in the last batch
                     plugin.log(Level.WARNING, "handleFinalBatchCompletion (" + taskId + "): Errors occurred in the final batch.", throwable);
                 }

                 if (!isBukkitTaskCancelled(taskId)) {
                    plugin.debug("handleFinalBatchCompletion (" + taskId + "): Task not cancelled, initiating natural cleanup."); // Use debug
                    cleanupBulkImport(true, taskId);
                 } else {
                    plugin.debug("handleFinalBatchCompletion (" + taskId + "): Task was cancelled while waiting for final batch. Cleanup should already be in progress/finished."); // Use debug
                     if (isRunning.get()) {
                        cleanupBulkImport(false, taskId);
                     }
                 }
             }, bukkitExecutor);
    }

    // Keep INFO for progress milestones
    private void logProgressIfNeeded(int currentCount, int taskId) {
         if (totalToProcess > 0 && (currentCount % 100 == 0 || currentCount == totalToProcess)) {
            plugin.log(Level.INFO, "Bulk import progress (" + taskId + "): " + currentCount + "/" + totalToProcess);
         }
    }

    private void runPeriodicSync(int taskId) {
        if (isBukkitTaskCancelled(taskId)) {
             plugin.debug("Periodic sync (" + taskId + ") cancelled before starting."); // Use debug
             return;
         }

        plugin.debug("Running Periodic Sync (" + taskId + ")"); // Use debug
        Collection<? extends Player> onlinePlayers = Bukkit.getOnlinePlayers();

        if (onlinePlayers.isEmpty()) {
            plugin.debug("Periodic sync (" + taskId + "): No players online."); // Use debug
            return;
        }

        plugin.debug("Periodic sync (" + taskId + "): Found " + onlinePlayers.size() + " online players."); // Use debug
        AtomicInteger syncCount = new AtomicInteger(0);
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (Player player : onlinePlayers) {
             if (isBukkitTaskCancelled(taskId)) {
                 plugin.debug("Periodic sync (" + taskId + ") cancelled mid-loop."); // Use debug
                 break;
             }

             plugin.debug("Periodic sync (" + taskId + "): Queueing sync for online player: " + player.getName()); // Use debug
             CompletableFuture<Void> future = statsFileReader.getPlayerDataMapAsync(player)
                .thenAcceptAsync(playerDataMap -> {
                     if (isBukkitTaskCancelled(taskId)) return;

                    if (playerDataMap != null && !playerDataMap.isEmpty()) {
                        httpUtils.queuePlayerData(playerDataMap);
                        syncCount.incrementAndGet();
                         plugin.debug("Periodic sync (" + taskId + "): Queued data for " + player.getName()); // Use debug
                    } else {
                         plugin.debug("Periodic sync (" + taskId + "): No data found for " + player.getName()); // Use debug
                    }
                }, bukkitExecutor)
                 .exceptionally(ex -> {
                     if (!isBukkitTaskCancelled(taskId)) {
                         // Keep WARNING for player-specific errors
                        plugin.log(Level.WARNING, "Error in periodic sync for player " + player.getName() + ": " + ex.getMessage());
                     }
                     return null;
                 });
             futures.add(future);
        }

         if (!isBukkitTaskCancelled(taskId) && !futures.isEmpty()) {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).whenComplete((unused, throwable) -> {
                if (!isBukkitTaskCancelled(taskId)) {
                    // Use debug for successful cycle completion
                    plugin.debug("Periodic sync cycle (" + taskId + ") completed. Successfully queued data for " + syncCount.get() + " online players.");
                    if (throwable != null) {
                        // Keep WARNING if cycle had errors
                        plugin.log(Level.WARNING, "Periodic sync cycle (" + taskId + ") finished, but some errors occurred during processing.", throwable);
                    }
                } else {
                    plugin.debug("Periodic sync (" + taskId + ") was cancelled while waiting for player futures."); // Use debug
                }
            });
         } else if (isBukkitTaskCancelled(taskId)) {
              plugin.debug("Periodic sync (" + taskId + ") cycle aborted due to cancellation."); // Use debug
         }
    }

    private void cleanupBulkImport(boolean finishedNaturally, int taskId) {
         if (isRunning.compareAndSet(true, false)) {
             // Keep INFO for overall start of cleanup
             plugin.log(Level.INFO, "StatsSyncTask (" + taskId + ") cleanupBulkImport running (finishedNaturally=" + finishedNaturally + ")");

             if (finishedNaturally) {
                 Instant endTime = Instant.now();
                 Duration duration = (importStartTime != null) ? Duration.between(importStartTime, endTime) : null;
                 // Keep INFO for final success summary
                 plugin.log(Level.INFO, "Bulk import (" + taskId + ") finished processing " + processedCount.get() + "/" + totalToProcess + " players" + (duration != null ? " in " + formatDuration(duration) : "."));
             } else {
                 // Keep INFO for final stopped/cancelled summary
                 plugin.log(Level.INFO, "Bulk import (" + taskId + ") stopped or cancelled after processing " + processedCount.get() + "/" + totalToProcess + " players.");
             }

             plugin.clearBulkImportTaskReferences();

             try {
                 plugin.debug("StatsSyncTask (" + taskId + ") cleanupBulkImport attempting self-cancel..."); // Use debug
                 this.cancel(); // Calls our override
                 plugin.debug("StatsSyncTask (" + taskId + ") cleanupBulkImport self-cancel called."); // Use debug
             } catch (IllegalStateException e) {
                 plugin.debug("StatsSyncTask (" + taskId + ") cleanupBulkImport ignoring IllegalStateException during self-cancel, task likely already cancelled/finished by Bukkit."); // Use debug
             } catch (Exception e) {
                 plugin.log(Level.SEVERE, "StatsSyncTask (" + taskId + ") cleanupBulkImport Unexpected error during self-cancel!", e); // Keep SEVERE
             }
             // Keep INFO for overall end of cleanup
             plugin.log(Level.INFO, "StatsSyncTask (" + taskId + ") cleanupBulkImport finished.");
         } else {
             plugin.debug("StatsSyncTask (" + taskId + ") cleanupBulkImport called but task was already marked as not running (isRunning=false)."); // Use debug
         }
    }

    @Override
    public synchronized void cancel() throws IllegalStateException {
        int taskId = -1; try { taskId = getTaskId(); } catch (IllegalStateException ignored) {}
        plugin.debug("StatsSyncTask (" + taskId + ") cancel() method invoked."); // Use debug

        try {
            if (!super.isCancelled()) {
                super.cancel();
                 plugin.debug("StatsSyncTask (" + taskId + ") cancel(): Called super.cancel()."); // Use debug
            } else {
                 plugin.debug("StatsSyncTask (" + taskId + ") cancel(): super.isCancelled() was already true."); // Use debug
            }
        } catch (IllegalStateException e) {
            plugin.debug("StatsSyncTask (" + taskId + ") cancel(): Caught IllegalStateException from super.isCancelled/cancel(), task likely already finished/cancelled by Bukkit."); // Use debug
        } finally {
            if (isBulkImport) {
                plugin.debug("StatsSyncTask (" + taskId + ") cancel(): Triggering cleanupBulkImport(false)."); // Use debug
                cleanupBulkImport(false, taskId);
            }
        }
    }

    private boolean isBukkitTaskCancelled(int taskId) {
        try {
            boolean cancelled = super.isCancelled();
            // plugin.debug("isBukkitTaskCancelled (" + taskId + "): super.isCancelled() returned " + cancelled); // Optional debug
            return cancelled;
        } catch (IllegalStateException e) {
            // Keep WARNING for scheduler state issues
            plugin.log(Level.WARNING,"isBukkitTaskCancelled (" + taskId + "): Caught IllegalStateException from super.isCancelled(). Task is likely finished or already cancelled.");
            return true;
        }
    }

    public boolean isTaskRunning() {
        return isBulkImport && isRunning.get();
    }

    private String formatDuration(Duration duration) {
        if (duration == null) return "N/A";
        long seconds = duration.toSeconds();
        long absSeconds = Math.abs(seconds);
        String positive = String.format(
                "%dh %02dm %02ds",
                absSeconds / 3600,
                (absSeconds % 3600) / 60,
                absSeconds % 60);
        return seconds < 0 ? "-" + positive : positive;
    }
}