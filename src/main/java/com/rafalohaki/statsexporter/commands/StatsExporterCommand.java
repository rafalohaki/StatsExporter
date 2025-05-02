package com.rafalohaki.statsexporter.commands;

import com.rafalohaki.statsexporter.StatsExporterPlugin;
import com.rafalohaki.statsexporter.tasks.StatsSyncTask;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.format.NamedTextColor;
import net.kyori.adventure.text.format.TextDecoration; // Import needed
import org.bukkit.Bukkit;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.scheduler.BukkitTask;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.stream.Collectors;

public class StatsExporterCommand implements CommandExecutor {

    private final StatsExporterPlugin plugin;
    private final Component prefix;

    public StatsExporterCommand(StatsExporterPlugin plugin) {
        this.plugin = plugin;
        this.prefix = Component.text()
            .append(Component.text("[", NamedTextColor.GRAY))
            .append(Component.text("StatsExporter", NamedTextColor.GOLD))
            .append(Component.text("] ", NamedTextColor.GRAY))
            .color(NamedTextColor.YELLOW)
            .build();
    }

    @Override
    public boolean onCommand(@NotNull CommandSender sender, @NotNull Command command, @NotNull String label, @NotNull String[] args) {
        if (!sender.hasPermission("statsexporter.admin")) {
            sender.sendMessage(Component.text("You do not have permission to use this command.", NamedTextColor.RED));
            return true;
        }

        if (args.length == 0) {
            sendUsage(sender, label);
            return true;
        }

        String subCommand = args[0].toLowerCase();

        switch (subCommand) {
            case "importvanilla":
                handleImportVanilla(sender);
                break;
            case "reload":
                handleReload(sender);
                break;
            case "status":
                handleStatus(sender);
                break;
            case "cancelimport":
                 handleCancelImport(sender);
                 break;
            default:
                sender.sendMessage(prefix.append(Component.text("Unknown subcommand: " + args[0], NamedTextColor.RED)));
                sendUsage(sender, label);
                break;
        }

        return true;
    }

    private void sendUsage(CommandSender sender, String commandLabel) {
        sender.sendMessage(prefix.append(Component.text("Usage:")));
        sender.sendMessage(Component.text()
                .append(Component.text("/" + commandLabel + " importvanilla", NamedTextColor.GOLD))
                .append(Component.text(" - Starts bulk import from vanilla stats files.", NamedTextColor.GRAY)));
        sender.sendMessage(Component.text()
                .append(Component.text("/" + commandLabel + " cancelimport", NamedTextColor.GOLD))
                .append(Component.text(" - Stops the current bulk import task.", NamedTextColor.GRAY)));
        sender.sendMessage(Component.text()
                .append(Component.text("/" + commandLabel + " reload", NamedTextColor.GOLD))
                .append(Component.text(" - Reloads the plugin configuration.", NamedTextColor.GRAY)));
        sender.sendMessage(Component.text()
                .append(Component.text("/" + commandLabel + " status", NamedTextColor.GOLD))
                .append(Component.text(" - Shows current plugin status.", NamedTextColor.GRAY)));
    }

    private void handleImportVanilla(CommandSender sender) {
        if (plugin.isBulkImportRunning()) {
            sender.sendMessage(prefix.append(Component.text("A bulk import task is already running. Use /" + "statsexporter" + " cancelimport to stop it first.", NamedTextColor.RED)));
            return;
        }

        sender.sendMessage(prefix.append(Component.text("Starting vanilla stats bulk import...")));
        plugin.debug("Initiating bulk import process via command...");

        List<UUID> uuidsToImport;
        try {
             uuidsToImport = plugin.getStatsFileReader().getAllPlayerUUIDsInStatsDir().collect(Collectors.toList());
        } catch (Exception e) {
             plugin.log(Level.SEVERE, "Failed to list UUIDs from stats directory during import command.", e);
             sender.sendMessage(prefix.append(Component.text("Error listing player files. Check console for details.", NamedTextColor.RED)));
             return;
        }

        if (uuidsToImport.isEmpty()) {
            sender.sendMessage(prefix.append(Component.text("No player statistic files found in the stats directory.")));
            plugin.debug("Bulk import command found no UUIDs to process.");
            return;
        }

        int totalFiles = uuidsToImport.size();
        sender.sendMessage(prefix.append(Component.text("Found "))
            .append(Component.text(totalFiles, NamedTextColor.GOLD))
            .append(Component.text(" player stat files to process.")));
        plugin.log(Level.INFO, "Starting bulk import of " + totalFiles + " player stats files, requested by " + sender.getName());

        long playersPerTick = plugin.getPluginConfig().getLong("import.playersPerTick", 2);
        long delayTicks = plugin.getPluginConfig().getLong("import.delayBetweenPlayersTicks", 0);
        // Ensure period is at least 1 tick for runTaskTimerAsynchronously
        // If configured delay is 0, use a period of 1 tick. Otherwise use the configured delay (but min 1).
        long period = Math.max(1L, delayTicks > 0 ? delayTicks : 1L);

        try {
            // Create the BukkitRunnable instance
            StatsSyncTask bulkImportRunnable = new StatsSyncTask(plugin, uuidsToImport);

            // *** CORRECTED SCHEDULING ***
            // Schedule the task using its *own* method, not the general scheduler method.
            // This correctly initializes the BukkitRunnable's internal state.
            BukkitTask task = bulkImportRunnable.runTaskTimerAsynchronously(
                    plugin,       // The plugin instance
                    20L,          // Initial delay in ticks (1 second)
                    period        // Repeat period in ticks
            );
            // ***************************

            // Store references to the runnable and the task for management (cancel, status)
            plugin.setBulkImportTask(bulkImportRunnable, task);

            sender.sendMessage(prefix.append(Component.text("Bulk import task scheduled. Processing approx. " +
                               playersPerTick + " players every " + period + " ticks.")));
            sender.sendMessage(prefix.append(Component.text("Use "))
                .append(Component.text("/" + "statsexporter" + " cancelimport", NamedTextColor.GOLD))
                .append(Component.text(" to stop.")));

        } catch (IllegalStateException e) {
             // This might catch "Already scheduled" if something went wrong with the isBulkImportRunning check,
             // or potentially other scheduler issues.
             plugin.log(Level.SEVERE, "Failed to schedule bulk import task! (IllegalStateException). Check if it was already running.", e);
             sender.sendMessage(prefix.append(Component.text("Error scheduling bulk import task (IllegalStateException). Check console.", NamedTextColor.RED)));
             plugin.clearBulkImportTaskReferences(); // Attempt cleanup
        } catch (Exception e) {
            // Catch any other unexpected exceptions during scheduling
            plugin.log(Level.SEVERE, "An unexpected error occurred while scheduling the bulk import task!", e);
            sender.sendMessage(prefix.append(Component.text("Error scheduling bulk import task. Check console.", NamedTextColor.RED)));
            plugin.clearBulkImportTaskReferences();
        }
    }

     private void handleCancelImport(CommandSender sender) {
        if (!plugin.isBulkImportRunning()) {
            sender.sendMessage(prefix.append(Component.text("No bulk import task is currently running.", NamedTextColor.RED)));
            return;
        }

        sender.sendMessage(prefix.append(Component.text("Attempting to cancel the bulk import task...")));
        plugin.cancelBulkImportTask(); // This now calls the cancel() override in StatsSyncTask

        // Give the asynchronous cancellation a moment to process and update state
        Bukkit.getScheduler().runTaskLater(plugin, () -> {
            if (!plugin.isBulkImportRunning()) {
                sender.sendMessage(prefix.append(Component.text("Bulk import task cancelled successfully.", NamedTextColor.GREEN)));
                plugin.log(Level.INFO, "Bulk import task cancelled by " + sender.getName());
            } else {
                // If it's still considered running, cancellation might have failed or is still processing.
                sender.sendMessage(prefix.append(Component.text("Failed to confirm bulk import task cancellation immediately. It might finish shortly or an error occurred.", NamedTextColor.RED)));
                plugin.log(Level.WARNING, "Bulk import cancellation requested by " + sender.getName() + ", but isBulkImportRunning() is still true after a short delay.");
            }
        }, 5L); // Using a slightly longer delay (5 ticks = 0.25s) just in case
    }

    private void handleReload(CommandSender sender) {
        sender.sendMessage(prefix.append(Component.text("Reloading configuration...")));
        plugin.reloadConfig();
        // Update internal plugin state if needed based on new config values.
        // Note: Critical changes like API URL/Key might still require a full plugin restart/reload
        // for components like OkHttpClient to reliably pick them up.
        sender.sendMessage(prefix.append(Component.text("Configuration reloaded.", NamedTextColor.GREEN)));
        sender.sendMessage(prefix.append(Component.text("Note: API URL/Key changes usually require a plugin restart (/plugman reload StatsExporter or server restart).", NamedTextColor.YELLOW)));
        plugin.log(Level.INFO, "Configuration reloaded via command by " + sender.getName());
    }

    private void handleStatus(CommandSender sender) {
         sender.sendMessage(prefix.append(Component.text("StatsExporter Status:").decorate(TextDecoration.UNDERLINED)));

         sender.sendMessage(Component.text(" Version: ", NamedTextColor.YELLOW)
             .append(Component.text(plugin.getPluginMeta().getVersion(), NamedTextColor.WHITE)));
         sender.sendMessage(Component.text(" API Endpoint: ", NamedTextColor.YELLOW)
             .append(Component.text(plugin.getPluginConfig().getString("api.url", "Not Set"), NamedTextColor.WHITE)));
         sender.sendMessage(Component.text(" Debug Mode: ", NamedTextColor.YELLOW)
             .append(Component.text(plugin.getPluginConfig().getBoolean("debug", false), NamedTextColor.WHITE)));
         sender.sendMessage(Component.text(" Sync on Quit: ", NamedTextColor.YELLOW)
             .append(Component.text(plugin.getPluginConfig().getBoolean("sync.onQuit", true), NamedTextColor.WHITE)));

         boolean periodicEnabled = plugin.getPluginConfig().getBoolean("sync.periodicOnlineSync", true);
         long periodicInterval = plugin.getPluginConfig().getLong("sync.periodicOnlineSyncIntervalMinutes", 20);
         sender.sendMessage(Component.text(" Periodic Sync: ", NamedTextColor.YELLOW)
             .append(Component.text(periodicEnabled, NamedTextColor.WHITE))
             .append(periodicEnabled ? Component.text(" (" + periodicInterval + " min interval)", NamedTextColor.GRAY) : Component.empty()));

         boolean importRunning = plugin.isBulkImportRunning();
         sender.sendMessage(Component.text(" Bulk Import Active: ", NamedTextColor.YELLOW)
             .append(importRunning ? Component.text("Yes", NamedTextColor.GREEN) : Component.text("No", NamedTextColor.RED)));

         if (plugin.getHttpUtils() != null) {
            sender.sendMessage(Component.text(" Current Upload Queue Size: ", NamedTextColor.YELLOW)
                .append(Component.text(plugin.getHttpUtils().getQueueSize(), NamedTextColor.WHITE)));
         } else {
             // HttpUtils might be null if plugin failed during startup before it was initialized
             sender.sendMessage(Component.text(" Current Upload Queue Size: ", NamedTextColor.YELLOW)
                .append(Component.text("N/A (HttpUtils not initialized)", NamedTextColor.GRAY)));
         }

         sender.sendMessage(Component.text("--------------------", NamedTextColor.GRAY));
    }
}