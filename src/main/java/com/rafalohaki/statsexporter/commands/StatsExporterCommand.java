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
        long period = Math.max(1L, delayTicks);

        try {
            StatsSyncTask bulkImportRunnable = new StatsSyncTask(plugin, uuidsToImport);

            // Use explicit Runnable to silence deprecation warning
            Runnable taskAsRunnable = bulkImportRunnable;
            BukkitTask task = Bukkit.getScheduler().runTaskTimerAsynchronously(
                    plugin,
                    taskAsRunnable, // Pass the Runnable
                    20L,            // Start after 1 second delay
                    period
            );

            plugin.setBulkImportTask(bulkImportRunnable, task);

            sender.sendMessage(prefix.append(Component.text("Bulk import task scheduled. Processing approx. " +
                               playersPerTick + " players every " + period + " ticks.")));
            sender.sendMessage(prefix.append(Component.text("Use "))
                .append(Component.text("/" + "statsexporter" + " cancelimport", NamedTextColor.GOLD))
                .append(Component.text(" to stop.")));

        } catch (Exception e) {
            plugin.log(Level.SEVERE, "Failed to schedule bulk import task!", e);
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
        plugin.cancelBulkImportTask();

        Bukkit.getScheduler().runTaskLater(plugin, () -> {
            if (!plugin.isBulkImportRunning()) {
                sender.sendMessage(prefix.append(Component.text("Bulk import task cancelled successfully.", NamedTextColor.GREEN)));
                plugin.log(Level.INFO, "Bulk import task cancelled by " + sender.getName());
            } else {
                sender.sendMessage(prefix.append(Component.text("Failed to cancel the bulk import task. It might have already finished or an error occurred.", NamedTextColor.RED)));
            }
        }, 2L);
    }

    private void handleReload(CommandSender sender) {
        sender.sendMessage(prefix.append(Component.text("Reloading configuration...")));
        plugin.reloadConfig();
        sender.sendMessage(prefix.append(Component.text("Configuration reloaded. Critical changes like API URL/Key may require a plugin restart (/plugman reload StatsExporter or server restart).", NamedTextColor.GREEN)));
        plugin.log(Level.INFO, "Configuration reloaded via command by " + sender.getName());
    }

    private void handleStatus(CommandSender sender) {
         // Correctly apply decoration using .decorate() and TextDecoration.UNDERLINED
         sender.sendMessage(prefix.append(Component.text("StatsExporter Status:").decorate(TextDecoration.UNDERLINED))); // <-- FIX APPLIED HERE

         sender.sendMessage(Component.text(" Version: ", NamedTextColor.YELLOW)
             .append(Component.text(plugin.getPluginMeta().getVersion(), NamedTextColor.WHITE)));
         sender.sendMessage(Component.text(" API Endpoint: ", NamedTextColor.YELLOW)
             .append(Component.text(plugin.getPluginConfig().getString("api.url", "Not Set"), NamedTextColor.WHITE)));
         sender.sendMessage(Component.text(" Debug Mode: ", NamedTextColor.YELLOW)
             .append(Component.text(plugin.getPluginConfig().getBoolean("debug", false), NamedTextColor.WHITE)));
         sender.sendMessage(Component.text(" Sync on Quit: ", NamedTextColor.YELLOW)
             .append(Component.text(plugin.getPluginConfig().getBoolean("sync.onQuit", true), NamedTextColor.WHITE)));

         boolean periodicEnabled = plugin.getPluginConfig().getBoolean("sync.periodicOnlineSync", true);
         sender.sendMessage(Component.text(" Periodic Sync: ", NamedTextColor.YELLOW)
             .append(Component.text(periodicEnabled, NamedTextColor.WHITE))
             .append(periodicEnabled ? Component.text(" (" + plugin.getPluginConfig().getLong("sync.periodicOnlineSyncIntervalMinutes", 20) + " min interval)", NamedTextColor.WHITE) : Component.empty()));

         boolean importRunning = plugin.isBulkImportRunning();
         sender.sendMessage(Component.text(" Bulk Import Active: ", NamedTextColor.YELLOW)
             .append(importRunning ? Component.text("Yes", NamedTextColor.GREEN) : Component.text("No", NamedTextColor.RED)));

         if (plugin.getHttpUtils() != null) {
            sender.sendMessage(Component.text(" Current Upload Queue Size: ", NamedTextColor.YELLOW)
                .append(Component.text(plugin.getHttpUtils().getQueueSize(), NamedTextColor.WHITE)));
         }

         sender.sendMessage(Component.text("--------------------", NamedTextColor.GRAY));
    }
}