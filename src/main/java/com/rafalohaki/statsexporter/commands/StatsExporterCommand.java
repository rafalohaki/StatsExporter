package com.rafalohaki.statsexporter.commands;

import com.rafalohaki.statsexporter.StatsExporterPlugin;
import com.rafalohaki.statsexporter.tasks.StatsSyncTask;
import org.bukkit.Bukkit;
import org.bukkit.ChatColor;
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
    private final String prefix = ChatColor.GRAY + "[" + ChatColor.GOLD + "StatsExporter" + ChatColor.GRAY + "] " + ChatColor.YELLOW;

    public StatsExporterCommand(StatsExporterPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public boolean onCommand(@NotNull CommandSender sender, @NotNull Command command, @NotNull String label, @NotNull String[] args) {
        if (!sender.hasPermission("statsexporter.admin")) {
            sender.sendMessage(ChatColor.RED + "You do not have permission to use this command.");
            return true;
        }

        if (args.length == 0) {
            sendUsage(sender, label); // <<< --- Pass label here --- <<<
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
            // Add "cancelimport" subcommand
            case "cancelimport":
                 handleCancelImport(sender);
                 break;
            default:
                sender.sendMessage(prefix + ChatColor.RED + "Unknown subcommand: " + args[0]);
                sendUsage(sender, label); // <<< --- Pass label here --- <<<
                break;
        }

        return true;
    }

    // <<< --- Modify method signature to accept label --- <<<
    private void sendUsage(CommandSender sender, String commandLabel) {
        sender.sendMessage(prefix + "Usage:");
        // <<< --- Use commandLabel parameter instead of label --- <<<
        sender.sendMessage(ChatColor.GOLD + "/" + commandLabel + " importvanilla" + ChatColor.GRAY + " - Starts bulk import from vanilla stats files.");
        sender.sendMessage(ChatColor.GOLD + "/" + commandLabel + " cancelimport" + ChatColor.GRAY + " - Stops the current bulk import task.");
        sender.sendMessage(ChatColor.GOLD + "/" + commandLabel + " reload" + ChatColor.GRAY + " - Reloads the plugin configuration.");
        sender.sendMessage(ChatColor.GOLD + "/" + commandLabel + " status" + ChatColor.GRAY + " - Shows current plugin status.");
    }

    private void handleImportVanilla(CommandSender sender) {
        // Use the improved check from the main plugin class
        if (plugin.isBulkImportRunning()) {
            sender.sendMessage(prefix + ChatColor.RED + "A bulk import task is already running. Use /" + "statsexporter" + " cancelimport to stop it first."); // Consider using the command label/alias here too, but "statsexporter" is safe.
            return;
        }

        sender.sendMessage(prefix + "Starting vanilla stats bulk import...");
        plugin.debug("Initiating bulk import process via command...");

        List<UUID> uuidsToImport;
        try {
             uuidsToImport = plugin.getStatsFileReader().getAllPlayerUUIDsInStatsDir().collect(Collectors.toList());
        } catch (Exception e) {
             plugin.log(Level.SEVERE, "Failed to list UUIDs from stats directory during import command.", e);
             sender.sendMessage(prefix + ChatColor.RED + "Error listing player files. Check console for details.");
             return;
        }

        if (uuidsToImport.isEmpty()) {
            sender.sendMessage(prefix + ChatColor.YELLOW + "No player statistic files found in the stats directory.");
            plugin.debug("Bulk import command found no UUIDs to process.");
            return;
        }

        int totalFiles = uuidsToImport.size();
        sender.sendMessage(prefix + "Found " + ChatColor.GOLD + totalFiles + ChatColor.YELLOW + " player stat files to process.");
        plugin.log(Level.INFO, "Starting bulk import of " + totalFiles + " player stats files, requested by " + sender.getName());

        long playersPerTick = plugin.getPluginConfig().getLong("import.playersPerTick", 2);
        long delayTicks = plugin.getPluginConfig().getLong("import.delayBetweenPlayersTicks", 0);
        long period = Math.max(1L, delayTicks); // Ensure period is at least 1 tick

        try {
            StatsSyncTask bulkImportRunnable = new StatsSyncTask(plugin, uuidsToImport);

            BukkitTask task = Bukkit.getScheduler().runTaskTimerAsynchronously(
                    plugin,
                    bulkImportRunnable,
                    20L, // Start after 1 second delay
                    period
            );

            // Store references in the main plugin class
            plugin.setBulkImportTask(bulkImportRunnable, task);

            sender.sendMessage(prefix + "Bulk import task scheduled. Processing approx. " +
                               playersPerTick + " players every " + period + " ticks.");
            sender.sendMessage(prefix + "Use " + ChatColor.GOLD + "/" + "statsexporter" + " cancelimport" + ChatColor.YELLOW + " to stop."); // Consider using the command label/alias here too

        } catch (Exception e) {
            plugin.log(Level.SEVERE, "Failed to schedule bulk import task!", e);
            sender.sendMessage(prefix + ChatColor.RED + "Error scheduling bulk import task. Check console.");
            plugin.clearBulkImportTaskReferences(); // Ensure references are cleared on error
        }
    }

     private void handleCancelImport(CommandSender sender) {
        if (!plugin.isBulkImportRunning()) {
            sender.sendMessage(prefix + ChatColor.RED + "No bulk import task is currently running.");
            return;
        }

        sender.sendMessage(prefix + "Attempting to cancel the bulk import task...");
        plugin.cancelBulkImportTask(); // Add this method to StatsExporterPlugin

        // Check status again after a short delay (cancellation might take a tick)
        Bukkit.getScheduler().runTaskLater(plugin, () -> {
            if (!plugin.isBulkImportRunning()) {
                sender.sendMessage(prefix + ChatColor.GREEN + "Bulk import task cancelled successfully.");
                plugin.log(Level.INFO, "Bulk import task cancelled by " + sender.getName());
            } else {
                sender.sendMessage(prefix + ChatColor.RED + "Failed to cancel the bulk import task. It might have already finished or an error occurred.");
            }
        }, 2L); // Check after 2 ticks
    }


    private void handleReload(CommandSender sender) {
        sender.sendMessage(prefix + "Reloading configuration...");
        plugin.reloadConfig();
        // Reload config values that might be used by components directly
        // Note: API URL/Key changes in HttpUtils or OkHttp client settings usually need a restart/plugin reload
        sender.sendMessage(prefix + ChatColor.GREEN + "Configuration reloaded. Critical changes like API URL/Key may require a plugin restart (/plugman reload StatsExporter or server restart).");
        plugin.log(Level.INFO, "Configuration reloaded via command by " + sender.getName());
    }

    private void handleStatus(CommandSender sender) {
         sender.sendMessage(prefix + ChatColor.UNDERLINE + "StatsExporter Status:");
         sender.sendMessage(ChatColor.YELLOW + " Version: " + ChatColor.WHITE + plugin.getDescription().getVersion());
         sender.sendMessage(ChatColor.YELLOW + " API Endpoint: " + ChatColor.WHITE + plugin.getPluginConfig().getString("api.url"));
         sender.sendMessage(ChatColor.YELLOW + " Debug Mode: " + ChatColor.WHITE + plugin.getPluginConfig().getBoolean("debug", false));
         sender.sendMessage(ChatColor.YELLOW + " Sync on Quit: " + ChatColor.WHITE + plugin.getPluginConfig().getBoolean("sync.onQuit", true));

         boolean periodicEnabled = plugin.getPluginConfig().getBoolean("sync.periodicOnlineSync", true);
         sender.sendMessage(ChatColor.YELLOW + " Periodic Sync: " + ChatColor.WHITE + periodicEnabled +
                            (periodicEnabled ?
                             " (" + plugin.getPluginConfig().getLong("sync.periodicOnlineSyncIntervalMinutes", 20) + " min interval)" : ""));

         boolean importRunning = plugin.isBulkImportRunning();
         sender.sendMessage(ChatColor.YELLOW + " Bulk Import Active: " + (importRunning ? ChatColor.GREEN + "Yes" : ChatColor.RED + "No"));

         // Add queue size info from HttpUtils (needs a getter method)
         if (plugin.getHttpUtils() != null) {
            sender.sendMessage(ChatColor.YELLOW + " Current Upload Queue Size: " + ChatColor.WHITE + plugin.getHttpUtils().getQueueSize());
         }

         sender.sendMessage(ChatColor.GRAY + "--------------------");
    }
}