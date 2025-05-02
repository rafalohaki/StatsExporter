package com.rafalohaki.statsexporter.listeners;

import com.rafalohaki.statsexporter.StatsExporterPlugin;
import com.rafalohaki.statsexporter.utils.HttpUtils;
import com.rafalohaki.statsexporter.utils.StatsFileReader;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerQuitEvent;

import java.util.concurrent.Executor;
import java.util.logging.Level;

public class PlayerListener implements Listener {

    private final StatsExporterPlugin plugin;
    private final HttpUtils httpUtils;
    private final StatsFileReader statsFileReader;
    private final boolean syncOnQuit;
    // Bukkit async executor
    private final Executor bukkitExecutor;


    public PlayerListener(StatsExporterPlugin plugin) {
        this.plugin = plugin;
        this.httpUtils = plugin.getHttpUtils();
        this.statsFileReader = plugin.getStatsFileReader();
        this.syncOnQuit = plugin.getPluginConfig().getBoolean("sync.onQuit", true);
        this.bukkitExecutor = runnable -> Bukkit.getScheduler().runTaskAsynchronously(plugin, runnable);
    }

    // Listen on HIGH priority to try and catch stats just before player fully disconnects
    // Though file saving might happen slightly after anyway.
    @EventHandler(priority = EventPriority.HIGH)
    public void onPlayerQuit(PlayerQuitEvent event) {
        if (!syncOnQuit) {
            return; // Sync on quit is disabled in config
        }

        Player player = event.getPlayer();
        plugin.debug("Player " + player.getName() + " quit. Queueing stats sync.");

        // Asynchronously read stats and then queue them
        statsFileReader.getPlayerDataMapAsync(player)
            .thenAcceptAsync(playerDataMap -> { // Process the result asynchronously
                if (playerDataMap != null && !playerDataMap.isEmpty()) {
                    plugin.debug("Successfully read stats for quitting player " + player.getName() + ". Queueing for send.");
                    httpUtils.queuePlayerData(playerDataMap);
                } else {
                    plugin.debug("Stats map was empty for quitting player " + player.getName() + ". Nothing to queue.");
                }
            }, bukkitExecutor) // Ensure this callback also runs on Bukkit's async thread pool
            .exceptionally(ex -> { // Handle potential errors during async processing
                plugin.log(Level.WARNING, "Error processing stats for quitting player " + player.getName() + ": " + ex.getMessage(), ex);
                return null; // Required for exceptionally stage
            });
    }
}