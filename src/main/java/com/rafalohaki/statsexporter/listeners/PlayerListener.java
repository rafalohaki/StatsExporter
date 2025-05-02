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
    private final Executor bukkitExecutor;


    public PlayerListener(StatsExporterPlugin plugin) {
        this.plugin = plugin;
        this.httpUtils = plugin.getHttpUtils();
        this.statsFileReader = plugin.getStatsFileReader();
        this.syncOnQuit = plugin.getPluginConfig().getBoolean("sync.onQuit", true);
        this.bukkitExecutor = runnable -> Bukkit.getScheduler().runTaskAsynchronously(plugin, runnable);
    }

    @EventHandler(priority = EventPriority.HIGH)
    public void onPlayerQuit(PlayerQuitEvent event) {
        if (!syncOnQuit) {
            return;
        }

        Player player = event.getPlayer();
        plugin.debug("Player " + player.getName() + " quit. Queueing stats sync."); // Use debug

        statsFileReader.getPlayerDataMapAsync(player)
            .thenAcceptAsync(playerDataMap -> {
                if (playerDataMap != null && !playerDataMap.isEmpty()) {
                    plugin.debug("Successfully read stats for quitting player " + player.getName() + ". Queueing for send."); // Use debug
                    httpUtils.queuePlayerData(playerDataMap); // Internal logging is debug
                } else {
                    plugin.debug("Stats map was empty or null for quitting player " + player.getName() + ". Nothing to queue."); // Use debug
                }
            }, bukkitExecutor)
            .exceptionally(ex -> {
                // Keep WARNING for errors processing quit event
                plugin.log(Level.WARNING, "Error processing stats for quitting player " + player.getName(), ex);
                return null;
            });
    }
}