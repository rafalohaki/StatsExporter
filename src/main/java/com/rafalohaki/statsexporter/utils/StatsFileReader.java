package com.rafalohaki.statsexporter.utils;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.rafalohaki.statsexporter.StatsExporterPlugin;
import org.bukkit.Bukkit;
import org.bukkit.OfflinePlayer;
import org.bukkit.World;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.stream.Stream;

public class StatsFileReader {

    private final StatsExporterPlugin plugin;
    private final Gson gson;
    private Path statsDirectoryPath;
    private final Executor bukkitExecutor;

    public StatsFileReader(StatsExporterPlugin plugin) {
        this.plugin = plugin;
        this.gson = new Gson(); // Create a dedicated Gson instance
        this.bukkitExecutor = runnable -> Bukkit.getScheduler().runTaskAsynchronously(plugin, runnable);
        findStatsDirectory();
    }

    private void findStatsDirectory() {
        World primaryWorld = Bukkit.getWorlds().get(0);
        if (primaryWorld == null) {
            plugin.log(Level.SEVERE, "Could not find the primary server world to locate the stats directory!"); // Keep SEVERE
            this.statsDirectoryPath = null;
            return;
        }
        File worldFolder = primaryWorld.getWorldFolder();
        this.statsDirectoryPath = Paths.get(worldFolder.getAbsolutePath(), "stats");

        if (!Files.isDirectory(this.statsDirectoryPath)) {
            // Keep WARNING as it might indicate an issue or first run
            plugin.log(Level.WARNING, "Stats directory not found at: " + this.statsDirectoryPath + ". It might be created later or statistics are disabled.");
        } else {
             plugin.log(Level.INFO, "Stats directory located at: " + this.statsDirectoryPath); // Keep INFO for successful location
        }
    }

    public CompletableFuture<Map<String, Object>> readStatsAsync(UUID playerUuid) {
        if (statsDirectoryPath == null) {
            // Log only once or less frequently if this becomes spammy
            plugin.log(Level.WARNING, "Stats directory path is not set, cannot read stats for " + playerUuid); // Keep WARNING
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }
        if (playerUuid == null) {
             plugin.log(Level.WARNING, "Attempted to read stats for a null UUID."); // Keep WARNING
             return CompletableFuture.completedFuture(Collections.emptyMap());
        }

        Path statFilePath = statsDirectoryPath.resolve(playerUuid.toString() + ".json");

        return CompletableFuture.supplyAsync(() -> {
            plugin.debug("Attempting to read stats file: " + statFilePath); // Use debug
            if (!Files.exists(statFilePath)) {
                plugin.debug("Stats file not found for UUID: " + playerUuid); // Use debug
                return Collections.<String, Object>emptyMap();
            }

            try (BufferedReader reader = Files.newBufferedReader(statFilePath, StandardCharsets.UTF_8)) {
                Type mapType = new TypeToken<Map<String, Object>>() {}.getType();
                Map<String, Object> statsMap = gson.fromJson(reader, mapType);
                plugin.debug("Successfully parsed stats for UUID: " + playerUuid); // Use debug
                return statsMap != null ? statsMap : Collections.emptyMap();

            } catch (NoSuchFileException e) {
                 plugin.debug("Stats file disappeared before reading for UUID: " + playerUuid); // Use debug
                 return Collections.<String, Object>emptyMap();
            } catch (JsonSyntaxException e) {
                // Keep WARNING for parse errors
                plugin.log(Level.WARNING, "Failed to parse JSON stats file for UUID " + playerUuid + ": " + e.getMessage());
                return Collections.<String, Object>emptyMap();
            } catch (IOException e) {
                // Keep WARNING for read errors
                plugin.log(Level.WARNING, "Failed to read stats file for UUID " + playerUuid + ": " + e.getMessage());
                return Collections.<String, Object>emptyMap();
            } catch (Exception e) {
                 // Keep SEVERE for unexpected errors
                 plugin.log(Level.SEVERE, "An unexpected error occurred reading stats for UUID " + playerUuid, e);
                 return Collections.<String, Object>emptyMap();
            }
        }, bukkitExecutor);
    }

     public CompletableFuture<Map<String, Object>> getPlayerDataMapAsync(OfflinePlayer player) {
         if (player == null) {
             plugin.log(Level.WARNING, "Attempted to get player data map for a null OfflinePlayer."); // Keep WARNING
             return CompletableFuture.completedFuture(Collections.emptyMap());
         }
         UUID uuid = player.getUniqueId();
         String name = player.getName(); // Can be null

         return readStatsAsync(uuid).thenApplyAsync(stats -> {
             if (stats == null || stats.isEmpty()) { // Added null check for safety
                 plugin.debug("No stats found or read failed for " + uuid + (name != null ? " (" + name + ")" : "")); // Use debug
                 return Collections.<String, Object>emptyMap();
             }

             Map<String, Object> playerData = new HashMap<>();
             playerData.put("uuid", uuid.toString());
             if (name != null) {
                playerData.put("name", name);
             }
             playerData.put("stats", stats); // Embed the raw stats map

             plugin.debug("Prepared player data map for " + uuid + (name != null ? " (" + name + ")" : "")); // Use debug
             return playerData;
         }, bukkitExecutor);
     }

    public Stream<UUID> getAllPlayerUUIDsInStatsDir() {
        if (statsDirectoryPath == null || !Files.isDirectory(statsDirectoryPath)) {
            // Keep WARNING as this prevents bulk import
            plugin.log(Level.WARNING, "Cannot list player UUIDs: Stats directory is not accessible at " + statsDirectoryPath);
            return Stream.empty();
        }
        try {
            return Files.list(statsDirectoryPath)
                    .filter(path -> path.toString().endsWith(".json") && Files.isRegularFile(path))
                    .map(path -> path.getFileName().toString().replace(".json", ""))
                    .map(uuidString -> {
                        try {
                            return UUID.fromString(uuidString);
                        } catch (IllegalArgumentException e) {
                            // Use debug for skipping non-UUID files
                            plugin.debug("Skipping non-UUID file in stats dir: " + uuidString + ".json");
                            return null;
                        }
                    })
                    .filter(uuid -> uuid != null);
        } catch (IOException e) {
            // Keep SEVERE as this breaks bulk import
            plugin.log(Level.SEVERE, "Failed to list files in stats directory: " + statsDirectoryPath, e);
            return Stream.empty();
        }
    }
}