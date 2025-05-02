package com.rafalohaki.statsexporter.utils;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.rafalohaki.statsexporter.StatsExporterPlugin;
import org.bukkit.Bukkit;
import org.bukkit.OfflinePlayer; // Needed for name lookup later
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
import java.util.concurrent.Executor; // <<< --- ADD THIS IMPORT --- <<<
import java.util.logging.Level;
import java.util.stream.Stream;

public class StatsFileReader {

    private final StatsExporterPlugin plugin;
    private final Gson gson;
    private Path statsDirectoryPath;
    private final Executor bukkitExecutor; // <<< --- ADD THIS FIELD --- <<<

    public StatsFileReader(StatsExporterPlugin plugin) {
        this.plugin = plugin;
        // Reuse Gson instance from HttpUtils or create a new one
        // For simplicity here, creating a new one:
        this.gson = new Gson();
        // Initialize bukkitExecutor <<< --- ADD THIS INITIALIZATION --- <<<
        this.bukkitExecutor = runnable -> Bukkit.getScheduler().runTaskAsynchronously(plugin, runnable);
        findStatsDirectory();
    }

    /**
     * Attempts to find the primary world's statistics directory.
     * Logs an error if it can't be found.
     */
    private void findStatsDirectory() {
        World primaryWorld = Bukkit.getWorlds().get(0); // Get the default world (usually 'world')
        if (primaryWorld == null) {
            plugin.log(Level.SEVERE, "Could not find the primary server world to locate the stats directory!");
            this.statsDirectoryPath = null;
            return;
        }
        File worldFolder = primaryWorld.getWorldFolder();
        this.statsDirectoryPath = Paths.get(worldFolder.getAbsolutePath(), "stats");

        if (!Files.isDirectory(this.statsDirectoryPath)) {
            plugin.log(Level.WARNING, "Stats directory not found at: " + this.statsDirectoryPath + ". It might be created later or statistics are disabled.");
            // It might not exist if no player has stats yet, so don't treat as fatal error
        } else {
             plugin.log(Level.INFO, "Stats directory located at: " + this.statsDirectoryPath);
        }
    }

    /**
     * Asynchronously reads and parses the statistics file for a given player UUID.
     *
     * @param playerUuid The UUID of the player whose stats to read.
     * @return A CompletableFuture containing a Map of the parsed statistics,
     *         or an empty map if the file doesn't exist or an error occurs.
     *         The map structure reflects the JSON file (e.g., {"stats": {...}, "DataVersion": ...}).
     */
    public CompletableFuture<Map<String, Object>> readStatsAsync(UUID playerUuid) {
        if (statsDirectoryPath == null) {
            plugin.log(Level.SEVERE, "Stats directory path is not set, cannot read stats for " + playerUuid);
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }

        Path statFilePath = statsDirectoryPath.resolve(playerUuid.toString() + ".json");

        // Use Bukkit's async scheduler via CompletableFuture.supplyAsync executor argument
        // Executor bukkitExecutor = runnable -> Bukkit.getScheduler().runTaskAsynchronously(plugin, runnable); // <<<--- REMOVE LOCAL VARIABLE (now a field)

        return CompletableFuture.supplyAsync(() -> {
            plugin.debug("Attempting to read stats file: " + statFilePath);
            if (!Files.exists(statFilePath)) {
                plugin.debug("Stats file not found for UUID: " + playerUuid);
                return Collections.<String, Object>emptyMap(); // Return empty map if file doesn't exist
            }

            try (BufferedReader reader = Files.newBufferedReader(statFilePath, StandardCharsets.UTF_8)) {
                // Define the type for Gson to parse into (Map<String, Object>)
                Type mapType = new TypeToken<Map<String, Object>>() {}.getType();
                Map<String, Object> statsMap = gson.fromJson(reader, mapType);
                plugin.debug("Successfully parsed stats for UUID: " + playerUuid);
                return statsMap != null ? statsMap : Collections.emptyMap(); // Ensure we don't return null

            } catch (NoSuchFileException e) {
                 plugin.debug("Stats file disappeared before reading for UUID: " + playerUuid);
                 return Collections.<String, Object>emptyMap();
            } catch (JsonSyntaxException e) {
                plugin.log(Level.WARNING, "Failed to parse JSON stats file for UUID " + playerUuid + ": " + e.getMessage());
                return Collections.<String, Object>emptyMap(); // Return empty on parse error
            } catch (IOException e) {
                plugin.log(Level.WARNING, "Failed to read stats file for UUID " + playerUuid + ": " + e.getMessage());
                return Collections.<String, Object>emptyMap(); // Return empty on IO error
            } catch (Exception e) {
                 plugin.log(Level.SEVERE, "An unexpected error occurred reading stats for UUID " + playerUuid + ": " + e.getMessage(), e);
                 return Collections.<String, Object>emptyMap();
            }
        }, bukkitExecutor); // Execute the reading/parsing asynchronously
    }

    /**
     * Asynchronously reads stats for a player and formats it into a map suitable
     * for sending via HttpUtils. Includes UUID and potentially the player's name.
     *
     * @param player The OfflinePlayer (can be online or offline)
     * @return A CompletableFuture containing a map like {"uuid": "...", "name": "...", "stats": {...}},
     *         or an empty map if reading fails.
     */
     public CompletableFuture<Map<String, Object>> getPlayerDataMapAsync(OfflinePlayer player) {
         UUID uuid = player.getUniqueId();
         String name = player.getName(); // Can be null if player hasn't joined / profile unavailable

         return readStatsAsync(uuid).thenApplyAsync(stats -> {
             if (stats.isEmpty()) {
                 plugin.debug("No stats found for " + uuid + (name != null ? " (" + name + ")" : ""));
                 return Collections.<String, Object>emptyMap();
             }

             Map<String, Object> playerData = new HashMap<>();
             playerData.put("uuid", uuid.toString());
             // Include name if available, your API endpoint might want this
             if (name != null) {
                playerData.put("name", name);
             }
             // Embed the actual stats under a "stats" key
             playerData.put("stats", stats);

             plugin.debug("Prepared player data map for " + uuid + (name != null ? " (" + name + ")" : ""));
             return playerData;
         }, bukkitExecutor); // Use async executor for the mapping step too, just in case <<<--- USE THE FIELD
     }


     /**
     * Gets a stream of all UUIDs found in the stats directory.
     * Useful for the bulk import command. This operation itself is relatively fast
     * as it only lists files, but processing the stream should be done carefully.
     *
     * @return A Stream of UUIDs corresponding to .json files in the stats directory,
     *         or an empty stream if the directory is not accessible or empty.
     */
    public Stream<UUID> getAllPlayerUUIDsInStatsDir() {
        if (statsDirectoryPath == null || !Files.isDirectory(statsDirectoryPath)) {
            plugin.log(Level.WARNING, "Cannot list player UUIDs: Stats directory is not accessible.");
            return Stream.empty();
        }
        try {
            return Files.list(statsDirectoryPath) // Stream<Path>
                    .filter(path -> path.toString().endsWith(".json") && Files.isRegularFile(path))
                    .map(path -> path.getFileName().toString().replace(".json", "")) // Get filename without extension
                    .map(uuidString -> { // Try to parse as UUID
                        try {
                            return UUID.fromString(uuidString);
                        } catch (IllegalArgumentException e) {
                            plugin.debug("Skipping non-UUID file in stats dir: " + uuidString + ".json");
                            return null; // Filter out invalid UUIDs
                        }
                    })
                    .filter(uuid -> uuid != null); // Remove nulls from failed parsing
        } catch (IOException e) {
            plugin.log(Level.SEVERE, "Failed to list files in stats directory: " + statsDirectoryPath, e);
            return Stream.empty();
        }
    }
}