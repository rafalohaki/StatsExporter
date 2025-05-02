# StatsExporter

A PaperMC/Spigot Minecraft server plugin that exports player vanilla statistics to an external API endpoint.

## Features

- Exports player stats on player quit and periodically for online players
- Bulk import of all existing player stats via command
- Configurable batching and API endpoint
- Asynchronous HTTP requests with OkHttp
- Simple setup and configuration

## Installation

1. **Download** or build the plugin JAR.
2. **Place** the JAR in your server's `plugins` folder.
3. **Start** your server to generate default configuration files.
4. **Configure** the plugin by editing `plugins/StatsExporter/config.yml`:
    - Set your API endpoint (`api.url`)
    - Set your API key (`api.key`)
    - Adjust batch and sync settings as needed

## Commands

| Command                                | Description                                |
| --------------------------------------- | ------------------------------------------ |
| `/statsexporter importvanilla`          | Bulk import all vanilla stats to the API   |
| `/statsexporter cancelimport`           | Cancel a running bulk import task          |
| `/statsexporter reload`                 | Reload plugin configuration                |
| `/statsexporter status`                 | Show plugin status and configuration       |

Aliases: `/se`, `/sxp`

## Permissions

- `statsexporter.admin` - Required to use all plugin commands (default: OP)

## Configuration

See `config.yml` for all available options, including API endpoint, batching, and sync intervals.

## Building

This project uses Maven. To build:

`mvn clean package`


The resulting JAR will be in the `target/` directory.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
