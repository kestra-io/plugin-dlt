# Kestra dlt Plugin

## What

- Provides plugin components under `io.kestra.plugin.dlt`.
- Includes classes such as `CLI`, `Run`.

## Why

- This plugin integrates Kestra with dlt.
- It provides tasks that run dlt (data load tool) pipelines and CLI commands to move data into analytics destinations.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `dlt`

Infrastructure dependencies (Docker Compose services):

- `app`

### Key Plugin Classes

- `io.kestra.plugin.dlt.CLI`
- `io.kestra.plugin.dlt.Run`

### Project Structure

```
plugin-dlt/
├── src/main/java/io/kestra/plugin/dlt/
├── src/test/java/io/kestra/plugin/dlt/
├── build.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
