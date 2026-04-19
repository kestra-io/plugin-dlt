# Kestra dlt Plugin

## What

- Provides plugin components under `io.kestra.plugin.dlt`.
- Includes classes such as `CLI`, `Run`.

## Why

- What user problem does this solve? Teams need to run dlt (data load tool) pipelines and CLI commands to move data into analytics destinations from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps dlt steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on dlt.

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
