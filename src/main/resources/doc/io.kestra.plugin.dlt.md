# How to use the dlt plugin

Run dlt (data load tool) pipelines from Kestra flows to extract and load data across sources and destinations.

## Authentication

dlt pipelines authenticate to sources and destinations via environment variables. Pass credentials through the `env` map using dlt's nested variable convention (e.g. `SOURCES__<SOURCE>__<SECTION>__CREDENTIALS__<KEY>`). Store secrets in [secrets](https://kestra.io/docs/concepts/secret) and apply connection properties globally with [plugin defaults](https://kestra.io/docs/workflow-components/plugin-defaults).

## Tasks

`CLI` runs dlt CLI commands in a container (default image `ghcr.io/kestra-io/dlt`) — set `commands` (required, list of shell commands such as `dlt init`, `dlt pipeline`, or `dlt run`). Use `beforeCommands` for setup steps, `env` for credentials, `inputFiles` to stage local files, and `outputFiles` to retrieve results.

`Run` executes an inline Python dlt script — set `script` (required, a Python string that defines and runs the pipeline). Use `beforeCommands`, `env`, `inputFiles`, and `outputFiles` the same way as `CLI`. The default container image is also `ghcr.io/kestra-io/dlt`.
