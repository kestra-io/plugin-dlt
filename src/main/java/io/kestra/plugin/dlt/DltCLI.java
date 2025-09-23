package io.kestra.plugin.dlt;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.*;
import io.kestra.core.models.tasks.runners.TargetOS;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.scripts.exec.AbstractExecScript;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Execute dlt (data load tool) commands to extract and load data from various sources.",
    description = "dlt is an open-source Python library that loads data from various data sources into well-structured datasets. " +
        "It offers a lightweight interface for extracting data from REST APIs, SQL databases, cloud storage, Python data structures, and many more."
)
@Plugin(
    examples = {
        @Example(
            title = "Initialize a new dlt project",
            full = true,
            code = """
                id: dlt_init_project
                namespace: company.team

                tasks:
                  - id: init
                    type: io.kestra.plugin.dlt.DltCLI
                    commands:
                      - dlt init rest_api duckdb
                """
        ),
        @Example(
            title = "Run a parametrized dlt pipeline extracting data from a REST API, load it into a DuckDB database and then query it.",
            full = true,
            code = """
                id: dlt_rest_api_duckdb
                namespace: company.team

                inputs:
                  - id: pipeline_name
                    type: STRING
                    defaults: rest_api_pokemon

                  - id: dataset_name
                    type: STRING
                    defaults: pokemon

                  - id: resources
                    type: ARRAY
                    itemType: STRING
                    defaults: ["pokemon", "berry", "location"]

                tasks:
                  - id: run
                    type: io.kestra.plugin.dlt.DltCLI
                    beforeCommands:
                      - pip install dlt[duckdb]>=1.16.0
                    commands:
                      - python rest_api.py
                    outputFiles:
                      - "{{inputs.pipeline_name}}.duckdb"
                    inputFiles: 
                      rest_api.py: |
                        import dlt
                        from dlt.sources.rest_api import rest_api_source

                        def load_pokemon() -> None:
                            pipeline = dlt.pipeline(
                                pipeline_name="{{inputs.pipeline_name}}",
                                destination='duckdb',
                                dataset_name="{{inputs.dataset_name}}",
                            )
                            pokemon_source = rest_api_source(
                                {
                                    "client": {
                                        "base_url": "https://pokeapi.co/api/v2/",
                                        "paginator": "json_link",
                                    },
                                    "resource_defaults": {
                                        "endpoint": {
                                            "params": {
                                                "limit": 1000,
                                            },
                                        },
                                    },
                                    "resources": {{inputs.resources}},
                                }
                            )

                            load_info = pipeline.run(pokemon_source)
                            print(load_info)

                        if __name__ == "__main__":
                            load_pokemon()

                  - id: duckdb
                    type: io.kestra.plugin.jdbc.duckdb.Query
                    databaseUri: "{{ outputs.run.outputFiles[inputs.pipeline_name ~ '.duckdb']}}"
                    sql: SELECT distinct name, url FROM {{inputs.dataset_name}}.pokemon;
                    store: true
                """
        ),
        @Example(
            title = "Run and trace a Chess API pipeline",
            full = true,
            code = """
                id: dlt_chess_demo
                namespace: company.team

                tasks:
                  - id: working_dir
                    type: io.kestra.plugin.core.flow.WorkingDirectory

                    tasks:
                      - id: init_pipeline
                        type: io.kestra.plugin.dlt.DltCLI
                        commands:
                          - dlt init chess duckdb

                      - id: run_pipeline
                        type: io.kestra.plugin.dlt.DltCLI
                        commands:
                          - python chess_pipeline.py
                          - dlt pipeline chess_pipeline trace
                """
        )
    }
)
public class DltCLI extends AbstractExecScript implements RunnableTask<ScriptOutput>, NamespaceFilesInterface, InputFilesInterface, OutputFilesInterface {
    private static final String DEFAULT_IMAGE = "ghcr.io/kestra-io/dlt";

    @Schema(
        title = "The dlt commands to run.",
        description = "List of dlt CLI commands to execute. Common commands include 'dlt init', 'dlt pipeline', 'dlt deploy', etc."
    )
    @NotNull
    protected Property<List<String>> commands;

    @Builder.Default
    protected Property<String> containerImage = Property.ofValue(DEFAULT_IMAGE);

    @Override
    protected DockerOptions injectDefaults(RunContext runContext, DockerOptions original) throws IllegalVariableEvaluationException {
        var builder = original.toBuilder();
        if (original.getImage() == null) {
            builder.image(runContext.render(this.getContainerImage()).as(String.class).orElse(null));
        }
        if (original.getEntryPoint() == null || original.getEntryPoint().isEmpty()) {
            builder.entryPoint(List.of(""));
        }
        return builder.build();
    }

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        TargetOS os = runContext.render(this.targetOS).as(TargetOS.class).orElse(null);

        List<String> processedCommands = runContext.render(this.commands).asList(String.class).stream()
            .map(String::trim)
            .flatMap(command -> {
                List<String> out = new ArrayList<>();

                if (command.startsWith("dlt ") && !command.contains("--non-interactive")) {
                    command = command.replaceFirst("^dlt\\b", "dlt --non-interactive");
                }
                out.add(command);

                if (command.matches("^dlt --non-interactive\\s+init\\b.*")) {
                    out.add("if [ -f requirements.txt ]; then pip install -r requirements.txt; fi");
                }
                return out.stream();
            })
            .toList();

        return this.commands(runContext)
            .withInterpreter(this.interpreter)
            .withBeforeCommands(beforeCommands)
            .withBeforeCommandsWithOptions(true)
            .withCommands(Property.ofValue(processedCommands))
            .withTargetOS(os)
            .run();
    }
}