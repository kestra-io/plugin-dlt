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
    title = "Execute dlt (data load tool) commands to extract, transform and load data from various sources.",
    description = "dlt is an open-source Python library that loads data from various, often messy data sources into well-structured, live datasets. " +
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
            title = "Load a local JSON file into DuckDB",
            full = true,
            code = """
                id: dlt_local_duckdb_query
                namespace: company.team

                tasks:
                  - id: working_dir
                    type: io.kestra.plugin.core.flow.WorkingDirectory

                    tasks:
                      - id: init_local
                        type: io.kestra.plugin.dlt.DltCLI
                        commands:
                          - dlt init local_file duckdb
                          - echo '[{"name":"Alice","age":30},{"name":"Bob","age":25},{"name":"Alice","age":35}]' > data.json

                      - id: run_local
                        type: io.kestra.plugin.dlt.DltCLI
                        beforeCommands:
                          - pip install pandas pyarrow fastparquet sqlalchemy pymysql
                        commands:
                          - python local_file_pipeline.py
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