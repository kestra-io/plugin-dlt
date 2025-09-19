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
                id: dlt_local_file
                namespace: company.team

                tasks:
                  - id: working_dir
                    type: io.kestra.plugin.core.flow.WorkingDirectory
                    tasks:
                      - id: init_local
                        type: io.kestra.plugin.dlt.DltCLI
                        containerImage: ghcr.io/kestra-io/dlt-runtime:local
                        beforeCommands:
                          - pip install pandas sqlalchemy
                        commands:
                          - dlt --non-interactive init local_file duckdb
                          - echo '[{"name":"Alice","age":30},{"name":"Bob","age":25}]' > data.json

                      - id: run_local
                        type: io.kestra.plugin.dlt.DltCLI
                        containerImage: ghcr.io/kestra-io/dlt-runtime:local
                        beforeCommands:
                          - pip install pandas sqlalchemy
                        commands:
                          - python local_file_pipeline.py
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

        List<String> commands = runContext.render(this.commands).asList(String.class);
        List<String> processedCommands = new ArrayList<>();

        // Process each command
        for (String raw : commands) {
            String c = raw.trim();

            // Ensure top-level non-interactive for ALL dlt commands
            if (c.startsWith("dlt ") && !c.contains("--non-interactive")) {
                c = c.replaceFirst("^dlt\\b", "dlt --non-interactive");
            }
            processedCommands.add(c);

            // After 'dlt init ...', install generated deps if present
            if (c.matches("^dlt --non-interactive\\s+init\\b.*")) {
                processedCommands.add("if [ -f requirements.txt ]; then pip install -r requirements.txt; fi");
            }
        }

        return this.commands(runContext)
            .withInterpreter(this.interpreter)
            .withBeforeCommands(beforeCommands)
            .withBeforeCommandsWithOptions(true)
            .withCommands(Property.ofValue(processedCommands))
            .withTargetOS(os)
            .run();
    }
}