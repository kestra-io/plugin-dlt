package io.kestra.plugin.dlt;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.runners.TargetOS;
import io.kestra.core.runners.FilesService;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.scripts.exec.AbstractExecScript;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(title = "Run a dlt pipeline from Python code.")
@Plugin(
    examples = {
        @Example(
            title = "Load a local CSV file and ingest it into DuckDB, then query it.",
            full = true,
            code = """
                id: dlt_csv_to_duckdb
                namespace: company.team

                tasks:
                  - id: run_csv
                    type: io.kestra.plugin.dlt.Run
                    beforeCommands:
                      - echo "id,name,score" > data.csv
                      - echo "1,Alice,90" >> data.csv
                      - echo "2,Bob,85" >> data.csv
                    outputFiles:
                      - "csv_pipeline.duckdb"
                    script: |
                      import dlt
                      import pandas as pd

                      df = pd.read_csv("data.csv")

                      pipeline = dlt.pipeline(
                          pipeline_name="csv_pipeline",
                          destination="duckdb",
                          dataset_name="csv_data"
                      )

                      info = pipeline.run(df.to_dict(orient="records"), table_name="scores")
                      print(info)

                  - id: duckdb
                    type: io.kestra.plugin.jdbc.duckdb.Query
                    databaseUri: "{{outputs.run_csv.outputFiles['csv_pipeline.duckdb']}}"
                    sql: SELECT * FROM csv_data.scores;
                    store: true
                """
        ),
        @Example(
            title = "Run a dlt pipeline to extract data from a Zendesk API and load it into a DuckDB database.",
            full = true,
            code = """
                id: run_script
                namespace: company.team

                tasks:
                  - id: load_zendesk
                    type: io.kestra.plugin.dlt.Run
                    beforeCommands:
                      - dlt --non-interactive init zendesk duckdb
                    env:
                      SOURCES__ZENDESK__ZENDESK_SUPPORT__CREDENTIALS__EMAIL: "{{ secret('ZENDESK_EMAIL') }}"
                      SOURCES__ZENDESK__ZENDESK_SUPPORT__CREDENTIALS__PASSWORD: "{{ secret('ZENDESK_PASSWORD') }}"
                      SOURCES__ZENDESK__ZENDESK_SUPPORT__CREDENTIALS__SUBDOMAIN: "{{ secret('ZENDESK_SUBDOMAIN') }}"
                    outputFiles:
                      - "zendesk_pipeline.duckdb"
                    script: |
                      import dlt
                      from zendesk import zendesk_support

                      pipeline = dlt.pipeline(
                          pipeline_name="zendesk_pipeline",
                          destination="duckdb",
                          dataset_name="zendesk_data"
                      )
                      load_info = pipeline.run(zendesk_support(load_all=False).tickets)
                      print(f"Loaded: {load_info}")
                """
        )        
    }
)
public class Run extends AbstractExecScript implements RunnableTask<ScriptOutput> {
    private static final String DEFAULT_IMAGE = "ghcr.io/kestra-io/dlt";

    @Builder.Default
    protected Property<String> containerImage = Property.ofValue(DEFAULT_IMAGE);

    @Schema(title = "Inline script to execute.")
    @NotNull
    protected Property<String> script;

    @Override
    protected DockerOptions injectDefaults(RunContext runContext, DockerOptions original)
        throws IllegalVariableEvaluationException {
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
        CommandsWrapper commands = this.commands(runContext);

        Map<String, String> inputFiles = FilesService.inputFiles(
            runContext,
            commands.getTaskRunner().additionalVars(runContext, commands),
            this.getInputFiles()
        );

        Path relativeScriptPath = runContext.workingDir().path()
            .relativize(runContext.workingDir().createTempFile(".py"));

        inputFiles.put(
            relativeScriptPath.toString(),
            commands.render(runContext, this.script)
        );
        commands = commands.withInputFiles(inputFiles);

        TargetOS os = runContext.render(this.targetOS).as(TargetOS.class).orElse(null);

        return commands
            .withInterpreter(this.interpreter)
            .withBeforeCommands(beforeCommands)
            .withBeforeCommandsWithOptions(true)
            .withCommands(Property.ofValue(List.of(
                "python " + commands.getTaskRunner()
                    .toAbsolutePath(runContext, commands, relativeScriptPath.toString(), os)
            )))
            .withTargetOS(os)
            .run();
    }

}
