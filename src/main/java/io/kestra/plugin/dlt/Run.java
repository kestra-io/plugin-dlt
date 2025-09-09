package io.kestra.plugin.dlt;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.*;
import io.kestra.core.models.tasks.runners.TargetOS;
import io.kestra.core.runners.FilesService;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.scripts.exec.AbstractExecScript;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Run a dlt pipeline from Python code."
)
@Plugin(
    examples = {
        @Example(
            title = "Run a dlt Zendesk pipeline to DuckDB",
            full = true,
            code = """
                id: dlt_zendesk_pipeline
                namespace: company.team

                tasks:
                  - id: load_zendesk
                    type: io.kestra.plugin.dlt.Run
                    env:
                      SOURCES__ZENDESK__ZENDESK_SUPPORT__CREDENTIALS__EMAIL: "{{ secret('ZENDESK_EMAIL') }}"
                      SOURCES__ZENDESK__ZENDESK_SUPPORT__CREDENTIALS__PASSWORD: "{{ secret('ZENDESK_PASSWORD') }}"
                      SOURCES__ZENDESK__ZENDESK_SUPPORT__CREDENTIALS__SUBDOMAIN: "{{ secret('ZENDESK_SUBDOMAIN') }}"
                    installExtras:
                      - zendesk
                      - duckdb
                    script: |
                      import dlt
                      from zendesk import zendesk_support

                      # Create pipeline
                      pipeline = dlt.pipeline(
                          pipeline_name="zendesk_pipeline",
                          destination="duckdb",
                          dataset_name="zendesk_data"
                      )

                      # Load tickets
                      load_info = pipeline.run(zendesk_support(load_all=False).tickets)
                      print(f"Load info: {load_info}")
                """
        ),
        @Example(
            title = "Run from Python file",
            full = true,
            code = """
                id: dlt_from_file
                namespace: company.team

                tasks:
                  - id: run_pipeline
                    type: io.kestra.plugin.dlt.Run
                    inputFiles:
                      pipeline.py: |
                        import dlt

                        @dlt.resource
                        def example_data():
                            yield [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

                        pipeline = dlt.pipeline("example", destination="duckdb")
                        load_info = pipeline.run(example_data())
                        print(f"Loaded: {load_info}")
                    file: pipeline.py
                """
        ),
        @Example(
            title = "Run Python module",
            full = true,
            code = """
                id: dlt_from_module
                namespace: company.team

                tasks:
                  - id: run_module
                    type: io.kestra.plugin.dlt.Run
                    module: my_pipeline_module
                    installPackages:
                      - requests
                      - pandas
                """
        )
    }
)
public class Run extends AbstractExecScript implements RunnableTask<ScriptOutput>, NamespaceFilesInterface, InputFilesInterface, OutputFilesInterface {
    private static final String DEFAULT_IMAGE = "ghcr.io/kestra-io/dlt";

    @Schema(
        title = "Inline Python script to execute."
    )
    protected Property<String> script;

    @Schema(
        title = "Optional Python file path relative to working directory."
    )
    protected Property<String> file;

    @Schema(
        title = "Optional Python module to run."
    )
    protected Property<String> module;

    @Builder.Default
    protected Property<String> containerImage = Property.ofValue(DEFAULT_IMAGE);

    @Schema(
        title = "Python extras to install (e.g., duckdb, bigquery, postgres).",
        description = "These will be installed as dlt[extra1,extra2,...]"
    )
    protected Property<List<String>> installExtras;

    @Schema(
        title = "Additional pip packages to install."
    )
    protected Property<List<String>> installPackages;

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
        CommandsWrapper commands = this.commands(runContext);

        Map<String, String> inputFiles = FilesService.inputFiles(runContext, commands.getTaskRunner().additionalVars(runContext, commands), this.getInputFiles());

        if (this.script != null) {
            Path relativeScriptPath = runContext.workingDir().path()
                .relativize(runContext.workingDir().createTempFile(".py"));
            inputFiles.put(
                relativeScriptPath.toString(),
                commands.render(runContext, this.script)
            );
        }

        commands = commands.withInputFiles(inputFiles);

        List<String> installCmds = new ArrayList<>();

        var extras = runContext.render(this.installExtras).asList(String.class);
        if (!extras.isEmpty()) {
            installCmds.add("pip install --no-cache-dir \"dlt[" + String.join(",", extras) + "]\"");
        }

        var packages = runContext.render(this.installPackages).asList(String.class);
        if (!packages.isEmpty()) {
            installCmds.add("pip install --no-cache-dir " + String.join(" ", packages));
        }

        String runCmd;
        if (this.script != null) {
            String scriptBody = runContext.render(this.script).as(String.class).orElseThrow();
            runCmd = "python - <<'SCRIPT_EOF'\n" + scriptBody.trim() + "\nSCRIPT_EOF";
        } else if (this.file != null) {
            String filePath = runContext.render(this.file).as(String.class).orElseThrow();
            runCmd = "python " + filePath;
        } else if (this.module != null) {
            String moduleName = runContext.render(this.module).as(String.class).orElseThrow();
            runCmd = "python -m " + moduleName;
        } else {
            throw new IllegalArgumentException("One of 'script', 'file', or 'module' must be provided.");
        }

        List<String> allCommands = new ArrayList<>();
        allCommands.addAll(installCmds);
        allCommands.add(runCmd);

        TargetOS os = runContext.render(this.targetOS).as(TargetOS.class).orElse(null);

        return commands
            .withInterpreter(this.interpreter)
            .withBeforeCommands(beforeCommands)
            .withCommands(Property.ofValue(allCommands))
            .withBeforeCommandsWithOptions(true)
            .withTargetOS(os)
            .run();
    }
}