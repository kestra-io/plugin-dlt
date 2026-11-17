package io.kestra.plugin.dlt;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.plugin.core.runner.Process;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.runner.docker.Docker;
import io.kestra.plugin.scripts.runner.docker.PullPolicy;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class DltDockerDefaultsTest {
    private static final List<String> SCRIPT = DltDockerDefaults.DEFAULT_IMAGE_ENTRYPOINT;

    @Inject
    RunContextFactory runContextFactory;

    private DockerOptions inject(DockerOptions original, String containerImage) throws Exception {
        return DltDockerDefaults.inject(runContextFactory.of(), original, Property.ofValue(containerImage));
    }

    @Test
    void defaultImageUsesEntrypointScript() throws Exception {
        var result = inject(DockerOptions.builder().build(), "ghcr.io/kestra-io/dlt");

        assertThat(result.getImage(), is("ghcr.io/kestra-io/dlt"));
        assertThat(result.getEntryPoint(), is(SCRIPT));
    }

    @Test
    void taggedDefaultImageUsesEntrypointScript() throws Exception {
        assertThat(inject(DockerOptions.builder().build(), "ghcr.io/kestra-io/dlt:latest").getEntryPoint(), is(SCRIPT));
    }

    @Test
    void imageSetOnRunnerIsTheOneChecked() throws Exception {
        var original = DockerOptions.builder().image("ghcr.io/kestra-io/dlt:1.0").build();

        assertThat(inject(original, "my/custom-image").getEntryPoint(), is(SCRIPT));
    }

    @Test
    void customImageKeepsClearedEntrypoint() throws Exception {
        assertThat(inject(DockerOptions.builder().build(), "my/dlt:latest").getEntryPoint(), is(List.of("")));
        assertThat(inject(DockerOptions.builder().build(), "ghcr.io/kestra-io/dlt-custom").getEntryPoint(), is(List.of("")));
    }

    @Test
    void userEntrypointIsPreserved() throws Exception {
        var original = DockerOptions.builder().entryPoint(List.of("/bin/custom")).build();

        assertThat(inject(original, "ghcr.io/kestra-io/dlt").getEntryPoint(), is(List.of("/bin/custom")));
    }

    private enum Kind { RUN, CLI }

    private CommandsWrapper commandsOf(Kind kind, TaskRunner<?> runner, String containerImage) throws Exception {
        var image = Property.ofValue(containerImage == null ? DltDockerDefaults.DEFAULT_IMAGE : containerImage);
        var runContext = runContextFactory.of();
        return switch (kind) {
            case RUN -> Run.builder().id("run").type(Run.class.getName())
                .script(Property.ofValue("print(1)")).taskRunner(runner).containerImage(image)
                .build().commands(runContext);
            case CLI -> CLI.builder().id("cli").type(CLI.class.getName())
                .commands(Property.ofValue(List.of("dlt --version"))).taskRunner(runner).containerImage(image)
                .build().commands(runContext);
        };
    }

    private List<String> entryPointOf(Kind kind, TaskRunner<?> runner, String containerImage) throws Exception {
        TaskRunner<?> effective = commandsOf(kind, runner, containerImage).getTaskRunner();
        return ((Docker) effective).getEntryPoint();
    }

    private static Docker.DockerBuilder<?, ?> docker() {
        return Docker.builder().type(Docker.class.getName()).pullPolicy(Property.ofValue(PullPolicy.IF_NOT_PRESENT));
    }

    @Test
    void noTaskRunnerWithDefaultImageUsesEntrypointScript() throws Exception {
        for (var kind : Kind.values()) {
            assertThat(entryPointOf(kind, null, null), is(SCRIPT));
        }
    }

    @Test
    void explicitDockerRunnerWithDefaultImageUsesEntrypointScript() throws Exception {
        for (var kind : Kind.values()) {
            assertThat(entryPointOf(kind, docker().build(), "ghcr.io/kestra-io/dlt:latest"), is(SCRIPT));
        }
    }

    @Test
    void userEntrypointOnRunnerIsPreserved() throws Exception {
        for (var kind : Kind.values()) {
            assertThat(entryPointOf(kind, docker().entryPoint(List.of("/bin/custom")).build(), null), is(List.of("/bin/custom")));
        }
    }

    @Test
    void customImageKeepsDefaultEntrypoint() throws Exception {
        for (var kind : Kind.values()) {
            assertThat(entryPointOf(kind, null, "python:3.11-slim"), is(List.of("")));
            assertThat(entryPointOf(kind, docker().build(), "ghcr.io/kestra-io/dlt-custom"), is(List.of("")));
        }
    }

    @Test
    void imageSetOnDockerRunnerIsTheOneChecked() throws Exception {
        assertThat(entryPointOf(Kind.RUN, docker().image("python:3.11-slim").build(), null), is(List.of("")));
        assertThat(entryPointOf(Kind.RUN, docker().image("ghcr.io/kestra-io/dlt").build(), "python:3.11-slim"), is(SCRIPT));
    }

    @Test
    void nonDockerRunnerIsUntouched() throws Exception {
        for (var kind : Kind.values()) {
            var runner = Process.builder().type(Process.class.getName()).build();

            TaskRunner<?> effective = commandsOf(kind, runner, null).getTaskRunner();

            assertThat(effective == runner, is(true));
        }
    }
}
