package io.kestra.plugin.dlt;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class DltDockerDefaultsTest {
    private static final List<String> SCRIPT = List.of("/usr/local/bin/kestra-entrypoint.sh");

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
}
