package io.kestra.plugin.dlt;

import java.util.List;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.kestra.plugin.scripts.runner.docker.Docker;

final class DltDockerDefaults {
    static final String DEFAULT_IMAGE = "ghcr.io/kestra-io/dlt";
    static final String DEFAULT_IMAGE_ENTRYPOINT = "/usr/local/bin/kestra-entrypoint.sh";

    private DltDockerDefaults() {}

    /**
     * Sets the non-root entrypoint on the Docker task runner of the wrapper when it runs the default dlt image.
     * <p>
     * Applied on the wrapper returned by {@code commands()} rather than in {@code injectDefaults}, which Kestra only
     * calls for the deprecated {@code docker} property on 1.3.x and never on 2.x. The runner is only replaced when it is
     * a {@link Docker} runner whose entrypoint is unset or still the default {@code [""]}; custom images, other runners
     * and user-defined entrypoints are left untouched.
     */
    static CommandsWrapper applyEntrypoint(CommandsWrapper commands) {
        TaskRunner<?> runner = commands.getTaskRunner();
        if (!(runner instanceof Docker docker)) {
            return commands;
        }

        // The Docker runner's own image wins over the task containerImage at runtime, so check it first.
        var image = docker.getImage() != null ? docker.getImage() : commands.getContainerImage();
        if (!isDefaultImage(image) || !hasDefaultEntryPoint(docker.getEntryPoint())) {
            return commands;
        }

        return commands.withTaskRunner(docker.toBuilder().entryPoint(List.of(DEFAULT_IMAGE_ENTRYPOINT)).build());
    }

    private static boolean hasDefaultEntryPoint(List<String> entryPoint) {
        return entryPoint == null || entryPoint.isEmpty() || entryPoint.equals(List.of(""));
    }

    static DockerOptions inject(RunContext runContext, DockerOptions original, Property<String> containerImage)
        throws IllegalVariableEvaluationException {
        var builder = original.toBuilder();
        var image = original.getImage();
        if (image == null) {
            image = runContext.render(containerImage).as(String.class).orElse(null);
            builder.image(image);
        }
        if (original.getEntryPoint() == null || original.getEntryPoint().isEmpty()) {
            // Only the default image ships the entrypoint script; custom images keep the cleared entrypoint.
            builder.entryPoint(isDefaultImage(image) ? List.of(DEFAULT_IMAGE_ENTRYPOINT) : List.of(""));
        }
        return builder.build();
    }

    static boolean isDefaultImage(String image) {
        return image != null && (image.equals(DEFAULT_IMAGE) || image.startsWith(DEFAULT_IMAGE + ":") || image.startsWith(DEFAULT_IMAGE + "@"));
    }
}
