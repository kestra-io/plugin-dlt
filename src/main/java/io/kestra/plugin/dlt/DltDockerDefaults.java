package io.kestra.plugin.dlt;

import java.util.List;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;

final class DltDockerDefaults {
    static final String DEFAULT_IMAGE = "ghcr.io/kestra-io/dlt";
    static final String DEFAULT_IMAGE_ENTRYPOINT = "/usr/local/bin/kestra-entrypoint.sh";

    private DltDockerDefaults() {}

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
