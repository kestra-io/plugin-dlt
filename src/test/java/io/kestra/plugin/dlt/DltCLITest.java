package io.kestra.plugin.dlt;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
public class DltCLITest {

    @Inject
    RunContextFactory runContextFactory;

    @Inject
    @Named(QueueFactoryInterface.WORKERTASKLOG_NAMED)
    private QueueInterface<LogEntry> logQueue;

    @Test
    void task() throws Exception {
        List<LogEntry> logs = new CopyOnWriteArrayList<>();
        Flux<LogEntry> receive = TestsUtils.receive(logQueue, l -> logs.add(l.getLeft()));

        var dltDltCLI = DltCLI.builder()
            .id("dlt-DltCLI-" + UUID.randomUUID())
            .type(DltCLI.class.getName())
            .commands(Property.ofValue(List.of("python pipeline.py")))
            .inputFiles(Map.of("pipeline.py", """
                import dlt

                @dlt.resource
                def sample_data():
                    return [{"id": 1, "message": "I love Kestra!"}]

                pipeline = dlt.pipeline(
                    pipeline_name="test_pipeline",
                    destination="duckdb",
                    dataset_name="test_data"
                )

                info = pipeline.run([sample_data()])
                print("I love Kestra!")
                print(f"Pipeline completed: {info.loads_ids}")
                """))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, dltDltCLI, ImmutableMap.of());
        ScriptOutput run = dltDltCLI.run(runContext);

        assertThat(run.getExitCode(), is(0));

        TestsUtils.awaitLog(logs, log -> log.getMessage() != null && log.getMessage().contains("I love Kestra!"));
        receive.blockLast();
        assertThat(List.copyOf(logs).stream().anyMatch(log -> log.getMessage() != null && log.getMessage().contains("I love Kestra!")), is(true));
    }

    @Test
    void dltInit() throws Exception {
        List<LogEntry> logs = new CopyOnWriteArrayList<>();
        Flux<LogEntry> receive = TestsUtils.receive(logQueue, l -> logs.add(l.getLeft()));

        var dltDltCLI = DltCLI.builder()
            .id("dlt-init-" + UUID.randomUUID())
            .type(DltCLI.class.getName())
            .commands(Property.ofValue(List.of(
                "dlt init rest_api duckdb",
                "echo 'DLT project initialized successfully!'"
            )))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, dltDltCLI, ImmutableMap.of());
        ScriptOutput run = dltDltCLI.run(runContext);

        assertThat(run.getExitCode(), is(0));

        TestsUtils.awaitLog(logs, log -> log.getMessage() != null && log.getMessage().contains("DLT project initialized successfully!"));
        receive.blockLast();
        assertThat(List.copyOf(logs).stream().anyMatch(log -> log.getMessage() != null && log.getMessage().contains("DLT project initialized successfully!")), is(true));
    }

    @Test
    void dltVersion() throws Exception {
        List<LogEntry> logs = new CopyOnWriteArrayList<>();
        Flux<LogEntry> receive = TestsUtils.receive(logQueue, l -> logs.add(l.getLeft()));

        var dltDltCLI = DltCLI.builder()
            .id("dlt-version-" + UUID.randomUUID())
            .type(DltCLI.class.getName())
            .commands(Property.ofValue(List.of(
                "dlt --version",
                "echo 'DLT version check completed!'"
            )))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, dltDltCLI, ImmutableMap.of());
        ScriptOutput run = dltDltCLI.run(runContext);

        assertThat(run.getExitCode(), is(0));

        TestsUtils.awaitLog(logs, log -> log.getMessage() != null && log.getMessage().contains("DLT version check completed!"));
        receive.blockLast();
        assertThat(List.copyOf(logs).stream().anyMatch(log -> log.getMessage() != null && log.getMessage().contains("DLT version check completed!")), is(true));
    }
}