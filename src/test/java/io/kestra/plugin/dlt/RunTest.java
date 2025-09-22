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
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
public class RunTest {

    @Inject
    RunContextFactory runContextFactory;

    @Inject
    @Named(QueueFactoryInterface.WORKERTASKLOG_NAMED)
    private QueueInterface<LogEntry> logQueue;

    @Test
    void script() throws Exception {
        List<LogEntry> logs = new CopyOnWriteArrayList<>();
        Flux<LogEntry> receive = TestsUtils.receive(logQueue, l -> logs.add(l.getLeft()));

        Run dltRun = Run.builder()
            .id("dlt-run-" + UUID.randomUUID())
            .type(Run.class.getName())
            .script(Property.ofValue("""
                import dlt

                @dlt.resource
                def sample_data():
                    return [
                        {"id": 1, "name": "Kestra", "type": "orchestrator"},
                        {"id": 2, "name": "DLT", "type": "data_loader"}
                    ]

                pipeline = dlt.pipeline(
                    pipeline_name="kestra_test",
                    destination="duckdb",
                    dataset_name="test_data"
                )

                info = pipeline.run([sample_data()])
                print("Kestra is amazing!")
                print(f"Successfully loaded {len(info.loads_ids)} loads")
                """))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, dltRun, ImmutableMap.of());
        ScriptOutput run = dltRun.run(runContext);

        assertThat(run.getExitCode(), is(0));
        assertThat(run.getStdOutLineCount() > 0, is(true));

        TestsUtils.awaitLog(logs, log -> log.getMessage() != null && log.getMessage().contains("Kestra is amazing!"));
        receive.blockLast();
        assertThat(List.copyOf(logs).stream().anyMatch(log -> log.getMessage() != null && log.getMessage().contains("Kestra is amazing!")), is(true));
    }

    @Test
    void scriptWithInstallPackages() throws Exception {
        List<LogEntry> logs = new CopyOnWriteArrayList<>();
        Flux<LogEntry> receive = TestsUtils.receive(logQueue, l -> logs.add(l.getLeft()));

        Run dltRun = Run.builder()
            .id("dlt-run-packages-" + UUID.randomUUID())
            .type(Run.class.getName())
            .script(Property.ofValue("""
                import dlt
                import requests

                @dlt.resource
                def api_data():
                    return [{"status": "success", "message": "DLT with packages works!"}]

                pipeline = dlt.pipeline(
                    pipeline_name="packages_test",
                    destination="duckdb",
                    dataset_name="api_data"
                )

                info = pipeline.run([api_data()])
                print("DLT with packages works!")
                """))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, dltRun, ImmutableMap.of());
        ScriptOutput run = dltRun.run(runContext);

        assertThat(run.getExitCode(), is(0));

        TestsUtils.awaitLog(logs, log -> log.getMessage() != null && log.getMessage().contains("DLT with packages works!"));
        receive.blockLast();
        assertThat(List.copyOf(logs).stream().anyMatch(log -> log.getMessage() != null && log.getMessage().contains("DLT with packages works!")), is(true));
    }

    @Test
    void scriptWithMultipleResources() throws Exception {
        List<LogEntry> logs = new CopyOnWriteArrayList<>();
        Flux<LogEntry> receive = TestsUtils.receive(logQueue, l -> logs.add(l.getLeft()));

        Run dltRun = Run.builder()
            .id("dlt-run-multi-" + UUID.randomUUID())
            .type(Run.class.getName())
            .script(Property.ofValue("""
                import dlt

                @dlt.source
                def multi_table_source():
                    @dlt.resource
                    def users():
                        return [
                            {"id": 1, "name": "Alice", "role": "admin"},
                            {"id": 2, "name": "Bob", "role": "user"}
                        ]

                    @dlt.resource
                    def orders():
                        return [
                            {"order_id": 1, "user_id": 1, "amount": 100},
                            {"order_id": 2, "user_id": 2, "amount": 50}
                        ]

                    return users, orders

                pipeline = dlt.pipeline(
                    pipeline_name="multi_resource_test",
                    destination="duckdb",
                    dataset_name="ecommerce"
                )

                info = pipeline.run(multi_table_source())
                print("Multi-resource DLT pipeline completed!")
                print(f"Tables loaded: {len(info.loads_ids)}")
                """))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, dltRun, ImmutableMap.of());
        ScriptOutput run = dltRun.run(runContext);

        assertThat(run.getExitCode(), is(0));

        TestsUtils.awaitLog(logs, log -> log.getMessage() != null && log.getMessage().contains("Multi-resource DLT pipeline completed!"));
        receive.blockLast();
        assertThat(List.copyOf(logs).stream().anyMatch(log -> log.getMessage() != null && log.getMessage().contains("Multi-resource DLT pipeline completed!")), is(true));
    }
}