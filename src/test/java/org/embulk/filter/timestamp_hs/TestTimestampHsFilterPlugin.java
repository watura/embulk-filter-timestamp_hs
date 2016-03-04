package org.embulk.filter.timestamp_hs;

import com.google.common.base.Joiner;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.spi.*;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.Types;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.msgpack.value.ValueFactory;

import java.util.List;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;


public class TestTimestampHsFilterPlugin {

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private ConfigSource newConfigSourceFromYaml(String yaml) {
        ConfigLoader loader = new ConfigLoader(Exec.getModelManager());
        return loader.fromYamlString(yaml);
    }

    @Test
    public void testThrowExceptionAbsentColumnOptions() {
        exception.expect(ConfigException.class);

        Exec.newConfigSource().loadConfig(
                TimestampHsFilterPlugin.PluginTask.class);
    }

    @Test
    public void testDefaultConfigs() {
        String configYaml = Joiner.on("\n").join(
                "column_options:",
                "  dummy: {}"
        );
        ConfigSource source = newConfigSourceFromYaml(configYaml);

        TimestampHsFilterPlugin.PluginTask task = source.loadConfig(
                TimestampHsFilterPlugin.PluginTask.class);

        assertThat(task.getDefaultTimezone(), is(DateTimeZone.UTC));
        assertThat(task.getDefaultTimestampFormat(), is("yyyy-MM-dd hh:mm:ss"));
    }

    @Test
    public void testDefaultConfigsWhenNullColumnOptions() {
        String configYaml = Joiner.on("\n").join(
                "column_options:",
                "  dummy:"
        );
        ConfigSource source = newConfigSourceFromYaml(configYaml);

        TimestampHsFilterPlugin.PluginTask task = source.loadConfig(
                TimestampHsFilterPlugin.PluginTask.class);

        assertThat(task.getDefaultTimezone(), is(DateTimeZone.UTC));
        assertThat(task.getDefaultTimestampFormat(), is("yyyy-MM-dd hh:mm:ss"));
    }

    @Test
    public void testThrowExceptionWhenColumnNameIsNotFound() {
        String configYaml = Joiner.on("\n").join(
                "column_options:",
                "  illegal_column_name:"
        );
        ConfigSource config = newConfigSourceFromYaml(configYaml);

        final Schema inputSchema = Schema.builder()
                .add("string_column", Types.STRING)
                .build();

        TimestampHsFilterPlugin plugin = new TimestampHsFilterPlugin();

        exception.expect(SchemaConfigException.class);

        plugin.transaction(config, inputSchema, new FilterPlugin.Control() {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema) {
            }
        });
    }

    @Test
    public void testConvertColumnType() {
        String configYaml = Joiner.on("\n").join(
                "column_options:",
                "  string_column:"
        );
        ConfigSource config = newConfigSourceFromYaml(configYaml);

        final Schema inputSchema = Schema.builder()
                .add("string_column", Types.STRING)
                .build();

        TimestampHsFilterPlugin plugin = new TimestampHsFilterPlugin();

        plugin.transaction(config, inputSchema, new FilterPlugin.Control() {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema) {
                Column outputColumn = outputSchema.getColumn(0);
                assertEquals(Types.TIMESTAMP, outputColumn.getType());
            }
        });
    }

    @Test
    public void testNotConvertColumnTypeWhenItIsNotString() {
        String configYaml = Joiner.on("\n").join(
                "column_options:",
                "  long_column:"
        );
        ConfigSource config = newConfigSourceFromYaml(configYaml);

        final Schema inputSchema = Schema.builder()
                .add("long_column", Types.LONG)
                .build();

        TimestampHsFilterPlugin plugin = new TimestampHsFilterPlugin();

        plugin.transaction(config, inputSchema, new FilterPlugin.Control() {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema) {
                Column outputColumn = outputSchema.getColumn(0);
                assertEquals(Types.LONG, outputColumn.getType());
            }
        });
    }

    @Test
    public void testNotConvertColumnTypeWhenNotSpecified() {
        String configYaml = Joiner.on("\n").join(
                "column_options:",
                "  string_column1:"
        );
        ConfigSource config = newConfigSourceFromYaml(configYaml);

        final Schema inputSchema = Schema.builder()
                .add("string_column1", Types.STRING)
                .add("string_column2", Types.STRING)
                .build();

        TimestampHsFilterPlugin plugin = new TimestampHsFilterPlugin();

        plugin.transaction(config, inputSchema, new FilterPlugin.Control() {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema) {
                Column outputColumn = outputSchema.getColumn(1);
                assertEquals(Types.STRING, outputColumn.getType());
            }
        });
    }

    @Test
    public void testSetNullWhenInputColumnIsNull() {
        // TODO: later
    }

    @Test
    public void testSetOriginalWhenInputTypeIsIllegal() {
        String configYaml = Joiner.on("\n").join(
                "column_options:",
                "  long_column:"
        );
        ConfigSource config = newConfigSourceFromYaml(configYaml);

        final Schema inputSchema = Schema.builder()
                .add("long_column", Types.LONG)
                .build();

        final TimestampHsFilterPlugin plugin = new TimestampHsFilterPlugin();

        plugin.transaction(config, inputSchema, new FilterPlugin.Control() {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema) {
                TestPageBuilderReader.MockPageOutput mockPageOutput
                        = new TestPageBuilderReader.MockPageOutput();

                PageOutput pageOutput = plugin.open(
                        taskSource,
                        inputSchema,
                        outputSchema,
                        mockPageOutput);

                List<Page> pages = PageTestUtils.buildPage(
                        runtime.getBufferAllocator(),
                        inputSchema,
                        12345L);

                for (Page page : pages) {
                    pageOutput.add(page);
                }
                pageOutput.finish();

                PageReader pageReader = new PageReader(outputSchema);
                pageReader.setPage(mockPageOutput.pages.get(0));
                assertThat(
                        pageReader.getLong(outputSchema.getColumn(0)),
                        is(12345L));
            }
        });
    }

    @Test
    public void testSetOriginalString() {
        String configYaml = Joiner.on("\n").join(
                "column_options:",
                "  timestamp:"
        );
        ConfigSource config = newConfigSourceFromYaml(configYaml);

        final Schema inputSchema = Schema.builder()
                .add("string_column", Types.STRING)
                .add("timestamp", Types.STRING)
                .build();

        final TimestampHsFilterPlugin plugin = new TimestampHsFilterPlugin();

        plugin.transaction(config, inputSchema, new FilterPlugin.Control() {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema) {
                TestPageBuilderReader.MockPageOutput mockPageOutput
                        = new TestPageBuilderReader.MockPageOutput();

                PageOutput pageOutput = plugin.open(
                        taskSource,
                        inputSchema,
                        outputSchema,
                        mockPageOutput);

                List<Page> pages = PageTestUtils.buildPage(
                        runtime.getBufferAllocator(),
                        inputSchema,
                        "test text",
                        "2000-01-01 00:00:00");

                for (Page page : pages) {
                    pageOutput.add(page);
                }
                pageOutput.finish();

                PageReader pageReader = new PageReader(outputSchema);
                pageReader.setPage(mockPageOutput.pages.get(0));
                assertThat(
                        pageReader.getString(outputSchema.getColumn(0)),
                        is("test text"));
            }
        });
    }

    @Test
    public void testSetOriginalBoolean() {
        String configYaml = Joiner.on("\n").join(
                "column_options:",
                "  timestamp:"
        );
        ConfigSource config = newConfigSourceFromYaml(configYaml);

        final Schema inputSchema = Schema.builder()
                .add("boolean_column", Types.BOOLEAN)
                .add("timestamp", Types.STRING)
                .build();

        final TimestampHsFilterPlugin plugin = new TimestampHsFilterPlugin();

        plugin.transaction(config, inputSchema, new FilterPlugin.Control() {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema) {
                TestPageBuilderReader.MockPageOutput mockPageOutput
                        = new TestPageBuilderReader.MockPageOutput();

                PageOutput pageOutput = plugin.open(
                        taskSource,
                        inputSchema,
                        outputSchema,
                        mockPageOutput);

                List<Page> pages = PageTestUtils.buildPage(
                        runtime.getBufferAllocator(),
                        inputSchema,
                        true,
                        "2000-01-01 00:00:00");

                for (Page page : pages) {
                    pageOutput.add(page);
                }
                pageOutput.finish();

                PageReader pageReader = new PageReader(outputSchema);
                pageReader.setPage(mockPageOutput.pages.get(0));
                assertThat(
                        pageReader.getBoolean(outputSchema.getColumn(0)),
                        is(true));
            }
        });
    }

    @Test
    public void testSetOriginalDouble() {
        String configYaml = Joiner.on("\n").join(
                "column_options:",
                "  timestamp:"
        );
        ConfigSource config = newConfigSourceFromYaml(configYaml);

        final Schema inputSchema = Schema.builder()
                .add("double_column", Types.DOUBLE)
                .add("timestamp", Types.STRING)
                .build();

        final TimestampHsFilterPlugin plugin = new TimestampHsFilterPlugin();

        plugin.transaction(config, inputSchema, new FilterPlugin.Control() {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema) {
                TestPageBuilderReader.MockPageOutput mockPageOutput
                        = new TestPageBuilderReader.MockPageOutput();

                PageOutput pageOutput = plugin.open(
                        taskSource,
                        inputSchema,
                        outputSchema,
                        mockPageOutput);

                List<Page> pages = PageTestUtils.buildPage(
                        runtime.getBufferAllocator(),
                        inputSchema,
                        123.456,
                        "2000-01-01 00:00:00");

                for (Page page : pages) {
                    pageOutput.add(page);
                }
                pageOutput.finish();

                PageReader pageReader = new PageReader(outputSchema);
                pageReader.setPage(mockPageOutput.pages.get(0));
                assertThat(
                        pageReader.getDouble(outputSchema.getColumn(0)),
                        is(123.456));
            }
        });
    }

    @Test
    public void testSetOriginalLong() {
        String configYaml = Joiner.on("\n").join(
                "column_options:",
                "  timestamp:"
        );
        ConfigSource config = newConfigSourceFromYaml(configYaml);

        final Schema inputSchema = Schema.builder()
                .add("long_column", Types.LONG)
                .add("timestamp", Types.STRING)
                .build();

        final TimestampHsFilterPlugin plugin = new TimestampHsFilterPlugin();

        plugin.transaction(config, inputSchema, new FilterPlugin.Control() {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema) {
                TestPageBuilderReader.MockPageOutput mockPageOutput
                        = new TestPageBuilderReader.MockPageOutput();

                PageOutput pageOutput = plugin.open(
                        taskSource,
                        inputSchema,
                        outputSchema,
                        mockPageOutput);

                List<Page> pages = PageTestUtils.buildPage(
                        runtime.getBufferAllocator(),
                        inputSchema,
                        12345L,
                        "2000-01-01 00:00:00");

                for (Page page : pages) {
                    pageOutput.add(page);
                }
                pageOutput.finish();

                PageReader pageReader = new PageReader(outputSchema);
                pageReader.setPage(mockPageOutput.pages.get(0));
                assertThat(
                        pageReader.getLong(outputSchema.getColumn(0)),
                        is(12345L));
            }
        });
    }

    @Test
    public void testSetOriginalTimestamp() {
        String configYaml = Joiner.on("\n").join(
                "column_options:",
                "  timestamp:"
        );
        ConfigSource config = newConfigSourceFromYaml(configYaml);

        final Schema inputSchema = Schema.builder()
                .add("timestamp_column", Types.TIMESTAMP)
                .add("timestamp", Types.STRING)
                .build();

        final TimestampHsFilterPlugin plugin = new TimestampHsFilterPlugin();

        plugin.transaction(config, inputSchema, new FilterPlugin.Control() {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema) {
                TestPageBuilderReader.MockPageOutput mockPageOutput
                        = new TestPageBuilderReader.MockPageOutput();

                PageOutput pageOutput = plugin.open(
                        taskSource,
                        inputSchema,
                        outputSchema,
                        mockPageOutput);

                List<Page> pages = PageTestUtils.buildPage(
                        runtime.getBufferAllocator(),
                        inputSchema,
                        Timestamp.ofEpochMilli(123456789),
                        "2000-01-01 00:00:00");

                for (Page page : pages) {
                    pageOutput.add(page);
                }
                pageOutput.finish();

                PageReader pageReader = new PageReader(outputSchema);
                pageReader.setPage(mockPageOutput.pages.get(0));
                assertThat(
                        pageReader.getTimestamp(outputSchema.getColumn(0)),
                        is(Timestamp.ofEpochMilli(123456789)));
            }
        });
    }

    @Test
    public void testSetOriginalJson() {
        String configYaml = Joiner.on("\n").join(
                "column_options:",
                "  timestamp:"
        );
        ConfigSource config = newConfigSourceFromYaml(configYaml);

        final Schema inputSchema = Schema.builder()
                .add("json_column", Types.JSON)
                .add("timestamp", Types.STRING)
                .build();

        final TimestampHsFilterPlugin plugin = new TimestampHsFilterPlugin();

        plugin.transaction(config, inputSchema, new FilterPlugin.Control() {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema) {
                TestPageBuilderReader.MockPageOutput mockPageOutput
                        = new TestPageBuilderReader.MockPageOutput();

                PageOutput pageOutput = plugin.open(
                        taskSource,
                        inputSchema,
                        outputSchema,
                        mockPageOutput);

                List<Page> pages = PageTestUtils.buildPage(
                        runtime.getBufferAllocator(),
                        inputSchema,
                        ValueFactory.newInteger(12345).asIntegerValue(),
                        "2000-01-01 00:00:00");

                for (Page page : pages) {
                    pageOutput.add(page);
                }
                pageOutput.finish();

                PageReader pageReader = new PageReader(outputSchema);
                pageReader.setPage(mockPageOutput.pages.get(0));
                assertThat(
                        pageReader.getJson(outputSchema.getColumn(0)).immutableValue(),
                        is(ValueFactory.newInteger(12345).immutableValue()));
            }
        });
    }

    @Test
    public void testSetConvertedTimestamp() {
        String configYaml = Joiner.on("\n").join(
                "column_options:",
                "  column1: { format: 'yyyy-MM-dd hh:mm:ss.SSS', timezone: 'UTC' }",
                "  column2: { format: 'yyyy-MM-dd hh:mm:ss', timezone: 'Asia/Tokyo' }"
        );
        ConfigSource config = newConfigSourceFromYaml(configYaml);

        final Schema inputSchema = Schema.builder()
                .add("column1", Types.STRING)
                .add("column2", Types.STRING)
                .build();

        final TimestampHsFilterPlugin plugin = new TimestampHsFilterPlugin();

        plugin.transaction(config, inputSchema, new FilterPlugin.Control() {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema) {
                TestPageBuilderReader.MockPageOutput mockPageOutput
                        = new TestPageBuilderReader.MockPageOutput();

                PageOutput pageOutput = plugin.open(
                        taskSource,
                        inputSchema,
                        outputSchema,
                        mockPageOutput);

                List<Page> pages = PageTestUtils.buildPage(
                        runtime.getBufferAllocator(),
                        inputSchema,
                        "2000-01-02 10:11:12.123",
                        "2010-11-12 20:21:22");

                for (Page page : pages) {
                    pageOutput.add(page);
                }
                pageOutput.finish();

                PageReader pageReader = new PageReader(outputSchema);
                pageReader.setPage(mockPageOutput.pages.get(0));

                DateTime expected1 = new DateTime(
                        2000, 1, 2, 10, 11, 12, 123, DateTimeZone.forOffsetHours(0));
                assertThat(
                        pageReader.getTimestamp(outputSchema.getColumn(0)),
                        is(Timestamp.ofEpochMilli(expected1.getMillis())));

                DateTime expected2 = new DateTime(
                        2010, 11, 12, 20, 21, 22, DateTimeZone.forOffsetHours(9));
                assertThat(
                        pageReader.getTimestamp(outputSchema.getColumn(1)),
                        is(Timestamp.ofEpochMilli(expected2.getMillis())));
            }
        });
    }

    @Test
    public void testSetConvertedTimestampWithDefaultSettings() {
        String configYaml = Joiner.on("\n").join(
                "default_timezone: 'Asia/Tokyo'",
                "default_timestamp_format: 'hh:mm:ss yyyy/MM/dd'",
                "column_options:",
                "  column1:",
                "  column2: { format: 'yyyy-MM-dd hh:mm:ss', timezone: 'UTC' }"
        );
        ConfigSource config = newConfigSourceFromYaml(configYaml);

        final Schema inputSchema = Schema.builder()
                .add("column1", Types.STRING)
                .add("column2", Types.STRING)
                .build();

        final TimestampHsFilterPlugin plugin = new TimestampHsFilterPlugin();

        plugin.transaction(config, inputSchema, new FilterPlugin.Control() {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema) {
                TestPageBuilderReader.MockPageOutput mockPageOutput
                        = new TestPageBuilderReader.MockPageOutput();

                PageOutput pageOutput = plugin.open(
                        taskSource,
                        inputSchema,
                        outputSchema,
                        mockPageOutput);

                List<Page> pages = PageTestUtils.buildPage(
                        runtime.getBufferAllocator(),
                        inputSchema,
                        "10:11:12 2000/01/02",
                        "2010-11-12 20:21:22");

                for (Page page : pages) {
                    pageOutput.add(page);
                }
                pageOutput.finish();

                PageReader pageReader = new PageReader(outputSchema);
                pageReader.setPage(mockPageOutput.pages.get(0));

                DateTime expected1 = new DateTime(
                        2000, 1, 2, 10, 11, 12, DateTimeZone.forOffsetHours(9));
                assertThat(
                        pageReader.getTimestamp(outputSchema.getColumn(0)),
                        is(Timestamp.ofEpochMilli(expected1.getMillis())));

                DateTime expected2 = new DateTime(
                        2010, 11, 12, 20, 21, 22, DateTimeZone.forOffsetHours(0));
                assertThat(
                        pageReader.getTimestamp(outputSchema.getColumn(1)),
                        is(Timestamp.ofEpochMilli(expected2.getMillis())));
            }
        });
    }

    @Test
    public void testThrowExceptionWhenIllegalFormat() {
        String configYaml = Joiner.on("\n").join(
                "column_options:",
                "  column: { format: 'yyyy-MM-dd hh:mm:ss' }"
        );
        ConfigSource config = newConfigSourceFromYaml(configYaml);

        final Schema inputSchema = Schema.builder()
                .add("column", Types.STRING)
                .build();

        final TimestampHsFilterPlugin plugin = new TimestampHsFilterPlugin();

        plugin.transaction(config, inputSchema, new FilterPlugin.Control() {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema) {
                TestPageBuilderReader.MockPageOutput mockPageOutput
                        = new TestPageBuilderReader.MockPageOutput();

                PageOutput pageOutput = plugin.open(
                        taskSource,
                        inputSchema,
                        outputSchema,
                        mockPageOutput);

                List<Page> pages = PageTestUtils.buildPage(
                        runtime.getBufferAllocator(),
                        inputSchema,
                        "timestamp");

                for (Page page : pages) {
                    pageOutput.add(page);
                }
                pageOutput.finish();

                PageReader pageReader = new PageReader(outputSchema);
                pageReader.setPage(mockPageOutput.pages.get(0));
                assertThat(
                        pageReader.getTimestamp(outputSchema.getColumn(0)),
                        is(Timestamp.ofEpochMilli(0)));
            }
        });
    }

}
