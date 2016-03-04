package org.embulk.filter.timestamp_hs;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.*;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.type.Types;
import org.joda.time.DateTimeZone;
import org.msgpack.value.Value;
import org.slf4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;


public class TimestampHsFilterPlugin implements FilterPlugin {

    public interface PluginTask extends Task {

        @Config("default_timezone")
        @ConfigDefault("\"UTC\"")
        DateTimeZone getDefaultTimezone();

        @Config("default_timestamp_format")
        @ConfigDefault("\"yyyy-MM-dd hh:mm:ss\"")
        String getDefaultTimestampFormat();

        @Config("column_options")
        Map<String, TimestampColumnOption> getColumnOptions();
    }

    public interface TimestampColumnOption
            extends Task, TimestampFormatter.TimestampColumnOption {
    }

    private final Logger log;

    public TimestampHsFilterPlugin() {
        this.log = Exec.getLogger(TimestampHsFilterPlugin.class);
    }

    @Override
    public void transaction(ConfigSource config,
                            Schema inputSchema,
                            FilterPlugin.Control control) {
        PluginTask task = config.loadConfig(PluginTask.class);

        // Validate column names.
        for (String columnName : task.getColumnOptions().keySet()) {
            inputSchema.lookupColumn(columnName);
        }

        Map<String, TimestampColumnOption> options = task.getColumnOptions();
        Schema.Builder builder = Schema.builder();

        // Convert type "string" to "timestamp".
        for (Column column : inputSchema.getColumns()) {
            if (options.containsKey(column.getName())) {
                if (column.getType().equals(Types.STRING)) {
                    builder.add(column.getName(), Types.TIMESTAMP);
                } else {
                    log.warn(String.format(
                            "Can not convert to timestamp because '%s' is not string.",
                            column.getName()));
                    builder.add(column.getName(), column.getType());
                }
            } else {
                builder.add(column.getName(), column.getType());
            }
        }

        control.run(task.dump(), builder.build());
    }

    @Override
    public PageOutput open(final TaskSource taskSource,
                           final Schema inputSchema,
                           final Schema outputSchema,
                           final PageOutput output) {

        PluginTask task = taskSource.loadTask(PluginTask.class);

        final Map<String, SimpleDateFormat> timestampParsers
                = generateTimestampParsers(task);

        return new PageOutput() {
            private PageReader reader = new PageReader(inputSchema);

            private PageBuilder builder = new PageBuilder(
                    Exec.getBufferAllocator(),
                    outputSchema,
                    output);

            @Override
            public void add(Page page) {
                reader.setPage(page);
                while (reader.nextRecord()) {
                    setValues();
                    builder.addRecord();
                }
            }

            private void setValues() {
                for (Column inputColumn : inputSchema.getColumns()) {
                    setValue(inputColumn);
                }
            }

            private void setValue(Column inputColumn) {
                if (reader.isNull(inputColumn)) {
                    builder.setNull(inputColumn);
                    return;
                }

                if (timestampParsers.containsKey(inputColumn.getName())
                        && inputColumn.getType().equals(Types.STRING)) {
                    setTimestampFromString(inputColumn);
                } else {
                    setNonConvertedValue(inputColumn);
                }
            }

            private void setNonConvertedValue(Column inputColumn) {
                if (Types.STRING.equals(inputColumn.getType())) {
                    final String value = reader.getString(inputColumn);
                    builder.setString(inputColumn, value);
                } else if (Types.BOOLEAN.equals(inputColumn.getType())) {
                    final boolean value = reader.getBoolean(inputColumn);
                    builder.setBoolean(inputColumn, value);
                } else if (Types.DOUBLE.equals(inputColumn.getType())) {
                    final double value = reader.getDouble(inputColumn);
                    builder.setDouble(inputColumn, value);
                } else if (Types.LONG.equals(inputColumn.getType())) {
                    final long value = reader.getLong(inputColumn);
                    builder.setLong(inputColumn, value);
                } else if (Types.TIMESTAMP.equals(inputColumn.getType())) {
                    final Timestamp value = reader.getTimestamp(inputColumn);
                    builder.setTimestamp(inputColumn, value);
                } else if (Types.JSON.equals(inputColumn.getType())) {
                    final Value value = reader.getJson(inputColumn);
                    builder.setJson(inputColumn, value);
                } else {
                    throw new DataException("Unexpected type:" + inputColumn.getType());
                }
            }

            private void setTimestampFromString(Column inputColumn) {
                String inputText = reader.getString(inputColumn);

                try {
                    SimpleDateFormat parser = timestampParsers.get(
                            inputColumn.getName());

                    Timestamp timestamp = Timestamp.ofEpochMilli(
                            parser.parse(inputText).getTime());

                    builder.setTimestamp(inputColumn, timestamp);

                } catch (ParseException e) {
                    log.warn(String.format(
                            "Could not convert string to timestamp: '%s'",
                            inputText));
                    builder.setNull(inputColumn);
                }
            }

            @Override
            public void finish() {
                builder.finish();
            }

            @Override
            public void close() {
                builder.close();
            }
        };
    }

    private Map<String, SimpleDateFormat> generateTimestampParsers(PluginTask task) {
        Map<String, TimestampColumnOption> options = task.getColumnOptions();
        Map<String, SimpleDateFormat> timestampParsers = new HashMap<>();

        for (Map.Entry<String, TimestampColumnOption> entry : options.entrySet()) {
            TimestampColumnOption option = entry.getValue();

            String format;
            DateTimeZone timezone;
            if (option == null) {
                format = task.getDefaultTimestampFormat();
                timezone = task.getDefaultTimezone();
            } else {
                format = option.getFormat().or(task.getDefaultTimestampFormat());
                timezone = option.getTimeZone().or(task.getDefaultTimezone());
            }

            SimpleDateFormat sdf = new SimpleDateFormat(format);
            sdf.setTimeZone(timezone.toTimeZone());

            timestampParsers.put(entry.getKey(), sdf);
        }

        return timestampParsers;
    }
}
