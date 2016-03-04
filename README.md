# Timestamp Hs filter plugin for Embulk

Convert string to timestamp at high speed.

## Overview

* **Plugin type**: filter

## Configuration

- **default_timezone**: Default timezone (string, default: `UTC`)
- **default_timestamp_format**: Default timezone format by [SimpleDateFormat](http://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html) style (string, default: `yyyy-MM-dd hh:mm:ss`)
- **column_options**: Timestamp column options (hash, required)
  - **timezone**: Timezone (hash, default: `default_timezone`)
  - **format**: Timestamp format (hash, default: `default_timestamp_format`)

## Example

```csv
2016-01-01 10:02:30.100,2016-01-01 10:02:30.111
2016-01-02 10:02:30.200,2016-01-02 10:02:30.211
2016-01-03 10:02:30.300,2016-01-03 10:02:30.311
2016-01-04 10:02:30.400,2016-01-04 10:02:30.411
2016-01-05 10:02:30.500,2016-01-05 10:02:30.511
```

```yaml
in:
  type: file
  path_prefix: applog
  parser:
    type: csv
    delimiter: ","
    columns:
    - {name: standardTimestamp, type: timestamp, format: '%Y-%m-%d %H:%M:%S.%L'}
    - {name: highSpeedTimestamp, type: string}

filters:
  - type: timestamp_hs
    default_timezone: 'UTC'
    column_options:
      highSpeedTimestamp: {format: 'yyyy-MM-dd hh:mm:ss.SSS'}
```

```sh
+-----------------------------+------------------------------+
| standardTimestamp:timestamp | highSpeedTimestamp:timestamp |
+-----------------------------+------------------------------+
| 2016-01-01 10:02:30.100 UTC |  2016-01-01 10:02:30.111 UTC |
| 2016-01-02 10:02:30.200 UTC |  2016-01-02 10:02:30.211 UTC |
| 2016-01-03 10:02:30.300 UTC |  2016-01-03 10:02:30.311 UTC |
| 2016-01-04 10:02:30.400 UTC |  2016-01-04 10:02:30.411 UTC |
| 2016-01-05 10:02:30.500 UTC |  2016-01-05 10:02:30.511 UTC |
+-----------------------------+------------------------------+
```

## Performance

Input file is 1,000,000 lines with one timestamp column.

- OS: Windows 10
- CPU: Core i5 2.67GHz
- Embulk: 0.8.6

Embulk standard timestamp: 240.910s

With timestamp_hs filter: 1.902s

## Install

```sh
$ embulk gem install embulk-filter-timestamp_hs
```
