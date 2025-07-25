# asloglatency

The Aerospike Log Latency tool (asloglatency) analyzes Aerospike log files and returns the latency measurements. It returns latency analysis for a given time period in a tabular, easy to read form. The utility analyzes a histogram by parsing latency log lines during successive time slices, and calculating the percentage of operations in each time slice that exceeded various latency thresholds.


## Usage

Run the tool at the command line and provide option parameters. Use the --help option to display the list of options.

```bash
$ ./log_latency.py --help
```

### Options

| Option          | Default                          | Description                                                                                                      |
|-----------------|----------------------------------|------------------------------------------------------------------------------------------------------------------|
| -l              | /var/log/aerospike/aerospike.log | Path to Aerospike's log file.                                                                                    |
| -h <\HISTOGRAM> |                                  | (**required**) Name of the histogram to query.                                                                   |
| -N <\NAMESPACE> |                                  | Name of the namespace to examine latency.                                                                        |
| -t <\TIME>      | 10                               | Analysis slice interval in seconds or time format.<br />**Time Format**: 1:00:00                                 |
| -f <\TIME>      | tail                             | Log time from which to analyze. May use the following formats: head, 'Sep 22 2011 22:40:14', -3600, or -1:00:00. |
| -d <\TIME>      |                                  | Maximum time period to analyze. May use the following formats: 3600 or 1:00:00.                                  |
| -n <\N>         | 3                                | Number of buckets to display.                                                                                    |
| -e <\N>         | 3                                | Show the 0-th and then every n-th bucket.                                                                        |
| -r              | Disabled                         | Run until user presses return key or ctrl-c.  Auto enabled if `-f tail`.                                         |

### Examples

The most common use case for this utility is monitoring a live server in real-time. The default parameters support this common pattern.

For each set of histogram data, the following intervals are tracked:

| Interval      |  Time required to complete                                   |
|---------------|--------------------------------------------------------------|
| 0             | 0 to 2<sup>0</sup> ms (&ge; 0 ms to < 1 ms)                  |
| 1             | 2<sup>0</sup> to 2<sup>1</sup> ms (&ge; 1 ms to &lt; 2 ms)   |
| 2             | 2<sup>1</sup> to 2<sup>2</sup> ms (&ge; 2 ms to &lt; 4 ms)   |
| 3             | 2<sup>2</sup> to 2<sup>3</sup> ms (&ge; 4 ms to &lt; 8 ms)   |
| 8 and higher  | &hellip;                                                     |

These can be configured using `asinfo` command. See [Configuration Reference](https://aerospike.com/docs/database/reference/info#latency).

```bash
$ ./log_latency.py -h {test}-write
```

It returns the following histogram for writes for namespace `test`:

```bash
{test}-write
Jul 06 2016 20:15:45
               % > (ms)
slice-to (sec)      1      8     64  ops/sec
-------------- ------ ------ ------ --------
20:15:55    10   1.40   1.19   0.00  35535.6
20:16:05    10   1.46   1.21   0.00  35143.0
20:16:15    10   1.48   1.25   0.00  35235.0
20:16:25    10   1.49   1.27   0.00  34741.3
20:16:35    10   1.40   1.19   0.00  35243.0
20:16:45    10   1.42   1.23   0.00  35202.1
20:16:55    10   1.44   1.22   0.00  35047.6
20:17:05    10   1.56   1.33   0.00  34085.9
-------------- ------ ------ ------ --------
     avg         1.46   1.24   0.00  35029.0
     max         1.56   1.33   0.00  35535.6
```

Press Enter or CTRL+C to stop the script and display averages & maximums.

Histogram buckets illustrate a distribution of events.  Column 1 contains the % of write commands that were not completed within 1 ms. The second column contains counts of writes that were not completed within 8 ms.

`asloglatency` does not calculate in increments between 0 and 1. If you need to measure sub-millisecond latencies, you can measure them and calculate averages on the client.


The following example is another way to write same query as above.

```bash
$ ./log_latency.py -N test -h write
```

Another common use case is to examine a time period in the past. The following example analyzes reads for the `test` namespace:

```bash
$ ./log_latency.py -h {test}-read -f -100 -d 0:02:00
```
The analysis starts 100 seconds before the end of the log file. It reviews two minutes of records, and analyzes data in 10 second increments.

```bash
{test}-read
Jul 06 2016 20:31:06
               % > (ms)
slice-to (sec)      1      8     64  ops/sec
-------------- ------ ------ ------ --------
20:31:16    10   1.33   1.12   0.00   8616.8
20:31:26    10   1.38   1.18   0.00   8745.4
20:31:36    10   1.49   1.24   0.00   8565.5
20:31:46    10   1.52   1.29   0.00   8488.6
20:31:56    10   1.36   1.14   0.00   8620.2
20:32:06    10   1.50   1.28   0.00   8358.7
20:32:16    10   1.42   1.22   0.00   8675.5
20:32:26    10   1.42   1.20   0.00   8416.8
20:32:36    10   1.57   1.34   0.00   8539.2
20:32:46    10   1.46   1.23   0.00   8555.7
-------------- ------ ------ ------ --------
     avg         1.45   1.22   0.00   8558.0
     max         1.57   1.34   0.00   8745.4
```

:::note
Analyzing a time period that bridges a server restart distorts results of time slices and average/maximum calculations.
:::

Another common use case is to examine aggregate latency for all namespaces.

```bash
$ ./log_latency.py -h query
```

It returns the following histogram for queries for all namespaces:

```bash
query
Jul 10 2016 11:06:27
               % > (ms)
slice-to (sec)      1      8     64  ops/sec
-------------- ------ ------ ------ --------
11:06:37    10   4.24   0.26   0.00   4507.4
11:06:47    10   4.55   0.19   0.00   4484.7
11:06:57    10   4.87   0.31   0.00   4478.2
11:07:07    10   6.35   0.68   0.00   4288.6
11:07:17    10   2.93   0.02   0.00   3247.9
11:07:27    10   1.31   0.02   0.00   3493.6
11:07:37    10   0.49   0.00   0.00   3394.5
11:07:47    10   0.30   0.00   0.00   2666.3
11:07:57    10   0.57   0.00   0.00   3094.7
11:08:07    10   0.25   0.00   0.00   2184.1
-------------- ------ ------ ------ --------
     avg         2.59   0.15   0.00   3584.0
     max         6.35   0.68   0.00   4507.4
```

### Histograms

See the [Monitoring latencies](https://aerospike.com/docs/database/observe/latency) page for monitoring latencies and details about the histograms.