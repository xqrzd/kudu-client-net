# .NET Client for Apache Kudu
![Apache Kudu](https://d3dr9sfxru4sde.cloudfront.net/i/k/apachekudu_logo_0716_345px.png)

## What is Kudu?
Kudu provides a combination of fast inserts/updates and efficient columnar scans to enable multiple real-time analytic workloads across a single storage layer. As a new complement to HDFS and Apache HBase, Kudu gives architects the flexibility to address a wider variety of use cases without exotic workarounds.

[https://kudu.apache.org](https://kudu.apache.org)

## Project Status
This client is in preview and isn't quite ready for production use.
Issue [#17](/../../issues/17) tracks the remaining work for the first stable release.

You can get the preview package on [Nuget](https://www.nuget.org/packages/Knet.Kudu.Client).

## Supported Versions
This library supports Apache Kudu 1.3 and newer. The newest version of this library
should always be used, regardless of the Apache Kudu version.
This client tries to maintain feature parity with the official C++ and Java clients.

## Quickstart

### Docker
Follow the [Apache Kudu Quickstart](https://kudu.apache.org/docs/quickstart.html) guide to get Kudu running in Docker.

### Create a Client

```csharp
KuduClient client = KuduClient.NewBuilder("localhost:7051,localhost:7151,localhost:7251")
    .Build();
```

> Note: KuduClient is intended to be a singleton. It is inefficient to create multiple clients.

### Create a Table

```csharp
var tableBuilder = new TableBuilder("twitter_firehose")
    .AddColumn("tweet_id", KuduType.Int64, opt => opt.Key(true))
    .AddColumn("user_name", KuduType.String)
    .AddColumn("created_at", KuduType.UnixtimeMicros)
    .AddColumn("text", KuduType.String);

await client.CreateTableAsync(tableBuilder);
```

See more table options in the [wiki](https://github.com/xqrzd/kudu-client-net/wiki/Create-Table).

### Open a Table

```csharp
KuduTable table = await client.OpenTableAsync("twitter_firehose");
```

> Note: This table can be cached and reused concurrently. It simply stores the table schema.

### Insert Data

```csharp
var rows = Enumerable.Range(0, 100).Select(i =>
{
    var row = table.NewInsert();
    row.SetInt64("tweet_id", i);
    row.SetString("user_name", $"user_{i}");
    row.SetDateTime("created_at", DateTime.UtcNow);
    row.SetString("text", $"sample tweet {i}");
    return row;
});

await client.WriteAsync(rows);
```

### Query Data

```csharp
KuduScanner scanner = client.NewScanBuilder(table)
    .SetProjectedColumns("tweet_id", "user_name", "created_at")
    .Build();

await foreach (ResultSet resultSet in scanner)
{
    Console.WriteLine($"Received {resultSet.Count} rows");
    PrintRows(resultSet);
}

static void PrintRows(ResultSet resultSet)
{
    foreach (RowResult row in resultSet)
    {
        var tweetId = row.GetInt64("tweet_id");
        var userName = row.GetString("user_name");
        var createdAtUtc = row.GetDateTime("created_at");

        Console.WriteLine($"tweet_id: {tweetId}, user_name: {userName}, created_at: {createdAtUtc}");
    }
}
```
