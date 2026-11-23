# Cross-host actor recovery trial

Can two application hosts submit changes to one catalog, lose contact with its owner, and continue from the acknowledged changes on a surviving host? This fixture exercises a string-keyed Orleans grain and an Akka.NET sharded entity with the same catalog key, journal and failure scenario. The comparison project runs independently of the HostLoom solution.

## Observed on 2026-11-23 UTC

| Runtime | Successful fresh clusters | Seconds from peer disconnect to first replacement read |
|---|---:|---:|
| Orleans 10.4.2 | 2 of 2 | 28.02, 24.08 |
| Akka.NET 1.5.75 | 2 of 2 | 19.33, 17.29 |

All four runs retained the two pre-partition acknowledgements in order and finished with the five expected entries. In each run the former credential successfully appended once while the replacement could already serve reads. Its next append was refused after the two surviving ingress nodes confirmed the replacement. Both surviving hosts then read the same journal. The two configurations have different failure-detector settings. These times describe the individual trials and have no production recovery target attached. [Per-run ownership and journal records](results-2026-11-23.json)

The environment was one Docker Desktop Linux ARM64 VM, Engine 29.9.3, application runtime .NET 10.0.14 and Redis 7.4.11. The SDK and Redis image digests were `sha256:212fe4cb74627d5adf9817a4088906ae3edff1bd853bb27565d4b46731efe61e` and `sha256:71da9275c5f3fcb97d0fa0c8c5b36cc995327265420f17a04bfd544f458059f7`. Each run began with new networks, members and storage.

## Reproduce

Use the repository's .NET 10 SDK, Python 3.10 or newer, and Docker with Linux containers. Run from the repository root, using a fresh output directory for each trial.

```sh
python3 benchmarks/actor-cluster/run.py --output artifacts/actor-cluster/trial-01
```

Use `--runtime orleans` or `--runtime akka` to run one case. Each case starts three application processes in separate containers and a private Redis instance. Only the three processes join the peer network. A second network carries the test requests, membership storage for Orleans and journal access. Neither network publishes host ports. The runner removes its uniquely named containers, volumes attached to them and networks on exit. It saves per-node logs and the acknowledged history in the selected output directory.

Orleans uses Redis membership, a two-second probe timeout, three missed probes and two votes to declare a silo dead. Akka.NET uses distributed-data shard coordination and its static quorum split-brain resolver with quorum two and a five-second stability interval. The entity reads its state from the journal on each read. Redis uses append-only logging with fsync on every write. [Orleans membership settings](https://learn.microsoft.com/en-us/dotnet/api/orleans.configuration.clustermembershipoptions) · [Akka.NET cluster sharding](https://getakka.net/articles/clustering/cluster-sharding.html) · [Akka.NET split-brain resolver](https://getakka.net/articles/clustering/split-brain-resolver.html)

## What the experiment checks

1. Wait until all three nodes report three members. Submit reads through two nodes. Both must reach the same activation of the same key. Submit two concurrent updates through those nodes and retain the acknowledgements and journal order.
2. Disconnect the owner's peer interface and verify that its peer port is unreachable in both directions from each survivor. Leave its path to Redis intact. One surviving ingress repeatedly reads the key until it reaches a replacement. That read must return both acknowledged changes in their original order.
3. Try to commit from the replacement before the second surviving ingress has confirmed it. The store must reject that write. Replay the former owner's credential from its isolated container. The store must still accept it at this point. Read through the second survivor and verify that it sees that additional commit and the same replacement.
4. Once both surviving ingress nodes have independently observed the replacement, try another old-credential write while issuing two new updates through the survivors. The store must reject the old credential and accept both new updates. Retry an earlier operation ID. It must add zero entries. Read through both surviving hosts and compare the exact five-entry journal.

The peer disconnection exercises the real runtime's failure detection and rerouting. The old-credential replay runs in a separate process in the original container and calls the same commit function as the actor. This keeps the stale-write check executable even if the isolated runtime shuts itself down. The reported interval runs from the disconnect command to the first successful replacement read and includes the connectivity checks and the driver's polling delay. Write authority transfers after that read.

## Who enforces single-writer effects

Each activation generates a fresh writer token. After routing a read, each ingress records which activation replied. Redis transfers authority when two distinct ingress nodes have observed the same candidate during the current writer's tenure. The transfer compares the writer observed before the read, retires the old token and clears the observations. A single atomic Redis script checks the token, deduplicates the operation ID and appends the change. The runner observes the transfer and never grants a writer token itself. [Transfer logic](witness.lua) · [Commit logic](commit.lua)

This application protocol assumes a fixed set of three trusted nodes and a single available Redis authority. A surviving activation can serve recovered reads while its writes are still being refused. An application would need to define how long that interval may last and how to drive confirmation in a write-only workload. Redis failover or data loss, host replacement with new identities, arbitrary partition histories and cross-system side effects require separate evidence. The duplicate check covers a retried acknowledged operation ID. The trial uses a shared journal, so it cannot qualify either framework's native persistence provider.
