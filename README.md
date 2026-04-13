CMAK (Cluster Manager for Apache Kafka) — Fork with KRaft & Kafka 4.x Support
=============

> **This is a fork of [yahoo/CMAK](https://github.com/yahoo/CMAK).**
> The original project managed Kafka clusters via ZooKeeper. This fork removes the ZooKeeper
> dependency entirely and adds support for **Kafka 4.x in KRaft mode**, including the latest
> **Apache Kafka 4.1.0**. Cluster configurations are stored in a local JSON file and broker
> connections use the Kafka `AdminClient` with bootstrap servers.
>
> Upstream version forked: **3.0.0.7** — this release: **3.0.1**

---

CMAK (previously known as Kafka Manager) is a tool for managing [Apache Kafka](http://kafka.apache.org) clusters.

CMAK supports the following:

 - Manage multiple clusters
 - Easy inspection of cluster state (topics, consumers, offsets, brokers, replica distribution, partition distribution)
 - Run preferred replica election
 - Generate partition assignments with option to select brokers to use
 - Run reassignment of partition (based on generated assignments)
 - Create a topic with optional topic configs
 - Delete topic
 - Topic list indicates topics marked for deletion
 - Batch generate partition assignments for multiple topics with option to select brokers to use
 - Batch run reassignment of partition for multiple topics
 - Add partitions to existing topic
 - Update config for existing topic
 - Optionally enable JMX polling for broker level and topic level metrics

Cluster Management

![cluster](/img/cluster.png)

***

Topic List

![topic](/img/topic-list.png)

***

Topic View

![topic](/img/topic.png)

***

Consumer List View

![consumer](/img/consumer-list.png)

***

Consumed Topic View

![consumer](/img/consumed-topic.png)

***

Broker List

![broker](/img/broker-list.png)

***

Broker View

![broker](/img/broker.png)

---

What Changed in This Fork
-------------------------

| Area | Change |
|---|---|
| **Kafka versions** | Added 3.3 through 4.1.0 (KRaft); dropped ZooKeeper requirement |
| **Cluster persistence** | Cluster configs saved to a JSON file (env: `CMAK_CLUSTER_CONFIG_FILE`) |
| **Docker** | Multi-stage `Dockerfile` — no local Java/SBT needed |
| **Compose files** | `docker-compose-cluster.yml` (3-broker KRaft) and `docker-compose.yml` (source/target) |
| **Default auth** | Basic auth disabled by default; enable via env vars |

---

Requirements
------------

1. Apache Kafka 2.x or 3.x or 4.x (KRaft mode supported from 3.0+)
2. Java 11+

---

Docker Tutorial
---------------

No local Java or SBT is required. Everything builds inside Docker.

### Step 1 — Build the image

```bash
docker build -t cmak:3.0.1 .
```

The `Dockerfile` is a multi-stage build:

- **Stage 1** uses `sbtscala/scala-sbt` to compile the project and produce a distribution zip.
- **Stage 2** uses a slim `eclipse-temurin:11-jre` image and unpacks the zip.

Alternatively, if you want to compile locally first and only Docker-ize the result:

```bash
# 1. compile (requires local SBT + Java 11)
sbt dist

# 2. build runtime image from pre-built zip
docker build -t cmak:3.0.1 -f Dockerfile.runtime .
```

Or use the convenience script (does both steps without local Java/SBT):

```bash
./build-docker.sh
```

### Step 2 — Choose a compose file

Two Docker Compose setups are provided. Pick the one that fits your use case.

#### Option A: 3-broker KRaft cluster (`docker-compose-cluster.yml`)

Starts three Kafka 4.1 brokers, CMAK, and Kafbat UI. Good for testing partition
reassignment, replication, and multi-broker scenarios.

```bash
docker compose -f docker-compose-cluster.yml up -d
```

| Service     | URL / Address              | Notes                        |
|-------------|----------------------------|------------------------------|
| CMAK        | http://localhost:9000      | Kafka manager UI             |
| Kafbat UI   | http://localhost:8090      | Read-only visual UI          |
| Kafka 1 JMX | localhost:9991             | JMX port for broker 1        |

#### Option B: Source → Target clusters (`docker-compose.yml`)

Starts two independent single-broker clusters. Good for testing MirrorMaker 2
or cross-cluster replication.

```bash
docker compose up -d
```

| Service      | URL / Address              | Notes                        |
|--------------|----------------------------|------------------------------|
| CMAK         | http://localhost:9000      | Kafka manager UI             |
| Kafbat UI    | http://localhost:8090      | Read-only visual UI          |
| kafka-source | localhost:9092             | Source cluster bootstrap     |
| kafka-target | localhost:9094             | Target cluster bootstrap     |

### Step 3 — Add your first cluster in the UI

CMAK starts with an empty cluster registry. Register each cluster manually:

1. Open **http://localhost:9000**
2. Click **Cluster → Add Cluster**
3. Fill in the form:

| Field              | 3-broker cluster value                      | Source/Target value                           |
|--------------------|---------------------------------------------|-----------------------------------------------|
| Cluster Name       | `kafka-cluster` (any name)                  | `kafka-source` / `kafka-target`               |
| Cluster Version    | `4.1.0`                                     | `4.1.0`                                       |
| Bootstrap Servers  | `kafka-1:29092,kafka-2:29092,kafka-3:29092` | `kafka-source:29092` / `kafka-target:39094`   |
| Enable JMX Polling | checked                                     | unchecked                                     |

4. Click **Save** — the cluster appears in the list and CMAK starts polling it.

> **Important**: use the internal Docker service names (`kafka-1`, `kafka-source`, etc.)
> as bootstrap servers, not `localhost`. `localhost` resolves to the CMAK container itself.

### Step 4 — Environment variables

| Variable                     | Default                                      | Description                                        |
|------------------------------|----------------------------------------------|----------------------------------------------------|
| `APPLICATION_SECRET`         | **required in production** (see note)        | Play Framework secret key — use `openssl rand -base64 48` |
| `CMAK_CLUSTER_CONFIG_FILE`   | `<app_home>/conf/cmak-clusters.json`         | Path where cluster configs are persisted           |
| `ZK_HOSTS`                   | —                                            | Ignored in KRaft mode; kept for compatibility      |
| `KAFKA_MANAGER_AUTH_ENABLED` | `false`                                      | Enable HTTP basic auth                             |
| `KAFKA_MANAGER_USERNAME`     | `admin`                                      | Basic auth username                                |
| `KAFKA_MANAGER_PASSWORD`     | `password`                                   | Basic auth password                                |

> **Security note**: always set `APPLICATION_SECRET` to a random value in production.
> Generate one with: `openssl rand -base64 48`

### Step 5 — Cluster configuration persistence

Cluster configs are saved to `CMAK_CLUSTER_CONFIG_FILE`. Both compose files mount a named
volume so configs survive container restarts:

```yaml
environment:
  CMAK_CLUSTER_CONFIG_FILE: /opt/cmak-data/cmak-clusters.json
volumes:
  - cmak-data:/opt/cmak-data
```

> Do not mount a volume over `/opt/cmak-3.0.1/conf` — that directory contains
> `application.conf` and `logback.xml` and must remain from the image.

### Step 6 — Stopping and restarting

```bash
# Stop without removing containers/volumes (clusters preserved on restart)
docker compose -f docker-compose-cluster.yml stop

# Stop and remove containers (volume is kept, clusters preserved)
docker compose -f docker-compose-cluster.yml down

# Stop and destroy everything including the cluster config volume
docker compose -f docker-compose-cluster.yml down -v
```

### Troubleshooting

**Container exits immediately with no logs**

Play Framework leaves a `RUNNING_PID` file behind when the JVM is killed hard. On the next
start it refuses to launch:

```
This application is already running (Or delete /opt/cmak-3.0.1/RUNNING_PID file).
```

Fix — remove and recreate the container so the writable layer is reset:

```bash
docker rm cmak
docker compose -f docker-compose-cluster.yml up -d cmak
```

**"Cluster config file not found" in logs**

This is a normal INFO message logged every 10 seconds until you add a cluster via the UI.
It does not indicate a problem.

**CMAK starts but shows no data after adding a cluster**

Verify the bootstrap servers use Docker service names, not `localhost`:

```bash
# correct
kafka-1:29092,kafka-2:29092,kafka-3:29092

# wrong — resolves to the CMAK container itself
localhost:9092
```

---

Configuration
-------------

The minimum configuration is the zookeeper hosts for CMAK state (legacy) or `CMAK_CLUSTER_CONFIG_FILE`
(KRaft mode). Settings are in `conf/application.conf` and can also be passed via environment variables.

You can optionally enable/disable features:

    application.features=["KMClusterManagerFeature","KMTopicManagerFeature","KMPreferredReplicaElectionFeature","KMReassignPartitionsFeature"]

 - KMClusterManagerFeature — add, update, delete clusters
 - KMTopicManagerFeature — add, update, delete topics
 - KMPreferredReplicaElectionFeature — run preferred replica election
 - KMReassignPartitionsFeature — generate and run partition reassignments

Consider setting these parameters for larger clusters with JMX enabled:

 - cmak.broker-view-thread-pool-size=< 3 * number_of_brokers>
 - cmak.broker-view-max-queue-size=< 3 * total # of partitions across all topics>
 - cmak.broker-view-update-seconds=< cmak.broker-view-max-queue-size / (10 * number_of_brokers) >

### Authenticating with LDAP

> Warning: configure SSL to prevent credentials from being passed unencrypted.

1. Enable basic authentication:
   - `basicAuthentication.enabled=true`
   - `basicAuthentication.realm=<realm>`

2. Enable LDAP:
   - `basicAuthentication.ldap.enabled=true`
   - `basicAuthentication.ldap.server=<fqdn>`
   - `basicAuthentication.ldap.port=389`
   - `basicAuthentication.ldap.username=<search DN>`
   - `basicAuthentication.ldap.password=<search password>`
   - `basicAuthentication.ldap.search-base-dn=<base DN>`
   - `basicAuthentication.ldap.search-filter=<filter>`

3. Optional — limit to a group:
   - `basicAuthentication.ldap.group-filter=<group DN>`

4. Optional — StartTLS or LDAPS:
   - `basicAuthentication.ldap.starttls=true`
   - `basicAuthentication.ldap.ssl=true`

---

Deployment (without Docker)
----------------------------

Build a distribution zip:

    ./sbt clean dist

Refer to [Play Framework production docs](https://www.playframework.com/documentation/2.4.x/ProductionConfiguration) for deployment details.

Starting the service:

    $ bin/cmak

    # custom port and config
    $ bin/cmak -Dconfig.file=/path/to/application.conf -Dhttp.port=8080

    # SASL/JAAS
    $ bin/cmak -Djava.security.auth.login.config=/path/to/my-jaas.conf

---

Packaging
---------

    sbt debian:packageBin
    sbt rpm:packageBin

---

Credits
-------

Most utility code was adapted from [Apache Kafka](http://kafka.apache.org) to work with
[Apache Curator](http://curator.apache.org).

This fork is based on [yahoo/CMAK](https://github.com/yahoo/CMAK) — originally developed
by employees at Yahoo / Verizon Media.

License
-------

Licensed under the Apache License 2.0. See accompanying LICENSE file for terms.

Consumer/Producer Lag
---------------------

Producer offset is polled. Consumer offset is read from the `__consumer_offsets` topic for
Kafka-managed consumers. Reported lag may occasionally be negative because the consumer
offset is read faster than the producer offset is polled — this is normal.
