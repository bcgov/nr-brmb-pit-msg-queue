# BC Permanent Pacific Time (PCT, UTC-7): Impact Report for nr-brmb-pit-msg-queue

| | |
|---|---|
| **Prepared** | 2026-10-07 |
| **Repo state** | public GitHub `dhlevi/nr-brmb-pit-msg-queue`, branch `master` @ `2b496e8` (2026-08-14, "Merge pull request #12 from bcgov/release/1.1.0"). This commit is identical to `master` in the upstream `bcgov/nr-brmb-pit-msg-queue` repository. |
| **Components** | NATS JetStream message queue for the PIT microservices: Helm chart `server/nats-chart` (NATS server `2.12.6-alpine`, config reloader 0.21.1, Prometheus exporter 0.18.0), stream setup scripts in `server/deploy-tools`, an OpenShift route, one GitHub deployment workflow, and two Java 21 example clients (`client`, `jnats` 2.25.1, 414 lines). |
| **Deadline** | **Sunday 2026-11-01, 02:00 local (09:00 UTC)**, about 3.5 weeks away |
| **Bottom line** | **No impact.** The repository configures message transport only. NATS keeps its internal timestamps in UTC, the streams use interest-based retention with no age limit, and consumer redelivery uses fixed durations (5 seconds to 60 minutes). Nothing depends on the BC time zone rules. Date and time values inside the messages are defined by the publishing and consuming applications (claims, policies and underwriting), which are covered in their own reports. |

---

## 1. What changed (same basis as the earlier reports)

- **Government rule.** BC stopped changing clocks after 2026-03-08. On **2026-11-01 clocks do not fall back.** BC stays at **UTC-7** all year, named *Pacific time (PCT)*.
- **IANA tzdata 2026b** models `America/Vancouver` as permanent UTC-7 from 2026-11-01 02:00.
- **JDK builds with the rule:** 8u501, 11.0.32, 17.0.20, 21.0.12 and 25.0.4 or later.

---

## 2. Summary of findings

| # | Area | Severity | Fails on Nov 1? | Fix |
|---|---|---|---|---|
| Q1 | NATS server and JetStream (stream and consumer metadata, message timestamps) | None | No; NATS stores times as UTC | n/a |
| Q2 | Stream retention and consumer redelivery settings in `setup_nats_stream.sh` | None | No; interest retention and fixed backoff durations | n/a |
| Q3 | Helm chart, OpenShift route and deployment workflow | None | No; no time zone setting, scheduled job or cron expression | n/a |
| Q4 | Example Java clients (`NatsPublish`, `NatsSubscribe`) | None | No; only `java.time.Duration` values | n/a |
| Q5 | Date and time values carried in message payloads | Out of scope | Depends on the publishers and consumers | See the application reports (R1) |

---

## 3. Overview

The repository deploys a three-stream NATS JetStream cluster (`policies-event-channel`, `claims-event-channel` and `underwriting-event-channel`) with two pull consumers per stream, and grants each PIT microservice its publish and subscribe permissions. It contains no application logic that creates or interprets dates.

---

## 4. Areas of Concern

### Q1. NATS server: none

The NATS server is written in Go and records stream, consumer and message timestamps in UTC (RFC 3339 with `Z`). The `nats:2.12.6-alpine` image sets no `TZ`, and the chart does not add one. Monitoring output and logs are therefore in UTC; anyone reading them should expect UTC rather than local time, before and after the change.

### Q2. Retention and redelivery: none

`server/deploy-tools/setup_nats_stream.sh`:

- Streams use `--retention=interest --discard=new`, so messages are kept until every consumer acknowledges them. There is no `--max-age`, so no wall-clock expiry exists.
- Consumers use `--backoff=linear --backoff-steps=20 --backoff-min=5s --backoff-max=60m --max-deliver=150`. These are elapsed durations, unaffected by any clock change.

### Q3. Deployment configuration: none

`server/deploy-tools/override-nats-values.yaml`, `server/nats-chart/values.yaml`, `openshift/msg-queue-route.yaml` and `.github/workflows/openshift-deploy-nats-chart.yml` contain no time zone settings, cron schedules or date logic.

### Q4. Example clients: none

`client/.../NatsPublish.java` and `NatsSubscribe.java` use `java.time.Duration` for connection and acknowledgement timeouts and receive an epoch `Long` in the connection event callback. They are examples and are not deployed.

### Q5. Message payloads: out of scope

Payloads carry whatever the publishing application serializes, such as event timestamps from `EventPublisher` in `nr-brmb-pit-claim`. Their correctness depends on the JVM and serialization settings of those applications, not on the queue.

---

## 5. Areas of Failure

None found.

---

## 6. Pre-existing Defects Found (independent of this change)

None found.

---

## 7. Potential Resolutions

| # | Action | Owner | Priority |
|---|--------|-------|----------|
| R1 | No change is required in this repository. Apply the platform fixes (JDK with tzdata 2026b, explicit `-Duser.timezone=America/Vancouver`) to the publishing and consuming applications, as described in their reports. | Consuming teams | Before 2026-11-01 |
| R2 | Optionally note in the README that NATS logs and monitoring timestamps are UTC. | Development | Optional |

---

## 8. Cross-References

- **nr-brmb-pit-claim** report: `cirras-claims-api` publishes to `claims-event-channel` (`EventPublisherSpringConfig.java` line 65); its event timestamps and sync dates are covered there. The queue permissions also define a `claims_sync_rest_consumer` on the policies and underwriting streams, which belongs to a claims sync service outside that repository.
