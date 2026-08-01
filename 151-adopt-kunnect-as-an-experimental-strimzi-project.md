# Adopt _Kunnect_ as a new experimental Strimzi project

This proposal suggests to adopt _Kunnect_ — a Kubernetes-native herder for Apache Kafka Connect — as a new experimental project under the Strimzi organization.
Kunnect runs every connector and every connector task as its own Kubernetes Pod and uses custom resources instead of the Kafka Connect REST API as its control plane.
It would live in its own GitHub repository with its own release cycle, its own CRD API group, and its own container image.
It would be clearly marked as experimental and would not be part of the Strimzi Cluster Operator, its installation files, or its release.

## Current situation

Strimzi supports Apache Kafka Connect through the `KafkaConnect` and `KafkaConnector` custom resources.
The `KafkaConnect` resource describes a Kafka Connect cluster.
Strimzi Cluster Operator creates a `StrimziPodSet` with one or more Pods, each running one Kafka Connect worker node in _distributed_ mode.
The worker nodes form a Connect cluster and use Kafka's group membership and the Connect REST API to elect a leader and to distribute the connectors and tasks between themselves.
This is implemented in Apache Kafka by the `DistributedHerder` class.

The `KafkaConnector` resource describes a single connector.
Strimzi Cluster Operator does not run the connector itself.
It translates the custom resource into calls to the Kafka Connect REST API of the corresponding Connect cluster and reads the connector and task status back from the same REST API.
The custom resource is in this case only a _front-end_ to the REST API which remains the actual control plane.

This design has some properties that follow directly from how the `DistributedHerder` works:

* All connectors and all tasks of a given Connect cluster share the worker JVMs.
  They share the heap, the CPU and memory limits of the Pod, the class path, and the `plugin.path` directory.
* All connectors of a given Connect cluster share a single Kafka identity.
  The credentials configured in the `KafkaConnect` custom resource are used by all connectors, so ACLs cannot be scoped to an individual connector.
* Connectors and tasks are scheduled by Kafka Connect and not by Kubernetes.
  Kubernetes schedules only the worker Pods and has no idea what runs inside them.
* Scaling happens at the granularity of the worker node.
  Adding a worker node adds capacity to the whole cluster, not to the connector that needs it.
* Any change to the Connect cluster — a new plugin, a new certificate, a configuration change, or a Strimzi upgrade — rolls the worker Pods and therefore disturbs every connector running on them.
* A connector or task that misbehaves (for example by leaking memory or by blocking a thread) affects the other connectors sharing the same worker node.

Strimzi already improved several aspects of this over the years.
Stable identities for the Connect worker nodes reduced the number of task rebalances during rolling updates.
Image volumes made it possible to add connector plugins without rebuilding the container image.
But the properties listed above are inherent to the `DistributedHerder` design.
They cannot be changed in the Strimzi operator code.

## Motivation

Kafka Connect's distributed mode was designed before Kubernetes existed and it implements its own cluster management.
It does its own membership, failure detection, scheduling, restarts, and configuration distribution through the config topic and the REST API.
When Kafka Connect runs on Kubernetes, all of these things are provided by Kubernetes as well and are implemented twice.

An alternative would be to keep only the parts of Kafka Connect that are about _connecting to systems_ — the plugins, the converters, the transformations, the error handling, the offset management — and let Kubernetes do the cluster management.
In such design:

* Every connector and every task runs in its own Pod, scheduled, restarted, and monitored by Kubernetes.
* Every connector can have its own container image, its own plugins, its own resource limits, its own `ServiceAccount`, and its own Kafka identity.
* A change to one connector rolls only the Pods of that connector.
* The custom resources are the control plane and not a front-end to a REST API.
* Scaling is done per connector by changing the number of its tasks.

Whether this is actually a better way to run Kafka Connect on Kubernetes is an open question.
It trades a small number of large Pods for a large number of small Pods, and that has its own costs.
It is a big change and it is not obvious that it should replace or complement what Strimzi does today.
Which is exactly why it should be explored as a separate experiment and not as a feature of the Strimzi Cluster Operator.

A working implementation of this idea already exists as the [Kunnect project](https://github.com/scholzj/kunnect).
Adopting it under the Strimzi organization would give it the visibility and the community feedback needed to evaluate the idea properly.

## Proposal

This proposal suggests to adopt the Kunnect project under the Strimzi umbrella as a new experimental project.
It will live in the Strimzi GitHub organization in a new repository called `kunnect`.

### What Kunnect is

Kunnect is an alternative Kafka Connect herder.
It does not modify or fork any Apache Kafka class.
It depends on the public Apache Kafka Maven artifacts (`connect-api`, `connect-runtime`, `connect-json`, `kafka-clients`) and extends Kafka Connect by composition only.
In particular, each Kunnect Pod builds and runs a real `org.apache.kafka.connect.runtime.Worker` instance.
That means the following are used as they are and are not reimplemented by Kunnect:

* Plugin discovery and class loader isolation (`Plugins` and `PluginClassLoader`)
* Converters, header converters, transformations, and predicates
* Error handling, retries, and dead letter queues
* Source offset storage in the Kafka offsets topic (the same topic layout the distributed herder uses)
* Sink consumer group offset commits
* Exactly-once source support

Kunnect consists of a single container image that can run in three different modes and of three custom resources:

| Custom resource | Created by | Purpose |
| :--- | :--- | :--- |
| `Kunnect` | User | Describes a logical Connect cluster — the Kafka bootstrap servers, the offsets topic, TLS, authentication, worker configuration, and cluster-wide plugins. |
| `KunnectConnector` | User | Describes a single connector — its class, configuration, number of tasks, plugins, its own authentication, and the Pod templates. |
| `KunnectConnectorTask` | Kunnect | Describes a single connector task as returned by `Connector#taskConfigs(...)`. Users do not create these. |

The three modes of the container image are:

1. The _controller_ mode runs the Kunnect operator.
   It watches the three custom resources and creates a `Secret` and a `Pod` for each connector and for each task.
2. The _connector_ mode runs a single `Connector` instance.
   It calls `Connector#taskConfigs(...)` and publishes one `KunnectConnectorTask` custom resource per task.
3. The _task_ mode runs a single `SourceTask` or `SinkTask`.

The flow is therefore:

```
Kunnect + KunnectConnector    ->  controller  ->  connector Pod
                                                       |
                                                       v
                                              KunnectConnectorTask
                                                       |
                                                       v
                                                  controller     ->  task Pod (one per task)
```

The configuration is never passed to the Pods through environment variables or command line arguments.
The controller resolves the `Kunnect` and `KunnectConnector` resources into a configuration document, stores it in a per-Pod `Secret`, and mounts the `Secret` into the Pod.
Kafka credentials therefore travel only through Secrets.

The following example shows the two user-facing custom resources:

```yaml
apiVersion: kunnect.io/v1
kind: Kunnect
metadata:
  name: my-connect
spec:
  bootstrapServers: my-cluster-kafka-bootstrap:9093
  tls:
    trustedCertificates:
      - fromSecret:
          secretName: my-cluster-cluster-ca-cert
          certificate: "*.crt"
  authentication:
    tls:
      certificate:
        fromSecret:
          name: my-connect-user
          key: user.crt
      key:
        fromSecret:
          name: my-connect-user
          key: user.key
  config:
    value.converter: org.apache.kafka.connect.json.JsonConverter
```

```yaml
apiVersion: kunnect.io/v1
kind: KunnectConnector
metadata:
  name: my-source
  labels:
    kunnect.io/cluster: my-connect
spec:
  class: io.example.MySourceConnector
  tasksMax: 3
  config:
    # ...
  plugins:
    - name: my-source-connector
      artifacts:
        - type: image
          reference: quay.io/my-org/my-source-connector:1.0.0
  template:
    taskContainer:
      resources:
        requests:
          cpu: 250m
          memory: 512Mi
```

The `plugins` section uses image volumes and follows the same structure as the one proposed for the `KafkaConnect` resource in [proposal 102](./102-using-image-volumes-to-improve-extensibility-of-Strimzi-operands.md).
The `tls`, `authentication`, and `template` sections deliberately follow the shape of the corresponding Strimzi APIs so that they are familiar to Strimzi users.
Because plugins can be configured per connector, two connectors on the same Kunnect cluster can use two different versions of the same plugin.

Kunnect uses the Strimzi CRD generator to generate the CRDs and the API reference documentation from the Java model classes.
This is a useful side effect of the adoption, because it validates that this tooling is usable outside of the Strimzi operators repository.

### A separate repository and a separate release cycle

Kunnect will be developed in a new repository named `kunnect` in the Strimzi GitHub organization.
It will not be part of the `strimzi-kafka-operator` repository and it will not be part of the Strimzi Cluster Operator release.
The reasons for keeping it separate are:

* It is an experiment.
  It might be abandoned or archived, and that should not require removing code from the operator repository or deprecating any part of the Strimzi API.
* It has its own release cadence.
  In the early phase it will need to release more often than Strimzi does and with fewer guarantees than a Strimzi release provides.
* It has its own dependencies and its own constraints.
  It requires Kubernetes 1.31 or newer with the `ImageVolume` feature gate and it currently builds against Apache Kafka 4.x only.
  The Strimzi operators support a wider range of Kubernetes and Kafka versions and should not be constrained by this.
* It does not share any code with the Cluster Operator, so there is no technical reason to keep it in the same repository.
* Keeping it separate makes it impossible for it to destabilize the Cluster Operator.

The following will be set up for the new repository:

* Apache License 2.0, the Strimzi Code of Conduct, and the Strimzi governance rules, as in any other Strimzi repository.
* A CI build using GitHub Actions, following the patterns from the `github-actions` repository.
* Container images pushed to `quay.io/strimzi/kunnect`.
* Its own versioning starting with `0.1.0` and its own release notes.
  Releases are made when there is something worth releasing and are not tied to the Strimzi operator releases.
* Installation files in the repository, but _not_ in the `install` directory of the operators repository.

Kunnect will use the `kunnect.io` API group and not the `kafka.strimzi.io` group.
This keeps it clearly separated from the Strimzi API and from the `v1` API work described in [proposal 113](./113-Strimzi-v1-CRD-API-and-1.0.0-release.md).
It also means that Kunnect CRDs can be installed next to the Strimzi CRDs in the same cluster without any conflict.

### What experimental means

The experimental status will be stated in the repository `README.md`, in the release notes, and on the website page, if any.
Concretely it means:

* Kunnect is not recommended for production use.
* The custom resource API can change in any release, including in incompatible ways.
  There is no API version graduation schedule and no conversion tooling.
* There is no supported upgrade path between Kunnect releases beyond deleting and recreating the resources.
* It is not covered by the Strimzi documentation, by the Strimzi system tests, or by the Strimzi operator support and upgrade guarantees.
* There is no promise that the project graduates or that it is maintained indefinitely.

The intention is to keep the experimental status until we have enough experience to answer the question the experiment asks.
At that point there are three possible outcomes:

1. Kunnect graduates into a regular Strimzi project with API stability guarantees, documentation, and system tests, and continues to be developed as a separate deployment option next to `KafkaConnect`.
2. Some of the ideas are taken over by the Cluster Operator and the `KafkaConnect` API, and Kunnect itself is archived.
3. The approach does not prove itself and the repository is archived, as was done with the Canary project in [proposal 086](./086-archive-canary.md).

Any of these will require a follow-up proposal.

### Relation to the Cluster Operator

Kunnect neither replaces nor deprecates anything.
The `KafkaConnect` and `KafkaConnector` custom resources and the Strimzi Cluster Operator continue to be developed exactly as they are today.
A user can run both in the same Kubernetes cluster and in the same namespace.

Kunnect does not require Strimzi.
It connects to any Apache Kafka cluster.
But when used with a Strimzi-managed Kafka cluster, it consumes the Secrets Strimzi creates — the cluster CA certificate Secret for TLS and the `KafkaUser` Secrets for authentication — in the same way the Strimzi operands do.

There will be no code dependency between the `strimzi-kafka-operator` repository and the `kunnect` repository in either direction.
The only shared artifact is the Strimzi CRD generator, which Kunnect consumes as a released Maven artifact.

### Current state and remaining work

The prototype is functional.
It supports the three custom resources, TLS encryption, TLS and SASL authentication with a separate identity for the Connect layer and for the connector, plugins mounted from image volumes, Pod and container templates including resources and additional volumes, automatic creation of least-privilege `ServiceAccounts` for the connector Pods, status reporting through the `.status` subresource, and the `scale` subresource on the connector resource.
Configuration changes are rolled out by recreating the affected Pods based on hash annotations.

The main things that are missing and that will be worked on after the adoption are:

* Leader election so that the controller can run with more than one replica
* Health probes and metrics for the controller and for the connector and task Pods
* Watching the referenced Secrets so that certificate and credential rotation is picked up immediately
* Connector lifecycle operations such as pause, resume, and restart
* System tests
* Documentation

None of these are blockers for adopting the project.
They are, however, blockers for any future graduation.

### Testing strategy

The project currently has unit tests covering the operator logic — the projection of the custom resources into the per-Pod configuration, the Pod and Secret construction, the drift detection, the certificate and credential resolution, the worker configuration, the status mirroring, and the publishing of the task resources.
The Fabric8 `KubernetesMockServer` is used where the Kubernetes API is involved.

A `systemtests` module will be added to the same repository.
It will run against a real Kubernetes cluster and a real Kafka cluster and it will cover the parts that unit tests cannot cover — the connector and task Pods driving a real `Worker`.
The Strimzi `test-container` project can be used to provide the Kafka cluster.
Kunnect system tests will not be added to the system tests of the operators repository.

## Affected/not affected projects

This proposal creates a new repository `kunnect` in the Strimzi GitHub organization.

The `strimzi-kafka-operator` repository is not affected.
No code changes, no CRD changes, and no changes to the installation files are needed there.

The `strimzi.github.io` repository may be affected if we decide to list the new project on the website or to publish a blog post about it.
The `governance` and `.github` repositories are affected only by adding the new repository to the lists they maintain.
The `github-actions` repository may be reused for the CI, but does not need to change.

## Compatibility

There is no impact on backwards compatibility.
Kunnect is a new project with a new API group and it does not change any existing behaviour or any existing custom resource.
Users who do not install it are not affected in any way.

Within the Kunnect project itself, no backwards compatibility is guaranteed while the project is experimental.

## Rejected alternatives

### Implementing this inside the Strimzi Cluster Operator

Implementing a per-Pod connector runtime as an alternative mode of the `KafkaConnect` resource, guarded by a feature gate, was considered.
This was rejected for now.
It would add a second, very different Connect implementation into an operator that is at the same time working towards the `v1` CRD API and the 1.0.0 release.
It would also mean that the API for the new mode has to be designed only once, up front, and then maintained, because it would immediately be part of the Strimzi API surface.
The whole point of the experiment is to be able to change the API freely while learning what the right API is.
If the experiment succeeds, this remains a possible outcome and would be covered by a follow-up proposal.

### Keeping the project outside of Strimzi

The project could stay where it is today as a personal repository and be listed in the `awesome-strimzi` repository.
This was rejected because the question the project asks — is this a better way to run Kafka Connect on Kubernetes — is a question about the future direction of Strimzi.
Answering it needs the review, the users, and the discussion that come with being part of the project.
A listing in `awesome-strimzi` does not provide that.

### Contributing this to Apache Kafka

A Kubernetes-native herder cannot be contributed to Apache Kafka.
Apache Kafka cannot take a dependency on Kubernetes and on a Kubernetes client library, and the herder implementation is only useful on Kubernetes.
The extension points Kunnect uses are the public Kafka Connect APIs, and no change to Apache Kafka is required.

### Forking or patching Kafka Connect

Reimplementing or forking parts of `connect-runtime` would have made some things easier, in particular the parts of `Worker` and `Herder` that assume a herder with a full cluster view.
This was rejected as an explicit design constraint of the project.
Everything is built on top of the public Kafka artifacts by composition, so that Kunnect can follow Apache Kafka releases without carrying patches.

## Additional resources

* The current Kunnect prototype: [https://github.com/scholzj/kunnect](https://github.com/scholzj/kunnect)
* A short demo of the prototype: [https://youtu.be/PbycQyk9dJs](https://youtu.be/PbycQyk9dJs)
* [Proposal 102](./102-using-image-volumes-to-improve-extensibility-of-Strimzi-operands.md) describing the image volumes used by Kunnect for the connector plugins
* [Proposal 045](./045-Stable-identities-for-Kafka-Connect-worker-nodes.md) describing the current Kafka Connect deployment model in Strimzi
