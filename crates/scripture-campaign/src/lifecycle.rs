//! Ephemeral run-namespace lifecycle (WP05 v3 isolation).
//!
//! Creates `scripture-correctness-<run-id>` with:
//! - dedicated RustFS Deployment/Service (emptyDir; never hostPath/PVC)
//! - per-run Secret + bucket
//! - default-deny egress (DNS + in-namespace RustFS only)
//! - temporary bootstrap/promote actor pods (labeled as temporary)
//!
//! Tracker RustFS / scripture-lab are never selected. Cleanup deletes only the
//! run namespace.

use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use getrandom::fill as fill_random;
use serde::Serialize;

use crate::profile::RustFsHomeFleetProfile;

const RUSTFS_IMAGE: &str = "rustfs/rustfs:1.0.0-beta.8";

/// Redacted identity of the run-owned store (never Tracker).
#[derive(Debug, Clone, Serialize)]
pub struct IsolatedStoreIdentity {
    /// Run namespace.
    pub namespace: String,
    /// In-namespace Service name.
    pub service: String,
    /// Service UID observed after create.
    pub service_uid: String,
    /// Service DNS used by campaign pods.
    pub service_dns: String,
    /// Bucket name (run-scoped).
    pub bucket: String,
    /// Node hosting the ephemeral RustFS pod.
    pub rustfs_node: String,
    /// Labels proving run ownership.
    pub labels: BTreeMapString,
}

/// String map alias for serde.
type BTreeMapString = std::collections::BTreeMap<String, String>;

/// Placement evidence for one actor pod.
#[derive(Debug, Clone, Serialize)]
pub struct ActorPlacement {
    /// Pod name.
    pub name: String,
    /// Pod UID.
    pub uid: String,
    /// Scheduled node.
    pub node: String,
    /// Role label.
    pub role: String,
    /// Adapter label (`temporary-bootstrap-promote`).
    pub adapter: String,
}

/// Handle for an active run namespace.
#[derive(Debug)]
pub struct RunLifecycle {
    /// kubectl context.
    pub kube_context: String,
    /// Run id.
    pub run_id: String,
    /// Namespace name.
    pub namespace: String,
    /// Isolated store identity (redacted).
    pub store: IsolatedStoreIdentity,
    /// Generated access key (never written to artifacts).
    access_key: String,
    /// Generated secret key (never written to artifacts).
    secret_key: String,
    /// Whether cleanup should run on drop.
    cleanup_on_drop: bool,
}

impl Drop for RunLifecycle {
    fn drop(&mut self) {
        if self.cleanup_on_drop {
            let _ = self.cleanup();
        }
    }
}

impl RunLifecycle {
    /// Creates the isolated run namespace and ephemeral RustFS.
    pub fn create(
        profile: &RustFsHomeFleetProfile,
        run_id: &str,
        keep_failed: bool,
    ) -> Result<Self, LifecycleError> {
        profile
            .validate_isolation()
            .map_err(|error| LifecycleError::Isolation(error.to_string()))?;
        let namespace = profile.run_namespace(run_id);
        refuse_outside_namespace_mutations(&namespace)?;

        let access_key = generate_token("ak", 24)?;
        let secret_key = generate_token("sk", 32)?;
        let bucket = format!("campaign-{}", crate::profile::sanitize_k8s_label(run_id));

        apply_yaml(&profile.kube_context, &namespace_yaml(run_id, &namespace))?;

        // Credentials via kubectl create — values never appear in applied YAML files on disk.
        create_secret(
            &profile.kube_context,
            &namespace,
            "rustfs-credentials",
            &[
                ("access_key", &access_key),
                ("secret_key", &secret_key),
                ("RUSTFS_ACCESS_KEY", &access_key),
                ("RUSTFS_SECRET_KEY", &secret_key),
            ],
            run_id,
        )?;

        apply_yaml(
            &profile.kube_context,
            &rustfs_manifest(run_id, &namespace, &profile.rustfs_node),
        )?;
        apply_yaml(
            &profile.kube_context,
            &network_policy_yaml(run_id, &namespace),
        )?;

        wait_for_deployment(
            &profile.kube_context,
            &namespace,
            "rustfs",
            Duration::from_secs(180),
        )?;

        let service_uid = resource_uid(&profile.kube_context, &namespace, "service", "rustfs")?;
        verify_run_owned_service(&profile.kube_context, &namespace, run_id, &service_uid)?;

        let service_dns = format!("http://rustfs.{namespace}.svc.cluster.local:9000");
        create_bucket_job(
            &profile.kube_context,
            &namespace,
            run_id,
            &bucket,
            &access_key,
            &secret_key,
            &service_dns,
        )?;

        let mut labels = BTreeMapString::new();
        labels.insert("scripture.dev/run-id".into(), run_id.to_owned());
        labels.insert("app.kubernetes.io/name".into(), "rustfs".into());
        labels.insert(
            "scripture.dev/purpose".into(),
            "ephemeral-campaign-store".into(),
        );

        Ok(Self {
            kube_context: profile.kube_context.clone(),
            run_id: run_id.to_owned(),
            namespace: namespace.clone(),
            store: IsolatedStoreIdentity {
                namespace,
                service: "rustfs".into(),
                service_uid,
                service_dns,
                bucket,
                rustfs_node: profile.rustfs_node.clone(),
                labels,
            },
            access_key,
            secret_key,
            cleanup_on_drop: !keep_failed,
        })
    }

    /// Disables automatic cleanup (for --keep-failed after a failure).
    pub fn retain(&mut self) {
        self.cleanup_on_drop = false;
    }

    /// Deletes only this run namespace.
    pub fn cleanup(&mut self) -> Result<(), LifecycleError> {
        refuse_outside_namespace_mutations(&self.namespace)?;
        kubectl(
            &self.kube_context,
            &["delete", "namespace", &self.namespace, "--wait=false"],
        )?;
        self.cleanup_on_drop = false;
        Ok(())
    }

    /// Writes redacted store identity (no credentials).
    pub fn write_store_identity(&self, dir: &Path) -> Result<(), LifecycleError> {
        std::fs::create_dir_all(dir)?;
        std::fs::write(
            dir.join("isolated-store.json"),
            serde_json::to_vec_pretty(&self.store)?,
        )?;
        Ok(())
    }

    /// Deploys temporary bootstrap actor A on a scenario-scoped HA store prefix.
    pub fn deploy_actor_a(
        &self,
        profile: &RustFsHomeFleetProfile,
        scenario: &str,
    ) -> Result<ActorPlacement, LifecycleError> {
        self.deploy_actor(
            profile,
            scenario,
            ActorDeploy {
                name: "scripture-actor-a",
                role: "writer-a",
                owner_id: "scripture-own-a!",
                node: &profile.writer_a_node,
                configmap: "scripture-actor-a-config",
                args: &[
                    "bootstrap",
                    "--config",
                    "/etc/scripture/scripture.yaml",
                    "--initial-term",
                    "1",
                ],
            },
        )
    }

    /// Deploys temporary promote actor B on the same scenario-scoped HA prefix.
    pub fn deploy_actor_b_promote(
        &self,
        profile: &RustFsHomeFleetProfile,
        scenario: &str,
        candidate_term: u64,
    ) -> Result<ActorPlacement, LifecycleError> {
        let term = candidate_term.to_string();
        let args_owned = [
            "promote".to_owned(),
            "--config".to_owned(),
            "/etc/scripture/scripture.yaml".to_owned(),
            "--candidate-term".to_owned(),
            term,
        ];
        let args_ref = [
            args_owned[0].as_str(),
            args_owned[1].as_str(),
            args_owned[2].as_str(),
            args_owned[3].as_str(),
            args_owned[4].as_str(),
        ];
        self.deploy_actor(
            profile,
            scenario,
            ActorDeploy {
                name: "scripture-actor-b",
                role: "writer-b",
                owner_id: "scripture-own-b!",
                node: &profile.writer_b_node,
                configmap: "scripture-actor-b-config",
                args: &args_ref,
            },
        )
    }

    /// Force-deletes an actor pod and waits until it is gone.
    pub fn kill_actor(&self, name: &str) -> Result<(), LifecycleError> {
        refuse_outside_namespace_mutations(&self.namespace)?;
        let _ = kubectl(
            &self.kube_context,
            &[
                "-n",
                &self.namespace,
                "delete",
                "pod",
                name,
                "--grace-period=0",
                "--force",
                "--ignore-not-found=true",
            ],
        )?;
        wait_for_pod_gone(
            &self.kube_context,
            &self.namespace,
            name,
            Duration::from_secs(120),
        )
    }

    /// Replaces NetworkPolicy egress so campaign pods cannot reach RustFS.
    pub fn deny_rustfs_egress(&self) -> Result<(), LifecycleError> {
        refuse_outside_namespace_mutations(&self.namespace)?;
        apply_yaml(
            &self.kube_context,
            &network_policy_dns_only_yaml(&self.run_id, &self.namespace),
        )
    }

    /// Restores default-deny egress allowing DNS + in-namespace RustFS.
    pub fn restore_rustfs_egress(&self) -> Result<(), LifecycleError> {
        refuse_outside_namespace_mutations(&self.namespace)?;
        apply_yaml(
            &self.kube_context,
            &network_policy_yaml(&self.run_id, &self.namespace),
        )
    }

    /// Overwrites run credentials with invalid values (family 17).
    pub fn invalidate_store_credentials(&self) -> Result<(), LifecycleError> {
        refuse_outside_namespace_mutations(&self.namespace)?;
        create_secret(
            &self.kube_context,
            &self.namespace,
            "rustfs-credentials",
            &[
                ("access_key", "invalid-access-key-xxxxx"),
                ("secret_key", "invalid-secret-key-yyyyyyyyyyyyyyyy"),
                ("RUSTFS_ACCESS_KEY", "invalid-access-key-xxxxx"),
                ("RUSTFS_SECRET_KEY", "invalid-secret-key-yyyyyyyyyyyyyyyy"),
            ],
            &self.run_id,
        )
    }

    /// Restores the original generated credentials into the run Secret.
    pub fn restore_store_credentials(&self) -> Result<(), LifecycleError> {
        refuse_outside_namespace_mutations(&self.namespace)?;
        create_secret(
            &self.kube_context,
            &self.namespace,
            "rustfs-credentials",
            &[
                ("access_key", &self.access_key),
                ("secret_key", &self.secret_key),
                ("RUSTFS_ACCESS_KEY", &self.access_key),
                ("RUSTFS_SECRET_KEY", &self.secret_key),
            ],
            &self.run_id,
        )
    }

    fn deploy_actor(
        &self,
        profile: &RustFsHomeFleetProfile,
        scenario: &str,
        spec: ActorDeploy<'_>,
    ) -> Result<ActorPlacement, LifecycleError> {
        let advertise = format!("tcp://{}.{}:9000", spec.name, self.namespace);
        let config = actor_config_yaml(
            spec.owner_id,
            &advertise,
            &self.store.service_dns,
            &self.store.bucket,
            &self.ha_prefix(scenario),
        );
        apply_configmap(
            &self.kube_context,
            &self.namespace,
            spec.configmap,
            &self.run_id,
            "scripture.yaml",
            &config,
        )?;
        apply_yaml(
            &self.kube_context,
            &actor_pod_yaml(&ActorPodSpec {
                name: spec.name,
                role: spec.role,
                namespace: &self.namespace,
                run_id: &self.run_id,
                node: spec.node,
                image: &profile.scripture_image,
                args: spec.args,
                configmap: spec.configmap,
            }),
        )?;
        apply_yaml(
            &self.kube_context,
            &actor_service_yaml(spec.name, &self.namespace, &self.run_id, spec.role),
        )?;
        wait_for_pod_ready(
            &self.kube_context,
            &self.namespace,
            spec.name,
            Duration::from_secs(180),
        )?;
        actor_placement(&self.kube_context, &self.namespace, spec.name, spec.role)
    }

    fn ha_prefix(&self, scenario: &str) -> String {
        format!("scripture/correctness/{}/{scenario}/ha", self.run_id)
    }

    /// Access key for in-process clients talking to the run store (not for artifacts).
    #[must_use]
    pub fn access_key(&self) -> &str {
        &self.access_key
    }

    /// Secret key for in-process clients (not for artifacts).
    #[must_use]
    pub fn secret_key(&self) -> &str {
        &self.secret_key
    }
}

struct ActorDeploy<'a> {
    name: &'a str,
    role: &'a str,
    owner_id: &'a str,
    node: &'a str,
    configmap: &'a str,
    args: &'a [&'a str],
}

/// Lifecycle failures.
#[derive(Debug, thiserror::Error)]
pub enum LifecycleError {
    /// Isolation / topology violation.
    #[error("isolation: {0}")]
    Isolation(String),
    /// kubectl failure.
    #[error("kubectl: {0}")]
    Kubectl(String),
    /// Timed out waiting for readiness.
    #[error("timeout: {0}")]
    Timeout(String),
    /// Randomness failure.
    #[error("random: {0}")]
    Random(String),
    /// Serialization failure.
    #[error("serialize: {0}")]
    Serialize(#[from] serde_json::Error),
    /// IO failure.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

fn refuse_outside_namespace_mutations(namespace: &str) -> Result<(), LifecycleError> {
    if !namespace.starts_with("scripture-correctness-") {
        return Err(LifecycleError::Isolation(format!(
            "refusing mutation outside run namespace pattern: {namespace}"
        )));
    }
    Ok(())
}

fn generate_token(prefix: &str, nbytes: usize) -> Result<String, LifecycleError> {
    let mut bytes = vec![0_u8; nbytes];
    fill_random(&mut bytes).map_err(|error| LifecycleError::Random(error.to_string()))?;
    let mut out = String::from(prefix);
    for byte in bytes {
        out.push(char::from(b'a' + (byte % 26)));
        out.push(char::from(b'0' + (byte % 10)));
    }
    Ok(out)
}

fn kubectl(context: &str, args: &[&str]) -> Result<String, LifecycleError> {
    let mut command = Command::new("kubectl");
    command.arg("--context").arg(context).args(args);
    let output = command
        .output()
        .map_err(|error| LifecycleError::Kubectl(format!("spawn: {error}")))?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    } else {
        Err(LifecycleError::Kubectl(format!(
            "{} => {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        )))
    }
}

fn apply_yaml(context: &str, yaml: &str) -> Result<(), LifecycleError> {
    let mut child = Command::new("kubectl")
        .arg("--context")
        .arg(context)
        .args(["apply", "-f", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| LifecycleError::Kubectl(format!("spawn apply: {error}")))?;
    if let Some(mut stdin) = child.stdin.take() {
        use std::io::Write;
        stdin
            .write_all(yaml.as_bytes())
            .map_err(|error| LifecycleError::Kubectl(format!("write apply: {error}")))?;
    }
    let output = child
        .wait_with_output()
        .map_err(|error| LifecycleError::Kubectl(format!("wait apply: {error}")))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(LifecycleError::Kubectl(format!(
            "apply failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )))
    }
}

fn create_secret(
    context: &str,
    namespace: &str,
    name: &str,
    literals: &[(&str, &str)],
    run_id: &str,
) -> Result<(), LifecycleError> {
    let mut data = String::new();
    for (key, value) in literals {
        // YAML double-quoted escaping for generated alphanumeric tokens.
        data.push_str(&format!("  {key}: \"{value}\"\n"));
    }
    apply_yaml(
        context,
        &format!(
            r#"apiVersion: v1
kind: Secret
metadata:
  name: {name}
  namespace: {namespace}
  labels:
    scripture.dev/run-id: {run_id}
    app.kubernetes.io/part-of: scripture
    scripture.dev/purpose: ephemeral-campaign-store
type: Opaque
stringData:
{data}"#
        ),
    )
}

fn resource_uid(
    context: &str,
    namespace: &str,
    kind: &str,
    name: &str,
) -> Result<String, LifecycleError> {
    let raw = kubectl(
        context,
        &[
            "-n",
            namespace,
            "get",
            kind,
            name,
            "-o",
            "jsonpath={.metadata.uid}",
        ],
    )?;
    let uid = raw.trim().to_owned();
    if uid.is_empty() {
        return Err(LifecycleError::Kubectl(format!(
            "missing uid for {kind}/{name}"
        )));
    }
    Ok(uid)
}

fn verify_run_owned_service(
    context: &str,
    namespace: &str,
    run_id: &str,
    expected_uid: &str,
) -> Result<(), LifecycleError> {
    let json = kubectl(
        context,
        &["-n", namespace, "get", "svc", "rustfs", "-o", "json"],
    )?;
    let value: serde_json::Value = serde_json::from_str(&json)?;
    let uid = value["metadata"]["uid"].as_str().unwrap_or_default();
    let ns = value["metadata"]["namespace"].as_str().unwrap_or_default();
    let label = value["metadata"]["labels"]["scripture.dev/run-id"]
        .as_str()
        .unwrap_or_default();
    if ns != namespace || uid != expected_uid || label != run_id {
        return Err(LifecycleError::Isolation(format!(
            "RustFS Service failed run-ownership check ns={ns} uid={uid} run-id={label}"
        )));
    }
    // EndpointSlices must live in the same namespace.
    let slices = kubectl(
        context,
        &[
            "-n",
            namespace,
            "get",
            "endpointslices",
            "-l",
            "kubernetes.io/service-name=rustfs",
            "-o",
            "json",
        ],
    )
    .unwrap_or_else(|_| "{\"items\":[]}".into());
    let slices_value: serde_json::Value = serde_json::from_str(&slices)?;
    if let Some(items) = slices_value["items"].as_array() {
        for item in items {
            let slice_ns = item["metadata"]["namespace"].as_str().unwrap_or_default();
            if slice_ns != namespace {
                return Err(LifecycleError::Isolation(format!(
                    "EndpointSlice outside run namespace: {slice_ns}"
                )));
            }
        }
    }
    Ok(())
}

fn wait_for_deployment(
    context: &str,
    namespace: &str,
    name: &str,
    timeout: Duration,
) -> Result<(), LifecycleError> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let available = kubectl(
            context,
            &[
                "-n",
                namespace,
                "get",
                "deploy",
                name,
                "-o",
                "jsonpath={.status.availableReplicas}",
            ],
        )
        .unwrap_or_default();
        if available.trim() == "1" {
            return Ok(());
        }
        std::thread::sleep(Duration::from_secs(2));
    }
    Err(LifecycleError::Timeout(format!(
        "deploy/{name} not available in {namespace}"
    )))
}

fn wait_for_pod_running(
    context: &str,
    namespace: &str,
    name: &str,
    timeout: Duration,
) -> Result<(), LifecycleError> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let phase = kubectl(
            context,
            &[
                "-n",
                namespace,
                "get",
                "pod",
                name,
                "-o",
                "jsonpath={.status.phase}",
            ],
        )
        .unwrap_or_default();
        if phase.trim() == "Running" {
            return Ok(());
        }
        if phase.trim() == "Failed" {
            return Err(LifecycleError::Kubectl(format!(
                "pod/{name} failed in {namespace}"
            )));
        }
        std::thread::sleep(Duration::from_secs(2));
    }
    Err(LifecycleError::Timeout(format!(
        "pod/{name} not Running in {namespace}"
    )))
}

fn wait_for_pod_ready(
    context: &str,
    namespace: &str,
    name: &str,
    timeout: Duration,
) -> Result<(), LifecycleError> {
    wait_for_pod_running(context, namespace, name, timeout)?;
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let ready = kubectl(
            context,
            &[
                "-n",
                namespace,
                "get",
                "pod",
                name,
                "-o",
                "jsonpath={.status.conditions[?(@.type==\"Ready\")].status}",
            ],
        )
        .unwrap_or_default();
        if ready.trim() == "True" {
            return Ok(());
        }
        let phase = kubectl(
            context,
            &[
                "-n",
                namespace,
                "get",
                "pod",
                name,
                "-o",
                "jsonpath={.status.phase}",
            ],
        )
        .unwrap_or_default();
        if phase.trim() == "Failed" || phase.trim() == "Succeeded" {
            return Err(LifecycleError::Kubectl(format!(
                "pod/{name} left Running before Ready (phase={})",
                phase.trim()
            )));
        }
        std::thread::sleep(Duration::from_secs(2));
    }
    Err(LifecycleError::Timeout(format!(
        "pod/{name} not Ready in {namespace}"
    )))
}

fn wait_for_pod_gone(
    context: &str,
    namespace: &str,
    name: &str,
    timeout: Duration,
) -> Result<(), LifecycleError> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let result = kubectl(
            context,
            &["-n", namespace, "get", "pod", name, "-o", "name"],
        );
        match result {
            Err(_) => return Ok(()),
            Ok(name_out) if name_out.trim().is_empty() => return Ok(()),
            Ok(_) => std::thread::sleep(Duration::from_secs(2)),
        }
    }
    Err(LifecycleError::Timeout(format!(
        "pod/{name} still present in {namespace}"
    )))
}

fn actor_placement(
    context: &str,
    namespace: &str,
    name: &str,
    role: &str,
) -> Result<ActorPlacement, LifecycleError> {
    let json = kubectl(
        context,
        &["-n", namespace, "get", "pod", name, "-o", "json"],
    )?;
    let value: serde_json::Value = serde_json::from_str(&json)?;
    Ok(ActorPlacement {
        name: name.to_owned(),
        uid: value["metadata"]["uid"]
            .as_str()
            .unwrap_or_default()
            .to_owned(),
        node: value["spec"]["nodeName"]
            .as_str()
            .unwrap_or_default()
            .to_owned(),
        role: role.to_owned(),
        adapter: "temporary-bootstrap-promote".into(),
    })
}

fn create_bucket_job(
    context: &str,
    namespace: &str,
    run_id: &str,
    bucket: &str,
    access_key: &str,
    secret_key: &str,
    endpoint: &str,
) -> Result<(), LifecycleError> {
    // Store bucket-init credentials in an env-only Job via secret keys already present.
    let _ = (access_key, secret_key, endpoint);
    apply_yaml(
        context,
        &format!(
            r#"apiVersion: batch/v1
kind: Job
metadata:
  name: rustfs-bucket-init
  namespace: {namespace}
  labels:
    scripture.dev/run-id: {run_id}
    scripture.dev/purpose: ephemeral-campaign-store
spec:
  backoffLimit: 3
  template:
    metadata:
      labels:
        scripture.dev/run-id: {run_id}
        scripture.dev/role: bucket-init
    spec:
      restartPolicy: Never
      containers:
        - name: aws
          image: amazon/aws-cli:2.35.5
          imagePullPolicy: IfNotPresent
          env:
            - name: AWS_ACCESS_KEY_ID
              valueFrom:
                secretKeyRef:
                  name: rustfs-credentials
                  key: access_key
            - name: AWS_SECRET_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: rustfs-credentials
                  key: secret_key
            - name: AWS_DEFAULT_REGION
              value: us-east-1
          command: ["/bin/sh","-eu","-c"]
          args:
            - |
              endpoint="http://rustfs.{namespace}.svc.cluster.local:9000"
              for i in $(seq 1 60); do
                if aws --endpoint-url "$endpoint" s3api head-bucket --bucket "{bucket}" 2>/dev/null; then
                  exit 0
                fi
                aws --endpoint-url "$endpoint" s3api create-bucket --bucket "{bucket}" && exit 0 || true
                sleep 2
              done
              echo "bucket init failed" >&2
              exit 1
"#
        ),
    )?;
    let deadline = Instant::now() + Duration::from_secs(180);
    while Instant::now() < deadline {
        let succeeded = kubectl(
            context,
            &[
                "-n",
                namespace,
                "get",
                "job",
                "rustfs-bucket-init",
                "-o",
                "jsonpath={.status.succeeded}",
            ],
        )
        .unwrap_or_default();
        if succeeded.trim() == "1" {
            return Ok(());
        }
        let failed = kubectl(
            context,
            &[
                "-n",
                namespace,
                "get",
                "job",
                "rustfs-bucket-init",
                "-o",
                "jsonpath={.status.failed}",
            ],
        )
        .unwrap_or_default();
        if !failed.trim().is_empty() && failed.trim() != "0" {
            return Err(LifecycleError::Kubectl(
                "rustfs-bucket-init job failed".into(),
            ));
        }
        std::thread::sleep(Duration::from_secs(2));
    }
    Err(LifecycleError::Timeout(
        "rustfs-bucket-init did not succeed".into(),
    ))
}

fn apply_configmap(
    context: &str,
    namespace: &str,
    name: &str,
    run_id: &str,
    key: &str,
    value: &str,
) -> Result<(), LifecycleError> {
    apply_yaml(
        context,
        &format!(
            r#"apiVersion: v1
kind: ConfigMap
metadata:
  name: {name}
  namespace: {namespace}
  labels:
    scripture.dev/run-id: {run_id}
data:
  {key}: |
{indented}
"#,
            indented = indent_block(value, 4)
        ),
    )
}

fn indent_block(raw: &str, spaces: usize) -> String {
    let pad = " ".repeat(spaces);
    raw.lines()
        .map(|line| format!("{pad}{line}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn namespace_yaml(run_id: &str, namespace: &str) -> String {
    format!(
        r#"apiVersion: v1
kind: Namespace
metadata:
  name: {namespace}
  labels:
    app.kubernetes.io/part-of: scripture
    scripture.dev/purpose: correctness-campaign
    scripture.dev/run-id: {run_id}
"#
    )
}

fn rustfs_manifest(run_id: &str, namespace: &str, node: &str) -> String {
    format!(
        r#"apiVersion: apps/v1
kind: Deployment
metadata:
  name: rustfs
  namespace: {namespace}
  labels:
    app.kubernetes.io/name: rustfs
    app.kubernetes.io/part-of: scripture
    scripture.dev/run-id: {run_id}
    scripture.dev/purpose: ephemeral-campaign-store
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: rustfs
      scripture.dev/run-id: {run_id}
  template:
    metadata:
      labels:
        app.kubernetes.io/name: rustfs
        scripture.dev/run-id: {run_id}
        scripture.dev/purpose: ephemeral-campaign-store
    spec:
      nodeSelector:
        kubernetes.io/hostname: {node}
      containers:
        - name: rustfs
          image: {RUSTFS_IMAGE}
          imagePullPolicy: IfNotPresent
          ports:
            - name: s3
              containerPort: 9000
            - name: console
              containerPort: 9001
          env:
            - name: RUSTFS_VOLUMES
              value: /data/rustfs0
            - name: RUSTFS_ADDRESS
              value: 0.0.0.0:9000
            - name: RUSTFS_CONSOLE_ADDRESS
              value: 0.0.0.0:9001
            - name: RUSTFS_CONSOLE_ENABLE
              value: "true"
            - name: RUSTFS_OBS_LOGGER_LEVEL
              value: warn
            - name: RUSTFS_UNSAFE_BYPASS_DISK_CHECK
              value: "true"
            - name: RUSTFS_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: rustfs-credentials
                  key: access_key
            - name: RUSTFS_SECRET_KEY
              valueFrom:
                secretKeyRef:
                  name: rustfs-credentials
                  key: secret_key
          volumeMounts:
            - name: data
              mountPath: /data/rustfs0
          readinessProbe:
            httpGet:
              path: /health/ready
              port: 9000
            initialDelaySeconds: 3
            periodSeconds: 3
          resources:
            requests:
              cpu: 100m
              memory: 256Mi
            limits:
              cpu: "1"
              memory: 1Gi
      volumes:
        - name: data
          emptyDir: {{}}
---
apiVersion: v1
kind: Service
metadata:
  name: rustfs
  namespace: {namespace}
  labels:
    app.kubernetes.io/name: rustfs
    scripture.dev/run-id: {run_id}
    scripture.dev/purpose: ephemeral-campaign-store
spec:
  selector:
    app.kubernetes.io/name: rustfs
    scripture.dev/run-id: {run_id}
  ports:
    - name: s3
      port: 9000
      targetPort: s3
"#
    )
}

fn network_policy_yaml(run_id: &str, namespace: &str) -> String {
    format!(
        r#"apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: campaign-default-deny-egress
  namespace: {namespace}
  labels:
    scripture.dev/run-id: {run_id}
    scripture.dev/purpose: isolation
spec:
  podSelector: {{}}
  policyTypes:
    - Egress
    - Ingress
  ingress:
    - from:
        - podSelector: {{}}
  egress:
    # DNS
    - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: kube-system
      ports:
        - protocol: UDP
          port: 53
        - protocol: TCP
          port: 53
    # In-namespace RustFS only
    - to:
        - podSelector:
            matchLabels:
              app.kubernetes.io/name: rustfs
              scripture.dev/run-id: {run_id}
      ports:
        - protocol: TCP
          port: 9000
"#
    )
}

fn network_policy_dns_only_yaml(run_id: &str, namespace: &str) -> String {
    format!(
        r#"apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: campaign-default-deny-egress
  namespace: {namespace}
  labels:
    scripture.dev/run-id: {run_id}
    scripture.dev/purpose: isolation
    scripture.dev/fault: directional-backend-loss
spec:
  podSelector: {{}}
  policyTypes:
    - Egress
    - Ingress
  ingress:
    - from:
        - podSelector: {{}}
  egress:
    - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: kube-system
      ports:
        - protocol: UDP
          port: 53
        - protocol: TCP
          port: 53
"#
    )
}

fn actor_config_yaml(
    owner_id: &str,
    advertise: &str,
    endpoint: &str,
    bucket: &str,
    prefix: &str,
) -> String {
    format!(
        r#"version: 1
node:
  owner_id: "{owner_id}"
  advertise: "{advertise}"
listener:
  bind: "0.0.0.0:9000"
verse:
  journal_id: "scripture-jrnl!!"
  verse_id: "scripture-verse!"
  cohort_id: "scripture-cohrt!"
  writer_id: "scripture-wrtr!!"
store:
  backend: rustfs
  endpoint: "{endpoint}"
  bucket: "{bucket}"
  region: us-east-1
  prefix: "{prefix}"
metrics:
  status_bind: "0.0.0.0:9100"
ha:
  mode: serving-authority
"#
    )
}

fn actor_service_yaml(name: &str, namespace: &str, run_id: &str, role: &str) -> String {
    format!(
        r#"apiVersion: v1
kind: Service
metadata:
  name: {name}
  namespace: {namespace}
  labels:
    app.kubernetes.io/name: scripture
    app.kubernetes.io/component: correctness-actor
    scripture.dev/run-id: {run_id}
    scripture.dev/role: {role}
    scripture.dev/adapter: temporary-bootstrap-promote
spec:
  selector:
    app.kubernetes.io/name: scripture
    scripture.dev/run-id: {run_id}
    scripture.dev/role: {role}
  ports:
    - name: raw-lines
      port: 9000
      targetPort: raw-lines
    - name: status
      port: 9100
      targetPort: status
"#
    )
}

fn actor_pod_yaml(spec: &ActorPodSpec<'_>) -> String {
    let args_yaml = spec
        .args
        .iter()
        .map(|arg| format!("        - {arg:?}"))
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        r#"apiVersion: v1
kind: Pod
metadata:
  name: {name}
  namespace: {namespace}
  labels:
    app.kubernetes.io/name: scripture
    app.kubernetes.io/component: correctness-actor
    scripture.dev/run-id: {run_id}
    scripture.dev/role: {role}
    scripture.dev/adapter: temporary-bootstrap-promote
spec:
  restartPolicy: Never
  nodeSelector:
    kubernetes.io/hostname: {node}
  containers:
    - name: scripture
      image: {image}
      imagePullPolicy: Never
      args:
{args_yaml}
      env:
        - name: RUSTFS_ACCESS_KEY
          valueFrom:
            secretKeyRef:
              name: rustfs-credentials
              key: RUSTFS_ACCESS_KEY
        - name: RUSTFS_SECRET_KEY
          valueFrom:
            secretKeyRef:
              name: rustfs-credentials
              key: RUSTFS_SECRET_KEY
      ports:
        - name: raw-lines
          containerPort: 9000
        - name: status
          containerPort: 9100
      volumeMounts:
        - name: config
          mountPath: /etc/scripture
          readOnly: true
      readinessProbe:
        httpGet:
          path: /readyz
          port: 9100
        initialDelaySeconds: 2
        periodSeconds: 2
      resources:
        requests:
          cpu: 100m
          memory: 256Mi
        limits:
          cpu: "1"
          memory: 1Gi
  volumes:
    - name: config
      configMap:
        name: {configmap}
"#,
        name = spec.name,
        namespace = spec.namespace,
        run_id = spec.run_id,
        role = spec.role,
        node = spec.node,
        image = spec.image,
        configmap = spec.configmap,
    )
}

struct ActorPodSpec<'a> {
    name: &'a str,
    role: &'a str,
    namespace: &'a str,
    run_id: &'a str,
    node: &'a str,
    image: &'a str,
    args: &'a [&'a str],
    configmap: &'a str,
}

#[cfg(test)]
mod tests {
    use super::{namespace_yaml, network_policy_yaml, refuse_outside_namespace_mutations};

    #[test]
    fn refuses_non_run_namespaces() {
        assert!(refuse_outside_namespace_mutations("scripture-lab").is_err());
        assert!(refuse_outside_namespace_mutations("scripture-correctness-abc").is_ok());
    }

    #[test]
    fn manifests_carry_run_id_and_no_lab() {
        let ns = namespace_yaml("r1", "scripture-correctness-r1");
        assert!(ns.contains("scripture.dev/run-id: r1"));
        assert!(!ns.contains("scripture-lab"));
        let np = network_policy_yaml("r1", "scripture-correctness-r1");
        assert!(np.contains("default-deny"));
        assert!(np.contains("app.kubernetes.io/name: rustfs"));
    }
}
