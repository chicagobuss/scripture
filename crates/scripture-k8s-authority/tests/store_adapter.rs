//! Adversarial and conformance proofs for the Kubernetes Serving Authority store.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use scripture::canon::VerseId;
use scripture::model::JournalId;
use scripture::serving_authority::{AuthorityKey, AuthorityState, ServingAuthorityRecord};
use scripture_k8s_authority::{
    KubernetesServingAuthorityStore, RECORD_FORMAT_V1, ServingAuthority,
    ServingAuthorityKubeTransport, ServingAuthoritySpec, TransportError, TransportFuture,
    authority_object_name, display_from_record,
};
use scripture_service::{
    CasOutcome, ServingAuthorityStore, ServingAuthorityStoreError, StoreVersion,
    run_serving_authority_store_conformance,
};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn install_crypto_provider() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn sample_key() -> AuthorityKey {
    AuthorityKey {
        journal_id: JournalId::from_bytes(*b"k8s-auth-jrnl!!!"),
        verse_id: VerseId::from_bytes(*b"k8s-auth-verse!!"),
    }
}

fn record(key: AuthorityKey) -> ServingAuthorityRecord {
    ServingAuthorityRecord::new(key, AuthorityState::Unassigned)
}

#[derive(Debug, Default)]
struct MapTransport {
    rows: Mutex<HashMap<String, ServingAuthority>>,
    seq: AtomicUsize,
    create_calls: AtomicUsize,
    replace_calls: AtomicUsize,
    last_replace_rv: Mutex<Option<String>>,
    next_create: Mutex<Option<Result<ServingAuthority, TransportError>>>,
    next_replace: Mutex<Option<Result<ServingAuthority, TransportError>>>,
}

impl ServingAuthorityKubeTransport for MapTransport {
    fn get<'a>(&'a self, name: &'a str) -> TransportFuture<'a, ServingAuthority> {
        Box::pin(async move {
            self.rows
                .lock()
                .expect("lock")
                .get(name)
                .cloned()
                .ok_or(TransportError::NotFound)
        })
    }

    fn create<'a>(&'a self, mut object: ServingAuthority) -> TransportFuture<'a, ServingAuthority> {
        Box::pin(async move {
            self.create_calls.fetch_add(1, Ordering::SeqCst);
            if let Some(forced) = self.next_create.lock().expect("lock").take() {
                return forced;
            }
            let name = object.metadata.name.clone().expect("name");
            let mut guard = self.rows.lock().expect("lock");
            if guard.contains_key(&name) {
                return Err(TransportError::Conflict);
            }
            let rv = self.seq.fetch_add(1, Ordering::SeqCst).to_string();
            object.metadata.resource_version = Some(rv);
            guard.insert(name, object.clone());
            Ok(object)
        })
    }

    fn replace<'a>(
        &'a self,
        name: &'a str,
        mut object: ServingAuthority,
    ) -> TransportFuture<'a, ServingAuthority> {
        Box::pin(async move {
            self.replace_calls.fetch_add(1, Ordering::SeqCst);
            *self.last_replace_rv.lock().expect("lock") = object.metadata.resource_version.clone();
            if let Some(forced) = self.next_replace.lock().expect("lock").take() {
                return forced;
            }
            let expected = object.metadata.resource_version.clone().expect("rv");
            let mut guard = self.rows.lock().expect("lock");
            let Some(current) = guard.get(name) else {
                return Err(TransportError::Conflict);
            };
            if current.metadata.resource_version.as_deref() != Some(expected.as_str()) {
                return Err(TransportError::Conflict);
            }
            let rv = self.seq.fetch_add(1, Ordering::SeqCst).to_string();
            object.metadata.resource_version = Some(rv);
            guard.insert(name.to_owned(), object.clone());
            Ok(object)
        })
    }
}

fn object_for(
    key: AuthorityKey,
    rv: Option<&str>,
    mutate: impl FnOnce(&mut ServingAuthoritySpec),
) -> ServingAuthority {
    let rec = record(key);
    let bytes = rec.encode().expect("encode");
    let mut spec = ServingAuthoritySpec {
        record_format: RECORD_FORMAT_V1.to_owned(),
        record: BASE64.encode(bytes),
        display: Some(display_from_record(&rec)),
    };
    mutate(&mut spec);
    ServingAuthority {
        metadata: ObjectMeta {
            name: Some(authority_object_name(&key)),
            namespace: Some("scripture-lab".into()),
            resource_version: rv.map(str::to_owned),
            ..Default::default()
        },
        spec,
    }
}

#[tokio::test]
async fn shared_conformance_passes_on_mock_transport() {
    let store = Arc::new(
        KubernetesServingAuthorityStore::from_transport(
            MapTransport::default(),
            "scripture-lab",
            None,
        )
        .expect("store"),
    );
    run_serving_authority_store_conformance(store as Arc<dyn ServingAuthorityStore>, sample_key())
        .await
        .expect("conformance");
}

#[tokio::test]
async fn get_404_maps_absent() {
    let store = KubernetesServingAuthorityStore::from_transport(
        MapTransport::default(),
        "scripture-lab",
        None,
    )
    .expect("ok");
    assert!(store.observe(sample_key()).await.expect("ok").is_none());
}

#[tokio::test]
async fn create_409_maps_conflict() {
    let transport = MapTransport::default();
    *transport.next_create.lock().expect("ok") = Some(Err(TransportError::Conflict));
    let store = KubernetesServingAuthorityStore::from_transport(transport, "scripture-lab", None)
        .expect("ok");
    assert_eq!(
        store
            .compare_and_swap(sample_key(), None, record(sample_key()))
            .await
            .expect("ok"),
        CasOutcome::Conflict
    );
}

#[tokio::test]
async fn replace_sends_exact_opaque_resource_version_and_stale_conflicts() {
    #[derive(Debug, Clone)]
    struct Shared(Arc<MapTransport>);
    impl ServingAuthorityKubeTransport for Shared {
        fn get<'a>(&'a self, name: &'a str) -> TransportFuture<'a, ServingAuthority> {
            self.0.get(name)
        }
        fn create<'a>(&'a self, object: ServingAuthority) -> TransportFuture<'a, ServingAuthority> {
            self.0.create(object)
        }
        fn replace<'a>(
            &'a self,
            name: &'a str,
            object: ServingAuthority,
        ) -> TransportFuture<'a, ServingAuthority> {
            self.0.replace(name, object)
        }
    }
    let shared = Arc::new(MapTransport::default());
    let store = KubernetesServingAuthorityStore::from_transport(
        Shared(Arc::clone(&shared)),
        "scripture-lab",
        None,
    )
    .expect("ok");
    let key = sample_key();
    assert_eq!(
        store
            .compare_and_swap(key, None, record(key))
            .await
            .expect("ok"),
        CasOutcome::Applied
    );
    let snap = store.observe(key).await.expect("ok").expect("present");
    let stale = StoreVersion::new(b"not-this-rv".to_vec());
    assert_eq!(
        store
            .compare_and_swap(key, Some(stale), record(key))
            .await
            .expect("ok"),
        CasOutcome::Conflict
    );
    assert_eq!(
        shared.last_replace_rv.lock().expect("ok").as_deref(),
        Some("not-this-rv")
    );
    assert_eq!(
        store
            .compare_and_swap(key, Some(snap.version.clone()), record(key))
            .await
            .expect("ok"),
        CasOutcome::Applied
    );
    assert_eq!(
        shared.last_replace_rv.lock().expect("ok").as_deref(),
        Some(std::str::from_utf8(snap.version.as_bytes()).expect("utf8 resourceVersion"))
    );
}

#[tokio::test]
async fn malformed_payloads_fail_closed() {
    let key = sample_key();
    #[derive(Debug)]
    struct Fixed(ServingAuthority);
    impl ServingAuthorityKubeTransport for Fixed {
        fn get<'a>(&'a self, _: &'a str) -> TransportFuture<'a, ServingAuthority> {
            let object = self.0.clone();
            Box::pin(async move { Ok(object) })
        }
        fn create<'a>(&'a self, _: ServingAuthority) -> TransportFuture<'a, ServingAuthority> {
            Box::pin(async { unreachable!() })
        }
        fn replace<'a>(
            &'a self,
            _: &'a str,
            _: ServingAuthority,
        ) -> TransportFuture<'a, ServingAuthority> {
            Box::pin(async { unreachable!() })
        }
    }
    let cases = [
        object_for(key, Some("1"), |spec| {
            spec.record = "%%%not-base64%%%".into();
        }),
        object_for(key, Some("1"), |spec| {
            spec.record_format = "wrong-format".into();
        }),
        object_for(key, Some("1"), |spec| {
            spec.record = BASE64.encode(b"not-a-scar-record");
        }),
        object_for(key, None, |_| {}),
        object_for(key, Some("1"), |spec| {
            spec.display = None;
        }),
        object_for(key, Some("1"), |spec| {
            if let Some(display) = &mut spec.display {
                display.state = "Wrong".into();
            }
        }),
        {
            let other = AuthorityKey {
                journal_id: JournalId::from_bytes(*b"other-journal!!!"),
                verse_id: VerseId::from_bytes(*b"other-verse!!!!!"),
            };
            object_for(other, Some("1"), |_| {})
        },
        {
            let mut object = object_for(key, Some("1"), |_| {});
            object.metadata.name = Some("sa-wrong-object".into());
            object
        },
        {
            let mut object = object_for(key, Some("1"), |_| {});
            object.metadata.namespace = Some("wrong-namespace".into());
            object
        },
    ];
    for object in cases {
        let store =
            KubernetesServingAuthorityStore::from_transport(Fixed(object), "scripture-lab", None)
                .expect("ok");
        let err = store.observe(key).await.expect_err("malformed");
        assert!(
            matches!(err, ServingAuthorityStoreError::MalformedPayload { .. }),
            "got {err:?}"
        );
    }
}

#[tokio::test]
async fn post_dispatch_timeout_is_indeterminate_without_retry() {
    let calls = Arc::new(AtomicUsize::new(0));
    #[derive(Debug)]
    struct OnceIndeterminate {
        calls: Arc<AtomicUsize>,
    }
    impl ServingAuthorityKubeTransport for OnceIndeterminate {
        fn get<'a>(&'a self, _: &'a str) -> TransportFuture<'a, ServingAuthority> {
            Box::pin(async { Err(TransportError::NotFound) })
        }
        fn create<'a>(&'a self, _: ServingAuthority) -> TransportFuture<'a, ServingAuthority> {
            let calls = Arc::clone(&self.calls);
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(TransportError::Indeterminate(Box::new(
                    std::io::Error::other("timeout"),
                )))
            })
        }
        fn replace<'a>(
            &'a self,
            _: &'a str,
            _: ServingAuthority,
        ) -> TransportFuture<'a, ServingAuthority> {
            Box::pin(async { unreachable!("no replace") })
        }
    }
    let store = KubernetesServingAuthorityStore::from_transport(
        OnceIndeterminate {
            calls: Arc::clone(&calls),
        },
        "scripture-lab",
        None,
    )
    .expect("ok");
    let err = store
        .compare_and_swap(sample_key(), None, record(sample_key()))
        .await
        .expect_err("indeterminate");
    assert!(matches!(err, ServingAuthorityStoreError::Indeterminate(_)));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn wiremock_get_has_no_resource_version_zero() {
    install_crypto_provider();
    let server = MockServer::start().await;
    let key = sample_key();
    let name = authority_object_name(&key);
    let body = serde_json::to_string(&object_for(key, Some("42"), |_| {})).expect("json");
    Mock::given(method("GET"))
        .and(path(format!(
            "/apis/scripture.dev/v1alpha1/namespaces/scripture-lab/servingauthorities/{name}"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .expect(1)
        .mount(&server)
        .await;

    let cluster_url = server.uri().parse().expect("uri");
    let mut config = kube::Config::new(cluster_url);
    config.accept_invalid_certs = true;
    let client = kube::Client::try_from(config).expect("client");
    let store =
        KubernetesServingAuthorityStore::from_client(client, "scripture-lab", None).expect("store");
    let snap = store.observe(key).await.expect("observe").expect("present");
    assert_eq!(snap.version.as_bytes(), b"42");
    let requests = server.received_requests().await.expect("requests");
    let query = requests[0].url.query().unwrap_or_default();
    assert!(
        !query.contains("resourceVersion=0"),
        "GET must not use watch-cache resourceVersion=0; query={query:?}"
    );
}

#[tokio::test]
async fn key_mismatch_rejected_before_transport() {
    let create_calls = Arc::new(AtomicUsize::new(0));
    #[derive(Debug)]
    struct Counting {
        calls: Arc<AtomicUsize>,
    }
    impl ServingAuthorityKubeTransport for Counting {
        fn get<'a>(&'a self, _: &'a str) -> TransportFuture<'a, ServingAuthority> {
            Box::pin(async { Err(TransportError::NotFound) })
        }
        fn create<'a>(&'a self, _: ServingAuthority) -> TransportFuture<'a, ServingAuthority> {
            let calls = Arc::clone(&self.calls);
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(TransportError::Unavailable(Box::new(
                    std::io::Error::other("should not call"),
                )))
            })
        }
        fn replace<'a>(
            &'a self,
            _: &'a str,
            _: ServingAuthority,
        ) -> TransportFuture<'a, ServingAuthority> {
            Box::pin(async { unreachable!() })
        }
    }
    let store = KubernetesServingAuthorityStore::from_transport(
        Counting {
            calls: Arc::clone(&create_calls),
        },
        "scripture-lab",
        None,
    )
    .expect("ok");
    let other = AuthorityKey {
        journal_id: JournalId::from_bytes(*b"other-journal!!!"),
        verse_id: VerseId::from_bytes(*b"other-verse!!!!!"),
    };
    let err = store
        .compare_and_swap(sample_key(), None, record(other))
        .await
        .expect_err("mismatch");
    assert!(matches!(
        err,
        ServingAuthorityStoreError::MalformedPayload { .. }
    ));
    assert_eq!(create_calls.load(Ordering::SeqCst), 0);
}
