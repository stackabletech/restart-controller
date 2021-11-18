use futures::{stream, Stream, StreamExt, TryStream};
use serde_json::json;
use snafu::Snafu;
use stackable_operator::{
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, EnvFromSource, EnvVar, PodSpec, Secret, Volume},
    },
    kube::{
        self,
        api::{DynamicObject, ListParams, Patch, PatchParams},
        runtime::{
            applier,
            controller::{trigger_self, trigger_with, Context, ReconcileRequest, ReconcilerAction},
            reflector::{self, reflector, ObjectRef, Store},
            utils::{try_flatten_applied, try_flatten_touched},
            watcher,
        },
        Resource, ResourceExt,
    },
};
use std::{
    collections::BTreeMap,
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};
use tracing::{error, info};

struct Ctx {
    kube: kube::Client,
    cms: Store<ConfigMap>,
    cms_inited: Arc<AtomicBool>,
    secrets: Store<Secret>,
    secrets_inited: Arc<AtomicBool>,
}
#[derive(Snafu, Debug)]
enum Error {
    #[snafu(display("tried to reconcile {} which has no namespace", obj_ref))]
    ObjectHasNoNamespace { obj_ref: ObjectRef<DynamicObject> },
    #[snafu(display("failed to apply update to {}", obj_ref))]
    Apply {
        source: kube::Error,
        obj_ref: ObjectRef<DynamicObject>,
    },
    #[snafu(display("configmaps were not yet loaded"))]
    ConfigMapsUninited,
    #[snafu(display("secrets were not yet loaded"))]
    SecretsUninited,
}

fn trigger_all<S, K>(
    stream: S,
    store: Store<K>,
) -> impl Stream<Item = Result<ReconcileRequest<K>, S::Error>>
where
    S: TryStream,
    K: Resource<DynamicType = ()> + Clone,
{
    trigger_with(stream, move |_| {
        store
            .state()
            .into_iter()
            .map(|obj| ObjectRef::from_obj(&obj))
    })
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    stackable_operator::logging::initialize_logging("RESTART_CONTROLLER_LOG");
    let kube = kube::Client::try_default().await?;
    let stses = kube::Api::<StatefulSet>::all(kube.clone());
    let cms = kube::Api::<ConfigMap>::all(kube.clone());
    let secrets = kube::Api::<Secret>::all(kube.clone());
    let sts_store = reflector::store::Writer::new(());
    let cm_store = reflector::store::Writer::new(());
    let secret_store = reflector::store::Writer::new(());
    let cms_inited = Arc::new(AtomicBool::from(false));
    let secrets_inited = Arc::new(AtomicBool::from(false));
    applier(
        |sts, ctx| Box::pin(reconcile(sts, ctx)),
        error_policy,
        Context::new(Ctx {
            kube,
            cms: cm_store.as_reader(),
            secrets: secret_store.as_reader(),
            cms_inited: cms_inited.clone(),
            secrets_inited: secrets_inited.clone(),
        }),
        sts_store.as_reader(),
        stream::select(
            stream::select(
                trigger_all(
                    try_flatten_touched(
                        reflector(cm_store, watcher(cms, ListParams::default())).inspect(|_| {
                            cms_inited.store(true, std::sync::atomic::Ordering::SeqCst)
                        }),
                    ),
                    sts_store.as_reader(),
                ),
                trigger_all(
                    try_flatten_touched(
                        reflector(secret_store, watcher(secrets, ListParams::default())).inspect(
                            |_| secrets_inited.store(true, std::sync::atomic::Ordering::SeqCst),
                        ),
                    ),
                    sts_store.as_reader(),
                ),
            ),
            trigger_self(
                try_flatten_applied(reflector(
                    sts_store,
                    watcher(
                        stses,
                        ListParams::default().labels("restarter.stackable.tech/enabled=true"),
                    ),
                )),
                (),
            ),
        ),
    )
    .for_each(|res| async move {
        match res {
            Ok((obj, _)) => info!(%obj, "Reconciled object"),
            Err(err) => {
                error!(
                    error = &err as &(dyn std::error::Error + 'static),
                    "Reconciliation failed"
                )
            }
        }
    })
    .await;
    Ok(())
}

fn find_pod_refs<'a, K: Resource + 'a>(
    pod_spec: &'a PodSpec,
    volume_ref: impl Fn(&Volume) -> Option<ObjectRef<K>> + 'a,
    env_var_ref: impl Fn(&EnvVar) -> Option<ObjectRef<K>> + 'a,
    env_from_ref: impl Fn(&EnvFromSource) -> Option<ObjectRef<K>> + 'a,
) -> impl Iterator<Item = ObjectRef<K>> + 'a {
    let volume_refs = pod_spec.volumes.iter().flatten().flat_map(volume_ref);
    let pod_containers = pod_spec
        .containers
        .iter()
        .chain(pod_spec.init_containers.iter().flatten());
    let container_env_var_refs = pod_containers
        .clone()
        .flat_map(|container| &container.env)
        .flatten()
        .flat_map(env_var_ref);
    let container_env_from_refs = pod_containers
        .flat_map(|container| &container.env_from)
        .flatten()
        .flat_map(env_from_ref);
    volume_refs
        .chain(container_env_var_refs)
        .chain(container_env_from_refs)
}

async fn reconcile(sts: StatefulSet, ctx: Context<Ctx>) -> Result<ReconcilerAction, Error> {
    if !ctx
        .get_ref()
        .cms_inited
        .load(std::sync::atomic::Ordering::SeqCst)
    {
        return ConfigMapsUninited.fail();
    }
    if !ctx
        .get_ref()
        .secrets_inited
        .load(std::sync::atomic::Ordering::SeqCst)
    {
        return SecretsUninited.fail();
    }

    let ns = sts.metadata.namespace.as_deref().unwrap();
    let mut annotations = BTreeMap::<String, String>::new();
    let pod_specs = sts
        .spec
        .iter()
        .flat_map(|sts_spec| sts_spec.template.spec.as_ref());
    let cm_refs = pod_specs
        .clone()
        .flat_map(|pod_spec| {
            find_pod_refs(
                pod_spec,
                |volume| {
                    Some(ObjectRef::<ConfigMap>::new(
                        volume.config_map.as_ref()?.name.as_deref()?,
                    ))
                },
                |env_var| {
                    Some(ObjectRef::<ConfigMap>::new(
                        env_var
                            .value_from
                            .as_ref()?
                            .config_map_key_ref
                            .as_ref()?
                            .name
                            .as_deref()?,
                    ))
                },
                |env_from| {
                    Some(ObjectRef::<ConfigMap>::new(
                        env_from.config_map_ref.as_ref()?.name.as_deref()?,
                    ))
                },
            )
        })
        .map(|cm_ref| cm_ref.within(ns));
    annotations.extend(
        cm_refs
            .flat_map(|cm_ref| ctx.get_ref().cms.get(&cm_ref))
            .flat_map(|cm| {
                Some((
                    format!("configmap.restarter.stackable.tech/{}", cm.metadata.name?),
                    format!("{}/{}", cm.metadata.uid?, cm.metadata.resource_version?),
                ))
            }),
    );
    let secret_refs = pod_specs
        .flat_map(|pod_spec| {
            find_pod_refs(
                pod_spec,
                |volume| {
                    Some(ObjectRef::<Secret>::new(
                        volume.secret.as_ref()?.secret_name.as_deref()?,
                    ))
                },
                |env_var| {
                    Some(ObjectRef::<Secret>::new(
                        env_var
                            .value_from
                            .as_ref()?
                            .secret_key_ref
                            .as_ref()?
                            .name
                            .as_deref()?,
                    ))
                },
                |env_from| {
                    Some(ObjectRef::<Secret>::new(
                        env_from.secret_ref.as_ref()?.name.as_deref()?,
                    ))
                },
            )
        })
        .map(|secret_ref| secret_ref.within(ns));
    annotations.extend(
        secret_refs
            .flat_map(|secret_ref| ctx.get_ref().secrets.get(&secret_ref))
            .flat_map(|cm| {
                Some((
                    format!("secret.restarter.stackable.tech/{}", cm.metadata.name?),
                    format!("{}/{}", cm.metadata.uid?, cm.metadata.resource_version?),
                ))
            }),
    );
    let stses = kube::Api::<StatefulSet>::namespaced(ctx.get_ref().kube.clone(), ns);
    stses
        .patch(
            &sts.name(),
            &PatchParams {
                force: true,
                field_manager: Some("restarter.stackable.tech/statefulset".to_string()),
                ..PatchParams::default()
            },
            &Patch::Apply(
                // Can't use typed API, see https://github.com/Arnavion/k8s-openapi/issues/112
                json!({
                    "apiVersion": "apps/v1",
                    "kind": "StatefulSet",
                    "metadata": {
                        "name": sts.metadata.name,
                        "namespace": sts.metadata.namespace,
                        "uid": sts.metadata.uid,
                    },
                    "spec": {
                        "template": {
                            "metadata": {
                                "annotations": annotations,
                            },
                        },
                    },
                }),
            ),
        )
        .await
        .unwrap();
    Ok(ReconcilerAction {
        requeue_after: None,
    })
}

fn error_policy(_error: &Error, _ctx: Context<Ctx>) -> ReconcilerAction {
    ReconcilerAction {
        requeue_after: Some(Duration::from_secs(5)),
    }
}
