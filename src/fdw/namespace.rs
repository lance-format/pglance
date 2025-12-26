use crate::fdw::options::parse_defelem_list;
use lance_namespace::LanceNamespace;
use lance_namespace_impls::ConnectBuilder;
use pgrx::pg_sys;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::runtime::Runtime;

const OPT_NS_PREFIX: &str = "ns.";
const OPT_NS_IMPL: &str = "ns.impl";

pub fn connect_namespace(
    runtime: &Runtime,
    server_oid: pg_sys::Oid,
) -> Result<Arc<dyn LanceNamespace>, String> {
    unsafe {
        let server = pg_sys::GetForeignServer(server_oid);
        if server.is_null() {
            return Err(format!("missing ForeignServer, server_oid={}", server_oid));
        }

        let mut raw_opts = BTreeMap::<String, String>::new();
        parse_defelem_list((*server).options, &mut raw_opts);

        let impl_name = raw_opts
            .get(OPT_NS_IMPL)
            .cloned()
            .ok_or_else(|| format!("missing required option: {}", OPT_NS_IMPL))?;

        let mut properties = HashMap::<String, String>::new();
        for (k, v) in raw_opts {
            let Some(key) = k.strip_prefix(OPT_NS_PREFIX) else {
                continue;
            };
            if key == "impl" {
                continue;
            }
            properties.insert(key.to_string(), v);
        }

        runtime
            .block_on(async {
                ConnectBuilder::new(impl_name)
                    .properties(properties)
                    .connect()
                    .await
            })
            .map_err(|e| e.to_string())
    }
}
