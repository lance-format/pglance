use pgrx::pg_sys;
use std::collections::BTreeMap;
use std::ffi::CStr;

const OPT_URI: &str = "uri";
const OPT_BATCH_SIZE: &str = "batch_size";
const OPT_NS_TABLE_ID: &str = "ns.table_id";

#[derive(Debug, Clone)]
pub enum LanceDatasetSource {
    Uri {
        uri: String,
    },
    Namespace {
        server_oid: pg_sys::Oid,
        table_id: Vec<String>,
    },
}

#[derive(Debug, Clone)]
pub struct LanceFdwOptions {
    pub source: LanceDatasetSource,
    pub batch_size: usize,
}

impl LanceFdwOptions {
    pub fn from_foreign_table(foreign_table_id: pg_sys::Oid) -> Result<Self, String> {
        unsafe {
            let foreign_table = pg_sys::GetForeignTable(foreign_table_id);
            if foreign_table.is_null() {
                return Err("missing ForeignTable".to_string());
            }

            let server = pg_sys::GetForeignServer((*foreign_table).serverid);
            if server.is_null() {
                return Err("missing ForeignServer".to_string());
            }

            let mut server_opts = BTreeMap::<String, String>::new();
            parse_defelem_list((*server).options, &mut server_opts);
            let mut table_opts = BTreeMap::<String, String>::new();
            parse_defelem_list((*foreign_table).options, &mut table_opts);

            let batch_size = table_opts
                .get(OPT_BATCH_SIZE)
                .or_else(|| server_opts.get(OPT_BATCH_SIZE))
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(1024);

            if let Some(table_id_json) = table_opts.get(OPT_NS_TABLE_ID) {
                let table_id = serde_json::from_str::<Vec<String>>(table_id_json).map_err(|e| {
                    format!(
                        "invalid option: {} must be a JSON array of strings, error={}",
                        OPT_NS_TABLE_ID, e
                    )
                })?;
                if table_id.is_empty() {
                    return Err(format!(
                        "invalid option: {} must not be empty",
                        OPT_NS_TABLE_ID
                    ));
                }

                return Ok(Self {
                    source: LanceDatasetSource::Namespace {
                        server_oid: (*foreign_table).serverid,
                        table_id,
                    },
                    batch_size,
                });
            }

            let uri = table_opts
                .get(OPT_URI)
                .or_else(|| server_opts.get(OPT_URI))
                .cloned()
                .ok_or_else(|| format!("missing required option: {}", OPT_URI))?;

            Ok(Self {
                source: LanceDatasetSource::Uri { uri },
                batch_size,
            })
        }
    }

    pub fn dataset_label(&self) -> String {
        match &self.source {
            LanceDatasetSource::Uri { uri } => format!("uri={}", uri),
            LanceDatasetSource::Namespace {
                server_oid,
                table_id,
            } => {
                let table_id_json = serde_json::to_string(table_id).unwrap_or_else(|_| "[]".into());
                format!("server_oid={} table_id={}", server_oid, table_id_json)
            }
        }
    }
}

pub fn parse_defelem_list(list: *mut pg_sys::List, out: &mut BTreeMap<String, String>) {
    unsafe {
        if list.is_null() {
            return;
        }

        let len = (*list).length.max(0) as usize;
        let cells = (*list).elements;
        if cells.is_null() {
            return;
        }

        for i in 0..len {
            let cell = *cells.add(i);
            let ptr = cell.ptr_value as *mut pg_sys::DefElem;
            if ptr.is_null() {
                continue;
            }

            let name_c = (*ptr).defname;
            if name_c.is_null() {
                continue;
            }
            let name = CStr::from_ptr(name_c).to_string_lossy().to_string();

            let val_c = pg_sys::defGetString(ptr);
            if val_c.is_null() {
                continue;
            }
            let value = CStr::from_ptr(val_c).to_string_lossy().to_string();

            out.insert(name, value);
        }
    }
}
