use pgrx::pg_sys;
use std::collections::BTreeMap;
use std::ffi::CStr;

#[derive(Debug, Clone)]
pub struct LanceFdwOptions {
    pub uri: String,
    pub batch_size: usize,
}

impl LanceFdwOptions {
    pub fn from_foreign_table(foreign_table_id: pg_sys::Oid) -> Result<Self, &'static str> {
        unsafe {
            let foreign_table = pg_sys::GetForeignTable(foreign_table_id);
            if foreign_table.is_null() {
                return Err("missing ForeignTable");
            }

            let server = pg_sys::GetForeignServer((*foreign_table).serverid);
            if server.is_null() {
                return Err("missing ForeignServer");
            }

            let mut opts = BTreeMap::<String, String>::new();
            parse_defelem_list((*server).options, &mut opts);
            parse_defelem_list((*foreign_table).options, &mut opts);

            let uri = opts
                .get("uri")
                .cloned()
                .ok_or("missing required option: uri")?;

            let batch_size = opts
                .get("batch_size")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(1024);

            Ok(Self { uri, batch_size })
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
