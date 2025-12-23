use crate::fdw::convert::arrow_value_to_datum;
use crate::fdw::options::LanceFdwOptions;
use futures::StreamExt;
use lance_rs::dataset::scanner::DatasetRecordBatchStream;
use lance_rs::Dataset;
use pgrx::pg_sys;
use std::ffi::CString;
use std::pin::Pin;
use std::sync::Arc;
use tokio::runtime::Runtime;

pub struct LanceScanState {
    runtime: Arc<Runtime>,
    dataset: Dataset,
    opts: LanceFdwOptions,
    stream: Pin<Box<DatasetRecordBatchStream>>,
    current_batch: Option<arrow::record_batch::RecordBatch>,
    current_row: usize,
    atttypids: Vec<pg_sys::Oid>,
    att_to_batch_col: Vec<Option<usize>>,
}

#[pgrx::pg_guard]
pub unsafe extern "C-unwind" fn get_foreign_rel_size(
    _root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    _foreigntableid: pg_sys::Oid,
) {
    if baserel.is_null() {
        return;
    }
    (*baserel).rows = 1000.0;
}

#[pgrx::pg_guard]
pub unsafe extern "C-unwind" fn get_foreign_paths(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    _foreigntableid: pg_sys::Oid,
) {
    if root.is_null() || baserel.is_null() {
        return;
    }

    let rows = (*baserel).rows;
    let startup_cost: f64 = 0.0;
    let total_cost: f64 = rows.max(1.0);

    let path = pg_sys::create_foreignscan_path(
        root,
        baserel,
        (*baserel).reltarget,
        rows,
        startup_cost,
        total_cost,
        std::ptr::null_mut(),
        std::ptr::null_mut(),
        std::ptr::null_mut(),
        std::ptr::null_mut(),
    );
    pg_sys::add_path(baserel, path.cast());
}

#[pgrx::pg_guard]
pub unsafe extern "C-unwind" fn get_foreign_plan(
    _root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    _foreigntableid: pg_sys::Oid,
    _best_path: *mut pg_sys::ForeignPath,
    tlist: *mut pg_sys::List,
    scan_clauses: *mut pg_sys::List,
    outer_plan: *mut pg_sys::Plan,
) -> *mut pg_sys::ForeignScan {
    let qpqual = pg_sys::extract_actual_clauses(scan_clauses, false);
    pg_sys::make_foreignscan(
        tlist,
        qpqual,
        if baserel.is_null() {
            0
        } else {
            (*baserel).relid
        },
        std::ptr::null_mut(),
        std::ptr::null_mut(),
        std::ptr::null_mut(),
        std::ptr::null_mut(),
        outer_plan,
    )
}

#[pgrx::pg_guard]
pub unsafe extern "C-unwind" fn begin_foreign_scan(
    node: *mut pg_sys::ForeignScanState,
    eflags: std::ffi::c_int,
) {
    if node.is_null() {
        return;
    }

    if (eflags & pg_sys::EXEC_FLAG_EXPLAIN_ONLY as i32) != 0 {
        return;
    }

    let relation = (*node).ss.ss_currentRelation;
    if relation.is_null() {
        pgrx::error!("missing current relation");
    }
    let relid = (*relation).rd_id;

    let opts = LanceFdwOptions::from_foreign_table(relid).unwrap_or_else(|e| {
        pgrx::error!("invalid foreign table options: {}", e);
    });

    let runtime = Arc::new(Runtime::new().unwrap_or_else(|e| {
        pgrx::error!("failed to create tokio runtime: {}", e);
    }));

    let dataset = runtime
        .block_on(async { Dataset::open(&opts.uri).await })
        .unwrap_or_else(|e| {
            pgrx::error!("failed to open dataset {}: {}", opts.uri, e);
        });

    let stream = create_stream(&runtime, &dataset, opts.batch_size);

    let tupdesc = (*relation).rd_att;
    if tupdesc.is_null() {
        pgrx::error!("missing tuple descriptor");
    }
    let natts = (*tupdesc).natts.max(0) as usize;
    let mut atttypids = Vec::with_capacity(natts);
    let mut attnames = Vec::with_capacity(natts);
    for i in 0..natts {
        let attr = *(*tupdesc).attrs.as_ptr().add(i);
        atttypids.push(attr.atttypid);
        if attr.attisdropped {
            attnames.push(None);
        } else {
            let name = std::ffi::CStr::from_ptr(attr.attname.data.as_ptr())
                .to_string_lossy()
                .to_string();
            attnames.push(Some(name));
        }
    }

    let dataset_field_names: Vec<String> = dataset
        .schema()
        .fields
        .iter()
        .map(|f| f.name.clone())
        .collect();

    let mut att_to_batch_col = Vec::with_capacity(natts);
    for name in attnames {
        if let Some(name) = name {
            let idx = dataset_field_names
                .iter()
                .position(|n| n == &name)
                .unwrap_or_else(|| {
                    pgrx::error!("column not found in dataset schema: {}", name);
                });
            att_to_batch_col.push(Some(idx));
        } else {
            att_to_batch_col.push(None);
        }
    }

    let state = Box::new(LanceScanState {
        runtime,
        dataset,
        opts,
        stream: Box::pin(stream),
        current_batch: None,
        current_row: 0,
        atttypids,
        att_to_batch_col,
    });

    (*node).fdw_state = Box::into_raw(state) as *mut std::ffi::c_void;
}

#[pgrx::pg_guard]
pub unsafe extern "C-unwind" fn iterate_foreign_scan(
    node: *mut pg_sys::ForeignScanState,
) -> *mut pg_sys::TupleTableSlot {
    if node.is_null() {
        return std::ptr::null_mut();
    }

    let slot = (*node).ss.ss_ScanTupleSlot;
    if slot.is_null() {
        return std::ptr::null_mut();
    }

    pg_sys::ExecClearTuple(slot);

    let state_ptr = (*node).fdw_state as *mut LanceScanState;
    if state_ptr.is_null() {
        return slot;
    }
    let state = &mut *state_ptr;

    loop {
        let need_batch = match &state.current_batch {
            None => true,
            Some(batch) => state.current_row >= batch.num_rows(),
        };

        if need_batch {
            let next = state.runtime.block_on(async { state.stream.next().await });
            match next {
                None => return slot,
                Some(Err(e)) => pgrx::error!("failed to read next batch: {}", e),
                Some(Ok(batch)) => {
                    state.current_batch = Some(batch);
                    state.current_row = 0;
                }
            }
        }

        let batch = state.current_batch.as_ref().unwrap();
        if state.current_row >= batch.num_rows() {
            state.current_batch = None;
            continue;
        }

        let row = state.current_row;
        state.current_row += 1;

        let tupdesc = (*slot).tts_tupleDescriptor;
        if tupdesc.is_null() {
            pgrx::error!("missing slot tuple descriptor");
        }

        let natts = (*tupdesc).natts.max(0) as usize;
        for i in 0..natts {
            let attr = *(*tupdesc).attrs.as_ptr().add(i);
            if attr.attisdropped {
                *(*slot).tts_values.add(i) = pg_sys::Datum::from(0usize);
                *(*slot).tts_isnull.add(i) = true;
                continue;
            }

            let batch_idx = state
                .att_to_batch_col
                .get(i)
                .copied()
                .flatten()
                .unwrap_or_else(|| {
                    pgrx::error!("missing batch column mapping for attribute {}", i + 1);
                });
            let col = batch.column(batch_idx);
            let (datum, isnull) = arrow_value_to_datum(col.as_ref(), row, state.atttypids[i])
                .unwrap_or_else(|e| {
                    pgrx::error!("failed to convert column {}: {}", i + 1, e);
                });
            *(*slot).tts_values.add(i) = datum;
            *(*slot).tts_isnull.add(i) = isnull;
        }

        pg_sys::ExecStoreVirtualTuple(slot);
        return slot;
    }
}

#[pgrx::pg_guard]
pub unsafe extern "C-unwind" fn rescan_foreign_scan(node: *mut pg_sys::ForeignScanState) {
    if node.is_null() {
        return;
    }

    let state_ptr = (*node).fdw_state as *mut LanceScanState;
    if state_ptr.is_null() {
        return;
    }
    let state = &mut *state_ptr;
    let stream = create_stream(&state.runtime, &state.dataset, state.opts.batch_size);
    state.stream = Box::pin(stream);
    state.current_batch = None;
    state.current_row = 0;
}

#[pgrx::pg_guard]
pub unsafe extern "C-unwind" fn end_foreign_scan(node: *mut pg_sys::ForeignScanState) {
    if node.is_null() {
        return;
    }

    let state_ptr = (*node).fdw_state as *mut LanceScanState;
    if state_ptr.is_null() {
        return;
    }

    drop(Box::from_raw(state_ptr));
    (*node).fdw_state = std::ptr::null_mut();
}

#[pgrx::pg_guard]
pub unsafe extern "C-unwind" fn explain_foreign_scan(
    node: *mut pg_sys::ForeignScanState,
    es: *mut pg_sys::ExplainState,
) {
    if node.is_null() || es.is_null() {
        return;
    }

    let relation = (*node).ss.ss_currentRelation;
    if relation.is_null() {
        return;
    }
    let relid = (*relation).rd_id;

    let opts = LanceFdwOptions::from_foreign_table(relid).ok();
    if let Some(opts) = opts {
        let uri_c = CString::new(opts.uri).unwrap_or_else(|_| {
            pgrx::error!("invalid foreign table option: uri contains NUL byte");
        });
        pg_sys::ExplainPropertyText(cstring_label("Lance URI").as_ptr(), uri_c.as_ptr(), es);

        pg_sys::ExplainPropertyInteger(
            cstring_label("Batch Size").as_ptr(),
            std::ptr::null(),
            opts.batch_size as i64,
            es,
        );

        let projection = format_projection_list(relation);
        let projection_c = CString::new(projection).unwrap_or_else(|_| {
            pgrx::error!("invalid projection list: contains NUL byte");
        });
        pg_sys::ExplainPropertyText(
            cstring_label("Projection").as_ptr(),
            projection_c.as_ptr(),
            es,
        );
    }
}

fn cstring_label(s: &'static str) -> CString {
    CString::new(s).expect("static label must not contain NUL")
}

fn format_projection_list(relation: *mut pg_sys::RelationData) -> String {
    unsafe {
        if relation.is_null() {
            return "[]".to_string();
        }

        let tupdesc = (*relation).rd_att;
        if tupdesc.is_null() {
            return "[]".to_string();
        }

        let natts = (*tupdesc).natts.max(0) as usize;
        let mut cols = Vec::with_capacity(natts);

        for i in 0..natts {
            let attr = *(*tupdesc).attrs.as_ptr().add(i);
            if attr.attisdropped {
                continue;
            }

            let name = std::ffi::CStr::from_ptr(attr.attname.data.as_ptr())
                .to_string_lossy()
                .to_string();
            cols.push(name);
        }

        if cols.is_empty() {
            "[]".to_string()
        } else {
            cols.join(", ")
        }
    }
}

fn create_stream(
    runtime: &Arc<Runtime>,
    dataset: &Dataset,
    batch_size: usize,
) -> DatasetRecordBatchStream {
    let mut scanner = dataset.scan();
    scanner.batch_size(batch_size);
    runtime
        .block_on(async { scanner.try_into_stream().await })
        .unwrap_or_else(|e| pgrx::error!("failed to create scanner stream: {}", e))
}
