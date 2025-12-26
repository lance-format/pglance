use crate::fdw::convert::{arrow_value_to_datum, validate_arrow_type_for_pg_oid, ConvertErrorKind};
use crate::fdw::namespace::connect_namespace;
use crate::fdw::options::{LanceDatasetSource, LanceFdwOptions};
use futures::StreamExt;
use lance_rs::dataset::builder::DatasetBuilder;
use lance_rs::dataset::scanner::DatasetRecordBatchStream;
use lance_rs::Dataset;
use pgrx::pg_sys;
use pgrx::{ereport, PgSqlErrorCode};
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
    attnames: Vec<Option<String>>,
    att_to_batch_col: Vec<Option<usize>>,
}

fn pg_type_name(oid: pg_sys::Oid) -> String {
    unsafe {
        let ptr = pg_sys::format_type_be(oid);
        if ptr.is_null() {
            return format!("oid {}", oid);
        }
        let s = std::ffi::CStr::from_ptr(ptr).to_string_lossy().to_string();
        pg_sys::pfree(ptr.cast());
        s
    }
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
        ereport!(
            ERROR,
            PgSqlErrorCode::ERRCODE_FDW_INVALID_OPTION_NAME,
            "invalid foreign table options",
            format!("relation_oid={} error={}", relid, e),
        );
    });

    let runtime = Arc::new(Runtime::new().unwrap_or_else(|e| {
        ereport!(
            ERROR,
            PgSqlErrorCode::ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
            "failed to create tokio runtime",
            format!("error={}", e),
        );
    }));

    let dataset = match &opts.source {
        LanceDatasetSource::Uri { uri } => runtime
            .block_on(async { Dataset::open(uri).await })
            .unwrap_or_else(|e| {
                ereport!(
                    ERROR,
                    PgSqlErrorCode::ERRCODE_FDW_TABLE_NOT_FOUND,
                    "failed to open lance dataset",
                    format!("uri={} error={}", uri, e),
                );
            }),
        LanceDatasetSource::Namespace {
            server_oid,
            table_id,
        } => {
            let namespace = connect_namespace(runtime.as_ref(), *server_oid).unwrap_or_else(|e| {
                ereport!(
                    ERROR,
                    PgSqlErrorCode::ERRCODE_FDW_INVALID_OPTION_NAME,
                    "invalid namespace server options",
                    format!("server_oid={} error={}", server_oid, e),
                );
            });

            let dataset_res: lance_rs::Result<Dataset> = runtime.block_on(async {
                DatasetBuilder::from_namespace(namespace, table_id.clone(), false)
                    .await?
                    .load()
                    .await
            });

            dataset_res.unwrap_or_else(|e| {
                ereport!(
                    ERROR,
                    PgSqlErrorCode::ERRCODE_FDW_TABLE_NOT_FOUND,
                    "failed to open lance dataset via namespace",
                    format!("{} error={}", opts.dataset_label(), e),
                );
            })
        }
    };

    let stream = create_stream(&runtime, &dataset, &opts.dataset_label(), opts.batch_size);

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

    let dataset_fields = &dataset.schema().fields;
    let dataset_field_names: Vec<String> = dataset_fields.iter().map(|f| f.name.clone()).collect();
    let mut name_to_idx = std::collections::BTreeMap::<String, usize>::new();
    for (idx, f) in dataset_fields.iter().enumerate() {
        name_to_idx.insert(f.name.clone(), idx);
    }

    let mut att_to_batch_col = Vec::with_capacity(natts);
    for (att_idx, name) in attnames.iter().enumerate() {
        if let Some(name) = name {
            let idx = name_to_idx.get(name).copied().unwrap_or_else(|| {
                ereport!(
                    ERROR,
                    PgSqlErrorCode::ERRCODE_FDW_COLUMN_NAME_NOT_FOUND,
                    "column not found in lance dataset schema",
                    format!(
                        "{} column={} dataset_columns={}",
                        opts.dataset_label(),
                        name,
                        dataset_field_names.join(",")
                    ),
                );
            });

            let field = &dataset_fields[idx];
            let arrow_type = field.data_type();
            if let Err(e) = validate_arrow_type_for_pg_oid(&arrow_type, atttypids[att_idx]) {
                let (errcode, message) = match e.kind {
                    ConvertErrorKind::TypeMismatch => (
                        PgSqlErrorCode::ERRCODE_FDW_INVALID_DATA_TYPE_DESCRIPTORS,
                        "column type mismatch between foreign table and dataset schema",
                    ),
                    ConvertErrorKind::UnsupportedType | ConvertErrorKind::ValueOutOfRange => (
                        PgSqlErrorCode::ERRCODE_FDW_INVALID_DATA_TYPE,
                        "unsupported column type for lance_fdw",
                    ),
                    ConvertErrorKind::Internal => (
                        PgSqlErrorCode::ERRCODE_FDW_ERROR,
                        "internal lance_fdw schema validation error",
                    ),
                };
                ereport!(
                    ERROR,
                    errcode,
                    message,
                    format!(
                        "{} column={} arrow_type={} pg_type={} error={}",
                        opts.dataset_label(),
                        name,
                        arrow_type,
                        pg_type_name(atttypids[att_idx]),
                        e
                    ),
                );
            }

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
        attnames: attnames.clone(),
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
                Some(Err(e)) => {
                    ereport!(
                        ERROR,
                        PgSqlErrorCode::ERRCODE_FDW_ERROR,
                        "failed to read next record batch",
                        format!("{} error={}", state.opts.dataset_label(), e),
                    );
                }
                Some(Ok(batch)) => {
                    state.current_batch = Some(batch);
                    state.current_row = 0;
                }
            }
        }

        let Some(batch) = state.current_batch.as_ref() else {
            state.current_row = 0;
            continue;
        };
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
                    ereport!(
                        ERROR,
                        PgSqlErrorCode::ERRCODE_FDW_INCONSISTENT_DESCRIPTOR_INFORMATION,
                        "missing batch column mapping for attribute",
                        format!("attribute_number={}", i + 1),
                    );
                });
            let col = batch.column(batch_idx);
            let (datum, isnull) = arrow_value_to_datum(col.as_ref(), row, state.atttypids[i])
                .unwrap_or_else(|e| {
                    let col_name = state
                        .attnames
                        .get(i)
                        .and_then(|v| v.as_deref())
                        .unwrap_or("<unknown>");
                    let (errcode, message) = match e.kind {
                        ConvertErrorKind::TypeMismatch => (
                            PgSqlErrorCode::ERRCODE_FDW_INVALID_DATA_TYPE_DESCRIPTORS,
                            "column type mismatch between foreign table and dataset schema",
                        ),
                        ConvertErrorKind::UnsupportedType | ConvertErrorKind::ValueOutOfRange => (
                            PgSqlErrorCode::ERRCODE_FDW_INVALID_DATA_TYPE,
                            "unsupported column type for lance_fdw",
                        ),
                        ConvertErrorKind::Internal => (
                            PgSqlErrorCode::ERRCODE_FDW_ERROR,
                            "internal lance_fdw conversion error",
                        ),
                    };
                    ereport!(
                        ERROR,
                        errcode,
                        message,
                        format!(
                            "{} column={} arrow_type={} pg_type={} error={}",
                            state.opts.dataset_label(),
                            col_name,
                            col.data_type(),
                            pg_type_name(state.atttypids[i]),
                            e
                        ),
                    );
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
    let stream = create_stream(
        &state.runtime,
        &state.dataset,
        &state.opts.dataset_label(),
        state.opts.batch_size,
    );
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
        match &opts.source {
            LanceDatasetSource::Uri { uri } => {
                let uri_label = match CString::new("Lance URI") {
                    Ok(v) => v,
                    Err(_) => return,
                };
                let uri_value = match CString::new(uri.replace('\0', "")) {
                    Ok(v) => v,
                    Err(_) => return,
                };
                pg_sys::ExplainPropertyText(uri_label.as_ptr(), uri_value.as_ptr(), es);
            }
            LanceDatasetSource::Namespace {
                server_oid,
                table_id,
            } => {
                let table_id_label = match CString::new("Lance Table ID") {
                    Ok(v) => v,
                    Err(_) => return,
                };
                let table_id_json = serde_json::to_string(table_id).unwrap_or_else(|_| "[]".into());
                let table_id_value = match CString::new(table_id_json.replace('\0', "")) {
                    Ok(v) => v,
                    Err(_) => return,
                };
                pg_sys::ExplainPropertyText(table_id_label.as_ptr(), table_id_value.as_ptr(), es);

                let server_label = match CString::new("Lance Namespace Server OID") {
                    Ok(v) => v,
                    Err(_) => return,
                };
                pg_sys::ExplainPropertyInteger(
                    server_label.as_ptr(),
                    std::ptr::null(),
                    i64::from(u32::from(*server_oid)),
                    es,
                );
            }
        }

        let batch_label = match CString::new("Batch Size") {
            Ok(v) => v,
            Err(_) => return,
        };
        pg_sys::ExplainPropertyInteger(
            batch_label.as_ptr(),
            std::ptr::null(),
            opts.batch_size as i64,
            es,
        );

        let projection_label = match CString::new("Projection") {
            Ok(v) => v,
            Err(_) => return,
        };

        let projection = format_projection_list(relation);
        let projection_value = match CString::new(projection) {
            Ok(v) => v,
            Err(_) => return,
        };
        pg_sys::ExplainPropertyText(projection_label.as_ptr(), projection_value.as_ptr(), es);
    }
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
    dataset_label: &str,
    batch_size: usize,
) -> DatasetRecordBatchStream {
    let mut scanner = dataset.scan();
    scanner.batch_size(batch_size);
    runtime
        .block_on(async { scanner.try_into_stream().await })
        .unwrap_or_else(|e| {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
                "failed to create scanner stream",
                format!("{} error={}", dataset_label, e),
            );
        })
}
