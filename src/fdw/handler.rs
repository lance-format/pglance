use crate::fdw::scan;
use pgrx::pg_sys;
use pgrx::PgBox;

pub unsafe fn build_fdw_routine() -> PgBox<pg_sys::FdwRoutine> {
    let mut routine = PgBox::<pg_sys::FdwRoutine>::alloc0();

    routine.type_ = pg_sys::NodeTag::T_FdwRoutine;

    routine.GetForeignRelSize = Some(scan::get_foreign_rel_size);
    routine.GetForeignPaths = Some(scan::get_foreign_paths);
    routine.GetForeignPlan = Some(scan::get_foreign_plan);

    routine.BeginForeignScan = Some(scan::begin_foreign_scan);
    routine.IterateForeignScan = Some(scan::iterate_foreign_scan);
    routine.ReScanForeignScan = Some(scan::rescan_foreign_scan);
    routine.EndForeignScan = Some(scan::end_foreign_scan);

    routine.ExplainForeignScan = Some(scan::explain_foreign_scan);

    routine.into_pg_boxed()
}
