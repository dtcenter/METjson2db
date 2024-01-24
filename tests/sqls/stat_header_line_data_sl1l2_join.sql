select
    *
from
    mv_gsl_global_g2g_rt.stat_header h,
    mv_gsl_global_g2g_rt.line_data_sl1l2 ld
where
    h.stat_header_id = ld.stat_header_id