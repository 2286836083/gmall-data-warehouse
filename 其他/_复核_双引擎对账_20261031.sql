-- 双引擎对账复核（只读 SELECT，不写任何表）
-- 用途：一次性复现「8 条 GMV 路径」+「8 张表行数逐项一致」+「关键 KPI 一致」
-- 来源口径：实验分析/实验12_Spark接入DolphinScheduler.md §7.1、§4.7；实验10 §1.3/§7.4

-- ===== A. 8 条 GMV 路径 =====
select 'A1 dws_trade_day(gmall)'            as path, cast(round(sum(gmv),2) as string) as gmv from gmall.dws_trade_day
union all select 'A2 dws_product(gmall)',    cast(round(sum(gmv),2) as string) from gmall.dws_product_event_day
union all select 'A3 dws_category(gmall)',   cast(round(sum(gmv),2) as string) from gmall.dws_category_event_day
union all select 'A4 ads_trade_day(gmall)',  cast(round(sum(gmv),2) as string) from gmall.ads_trade_day
union all select 'A5 dwd_purchase_fact',     cast(round(sum(amount),2) as string) from gmall.dwd_purchase_fact_di
union all select 'A6 dws_trade(bench)',      cast(round(sum(gmv),2) as string) from gmall_bench.dws_trade_day
union all select 'A7 dws_product(bench)',    cast(round(sum(gmv),2) as string) from gmall_bench.dws_product_event_day
union all select 'A8 dws_category(bench)',   cast(round(sum(gmv),2) as string) from gmall_bench.dws_category_event_day;

-- ===== B. 8 张表行数：Hive 正式库 vs Spark 隔离库 =====
select 'dws_user.gmall'      as src, count(*) as cnt from gmall.dws_user_event_day
union all select 'dws_user.spark',       count(*) from gmall_bench.dws_user_event_day
union all select 'dws_product.gmall',    count(*) from gmall.dws_product_event_day
union all select 'dws_product.spark',    count(*) from gmall_bench.dws_product_event_day
union all select 'dws_category.gmall',   count(*) from gmall.dws_category_event_day
union all select 'dws_category.spark',   count(*) from gmall_bench.dws_category_event_day
union all select 'dws_trade.gmall',      count(*) from gmall.dws_trade_day
union all select 'dws_trade.spark',      count(*) from gmall_bench.dws_trade_day
union all select 'ads_active.gmall',     count(*) from gmall.ads_active_user_day
union all select 'ads_active.spark',     count(*) from gmall_bench.ads_active_user_day
union all select 'ads_funnel.gmall',     count(*) from gmall.ads_funnel_day
union all select 'ads_funnel.spark',     count(*) from gmall_bench.ads_funnel_day
union all select 'ads_repurchase.gmall', count(*) from gmall.ads_repurchase_day
union all select 'ads_repurchase.spark', count(*) from gmall_bench.ads_repurchase_day
union all select 'ads_topn.gmall',       count(*) from gmall.ads_top_product_day
union all select 'ads_topn.spark',       count(*) from gmall_bench.ads_top_product_day;

-- ===== C. 关键 KPI 与分区完整性 =====
select 'GMV.gmall'        as k, cast(round(sum(gmv),2) as string) as v from gmall.dws_trade_day
union all select 'GMV.spark',        cast(round(sum(gmv),2) as string) from gmall_bench.dws_trade_day
union all select 'orders.gmall',     cast(sum(order_cnt) as string) from gmall.dws_trade_day
union all select 'orders.spark',     cast(sum(order_cnt) as string) from gmall_bench.dws_trade_day
union all select 'buyer.gmall',      cast(sum(buyer_cnt) as string) from gmall.dws_trade_day
union all select 'buyer.spark',      cast(sum(buyer_cnt) as string) from gmall_bench.dws_trade_day
union all select 'dau.gmall',        cast(sum(dau) as string) from gmall.ads_active_user_day
union all select 'dau.spark',        cast(sum(dau) as string) from gmall_bench.ads_active_user_day;

select 'part.dws_user.gmall'    as k, cast(count(*) as string) as v from (select distinct dt from gmall.dws_user_event_day) t
union all select 'part.dws_user.spark',    cast(count(*) as string) from (select distinct dt from gmall_bench.dws_user_event_day) t
union all select 'part.dws_product.gmall', cast(count(*) as string) from (select distinct dt from gmall.dws_product_event_day) t
union all select 'part.dws_product.spark', cast(count(*) as string) from (select distinct dt from gmall_bench.dws_product_event_day) t;
