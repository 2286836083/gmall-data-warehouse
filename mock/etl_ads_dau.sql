USE gmall;
SET hive.exec.mode.local.auto=false;
TRUNCATE TABLE ads_dau;

-- 08-20 (首日: 无历史, 无昨日)
INSERT INTO ads_dau
SELECT '2026-08-20',
  COUNT(DISTINCT t.user_id) dau,
  0 new_dau,
  0 active_dau,
  0 wakeup_dau
FROM (SELECT DISTINCT user_id FROM dws_user_day WHERE dt='2026-08-20' AND pv_cnt>0) t;

-- 08-21
INSERT INTO ads_dau
SELECT '2026-08-21',
  COUNT(DISTINCT t.user_id) dau,
  COUNT(DISTINCT CASE WHEN h.user_id IS NULL THEN t.user_id END) new_dau,
  COUNT(DISTINCT CASE WHEN y.user_id IS NOT NULL THEN t.user_id END) active_dau,
  COUNT(DISTINCT CASE WHEN y.user_id IS NULL AND h.user_id IS NOT NULL THEN t.user_id END) wakeup_dau
FROM (SELECT DISTINCT user_id FROM dws_user_day WHERE dt='2026-08-21' AND pv_cnt>0) t
LEFT JOIN (SELECT DISTINCT user_id FROM dws_user_day WHERE dt < '2026-08-21' AND pv_cnt>0) h ON t.user_id=h.user_id
LEFT JOIN (SELECT DISTINCT user_id FROM dws_user_day WHERE dt='2026-08-20' AND pv_cnt>0) y ON t.user_id=y.user_id;

-- 08-22
INSERT INTO ads_dau
SELECT '2026-08-22',
  COUNT(DISTINCT t.user_id) dau,
  COUNT(DISTINCT CASE WHEN h.user_id IS NULL THEN t.user_id END) new_dau,
  COUNT(DISTINCT CASE WHEN y.user_id IS NOT NULL THEN t.user_id END) active_dau,
  COUNT(DISTINCT CASE WHEN y.user_id IS NULL AND h.user_id IS NOT NULL THEN t.user_id END) wakeup_dau
FROM (SELECT DISTINCT user_id FROM dws_user_day WHERE dt='2026-08-22' AND pv_cnt>0) t
LEFT JOIN (SELECT DISTINCT user_id FROM dws_user_day WHERE dt < '2026-08-22' AND pv_cnt>0) h ON t.user_id=h.user_id
LEFT JOIN (SELECT DISTINCT user_id FROM dws_user_day WHERE dt='2026-08-21' AND pv_cnt>0) y ON t.user_id=y.user_id;

-- 08-23
INSERT INTO ads_dau
SELECT '2026-08-23',
  COUNT(DISTINCT t.user_id) dau,
  COUNT(DISTINCT CASE WHEN h.user_id IS NULL THEN t.user_id END) new_dau,
  COUNT(DISTINCT CASE WHEN y.user_id IS NOT NULL THEN t.user_id END) active_dau,
  COUNT(DISTINCT CASE WHEN y.user_id IS NULL AND h.user_id IS NOT NULL THEN t.user_id END) wakeup_dau
FROM (SELECT DISTINCT user_id FROM dws_user_day WHERE dt='2026-08-23' AND pv_cnt>0) t
LEFT JOIN (SELECT DISTINCT user_id FROM dws_user_day WHERE dt < '2026-08-23' AND pv_cnt>0) h ON t.user_id=h.user_id
LEFT JOIN (SELECT DISTINCT user_id FROM dws_user_day WHERE dt='2026-08-22' AND pv_cnt>0) y ON t.user_id=y.user_id;

-- 08-24
INSERT INTO ads_dau
SELECT '2026-08-24',
  COUNT(DISTINCT t.user_id) dau,
  COUNT(DISTINCT CASE WHEN h.user_id IS NULL THEN t.user_id END) new_dau,
  COUNT(DISTINCT CASE WHEN y.user_id IS NOT NULL THEN t.user_id END) active_dau,
  COUNT(DISTINCT CASE WHEN y.user_id IS NULL AND h.user_id IS NOT NULL THEN t.user_id END) wakeup_dau
FROM (SELECT DISTINCT user_id FROM dws_user_day WHERE dt='2026-08-24' AND pv_cnt>0) t
LEFT JOIN (SELECT DISTINCT user_id FROM dws_user_day WHERE dt < '2026-08-24' AND pv_cnt>0) h ON t.user_id=h.user_id
LEFT JOIN (SELECT DISTINCT user_id FROM dws_user_day WHERE dt='2026-08-23' AND pv_cnt>0) y ON t.user_id=y.user_id;