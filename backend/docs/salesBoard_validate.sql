/*
    salesBoard 上线前验证脚本

    用法：
    1. 先执行 salesBoard.sql 创建/更新视图。
    2. 在测试库或生产镜像库执行本脚本。
    3. 逐段核对输出结果，重点看：
       - 视图总行数是否符合预期
       - 房源数据是否一房一行
       - 未转认筹金额是否成功进入视图
       - 未收首付/按揭/分期金额拆分是否正确
       - 关键字段是否存在异常空值、负值、重复行
*/

SET NOCOUNT ON;

PRINT 'Step 1: 视图总览';
SELECT
    COUNT(1) AS total_rows,
    SUM(CASE WHEN fyid IS NOT NULL THEN 1 ELSE 0 END) AS house_rows,
    SUM(CASE WHEN fyid IS NULL THEN 1 ELSE 0 END) AS wzrg_rows
FROM dbo.salesBoard;

PRINT 'Step 2: 视图主键是否稳定且唯一';
SELECT
    COUNT(1) AS total_rows,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(1) - COUNT(DISTINCT id) AS duplicate_id_rows
FROM dbo.salesBoard;

PRINT 'Step 3: 房源级数据是否一房一行';
SELECT TOP (50)
    fyid,
    COUNT(1) AS row_count
FROM dbo.salesBoard
WHERE fyid IS NOT NULL
GROUP BY fyid
HAVING COUNT(1) > 1
ORDER BY row_count DESC, fyid DESC;

PRINT 'Step 4: 未转认筹数据是否进入视图';
WITH wzrg_src AS (
    SELECT
        COALESCE(sk.szgs, tk.szgs) AS szfb,
        ISNULL(sk.rcsk, 0) AS skze,
        ISNULL(tk.rctk, 0) AS tkze
    FROM (
        SELECT
            mt.szgs,
            SUM(ISNULL(dt.bcjkje, 0)) AS rcsk
        FROM dbo.uf_skjltz AS mt
        INNER JOIN dbo.uf_skjltz_dt1 AS dt
            ON mt.id = dt.mainid
        WHERE mt.rcxybh IS NOT NULL
          AND LTRIM(RTRIM(mt.rcxybh)) <> ''
          AND NOT EXISTS (
              SELECT 1
              FROM dbo.uf_rgxytz_dt3 AS d3
              WHERE d3.rcbh = mt.rcxybh
          )
        GROUP BY mt.szgs
    ) AS sk
    FULL OUTER JOIN (
        SELECT
            gs AS szgs,
            SUM(ISNULL(kckx, 0) + ISNULL(bcje, 0) + ISNULL(tkje, 0)) AS rctk
        FROM dbo.uf_tkjltz
        WHERE rgxybm IS NULL
           OR LTRIM(RTRIM(rgxybm)) = ''
        GROUP BY gs
    ) AS tk
        ON sk.szgs = tk.szgs
)
SELECT
    s.szfb,
    s.skze AS src_skze,
    s.tkze AS src_tkze,
    v.skze AS view_skze,
    v.tkze AS view_tkze
FROM wzrg_src AS s
LEFT JOIN dbo.salesBoard AS v
    ON v.fyid IS NULL
   AND v.szfb = s.szfb
WHERE ISNULL(s.skze, 0) <> ISNULL(v.skze, 0)
   OR ISNULL(s.tkze, 0) <> ISNULL(v.tkze, 0)
   OR v.id IS NULL
ORDER BY s.szfb;

PRINT 'Step 5: 认购和签约是否存在一对多放大量';
WITH rgtz_count AS (
    SELECT
        fwbh,
        COUNT(1) AS cnt
    FROM dbo.uf_rgxytz
    WHERE xyzt = 0
    GROUP BY fwbh
    HAVING COUNT(1) > 1
),
qytz_count AS (
    SELECT
        rgxybm,
        COUNT(1) AS cnt
    FROM dbo.uf_qysptz
    WHERE xyzt = 0
    GROUP BY rgxybm
    HAVING COUNT(1) > 1
)
SELECT
    'uf_rgxytz' AS src_table,
    COUNT(1) AS duplicate_business_keys
FROM rgtz_count
UNION ALL
SELECT
    'uf_qysptz' AS src_table,
    COUNT(1) AS duplicate_business_keys
FROM qytz_count;

PRINT 'Step 6: 检查退款金额符号语义';
SELECT TOP (20)
    id,
    fwbh,
    skjezzhj,
    ytjehj,
    CASE
        WHEN ytjehj < 0 THEN N'退款字段为负数，当前视图算法可能需要改回减法'
        ELSE N'退款字段为正数，当前视图算法保持加回'
    END AS sign_hint
FROM dbo.uf_rgxytz
WHERE xyzt = 0
  AND ISNULL(ytjehj, 0) <> 0
ORDER BY id DESC;

PRINT 'Step 7: 检查未收金额是否出现负值或异常';
SELECT TOP (50)
    fyid,
    fwbh,
    qyzj,
    skze,
    tkze,
    sfkje,
    dkje,
    wssfje,
    wsajje,
    fqwfk
FROM dbo.salesBoard
WHERE ISNULL(wssfje, 0) < 0
   OR ISNULL(wsajje, 0) < 0
   OR ISNULL(fqwfk, 0) < 0
ORDER BY fyid DESC;

PRINT 'Step 8: 检查贷款类拆分是否守恒';
SELECT TOP (100)
    fyid,
    fwbh,
    fkfs,
    qyzj,
    skze,
    tkze,
    sfkje,
    dkje,
    ISNULL(wssfje, 0) + ISNULL(wsajje, 0) AS calc_total,
    CASE
        WHEN qyzj IS NULL THEN NULL
        WHEN qyzj - ISNULL(skze, 0) + ISNULL(tkze, 0) < 0 THEN 0
        ELSE qyzj - ISNULL(skze, 0) + ISNULL(tkze, 0)
    END AS expected_wskje
FROM dbo.salesBoard
WHERE fyid IS NOT NULL
  AND fkfs IN (1, 2, 3)
  AND ISNULL(wssfje, 0) + ISNULL(wsajje, 0) <>
      CASE
          WHEN qyzj IS NULL THEN NULL
          WHEN qyzj - ISNULL(skze, 0) + ISNULL(tkze, 0) < 0 THEN 0
          ELSE qyzj - ISNULL(skze, 0) + ISNULL(tkze, 0)
      END
ORDER BY fyid DESC;

PRINT 'Step 9: 检查分期类拆分是否守恒';
SELECT TOP (100)
    fyid,
    fwbh,
    fkfs,
    qyzj,
    skze,
    tkze,
    fqwfk,
    CASE
        WHEN qyzj IS NULL THEN NULL
        WHEN qyzj - ISNULL(skze, 0) + ISNULL(tkze, 0) < 0 THEN 0
        ELSE qyzj - ISNULL(skze, 0) + ISNULL(tkze, 0)
    END AS expected_wskje
FROM dbo.salesBoard
WHERE fyid IS NOT NULL
  AND fkfs IN (0, 4)
  AND ISNULL(fqwfk, 0) <>
      CASE
          WHEN qyzj IS NULL THEN NULL
          WHEN qyzj - ISNULL(skze, 0) + ISNULL(tkze, 0) < 0 THEN 0
          ELSE qyzj - ISNULL(skze, 0) + ISNULL(tkze, 0)
      END
ORDER BY fyid DESC;

PRINT 'Step 10: 检查视图关键空值';
SELECT
    SUM(CASE WHEN fyid IS NOT NULL AND fwbh IS NULL THEN 1 ELSE 0 END) AS house_without_fwbh,
    SUM(CASE WHEN fyid IS NOT NULL AND szfb IS NULL THEN 1 ELSE 0 END) AS house_without_szfb,
    SUM(CASE WHEN fyid IS NOT NULL AND qyzj IS NULL AND skze IS NOT NULL THEN 1 ELSE 0 END) AS rg_without_qy
FROM dbo.salesBoard;

PRINT 'Step 11: 抽样查看最新房源记录';
SELECT TOP (50)
    *
FROM dbo.salesBoard
WHERE fyid IS NOT NULL
ORDER BY fyid DESC;

PRINT 'Step 12: 抽样查看未转认筹记录';
SELECT TOP (50)
    *
FROM dbo.salesBoard
WHERE fyid IS NULL
ORDER BY szfb;
