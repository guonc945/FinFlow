CREATE OR ALTER VIEW dbo.salesBoard
AS
WITH rgtz_ranked AS (
    SELECT
        id,
        fwbh,
        ISNULL(skjezzhj, 0) AS skze,
        ISNULL(ytjehj, 0) AS tkze,
        yf AS khmc,
        ROW_NUMBER() OVER (
            PARTITION BY fwbh
            ORDER BY id DESC
        ) AS rn
    FROM dbo.uf_rgxytz
    WHERE xyzt = 0
),
rgtz AS (
    SELECT
        id,
        fwbh,
        skze,
        tkze,
        khmc
    FROM rgtz_ranked
    WHERE rn = 1
),
qytz_ranked AS (
    SELECT
        id,
        fwbm,
        rgxybm,
        khxm AS khmc,
        ISNULL(qyzj, 0) AS qyzj,
        qydj,
        fkfs,
        ISNULL(sfkje, 0) AS sfkje,
        ISNULL(dkje, 0) AS dkje,
        ROW_NUMBER() OVER (
            PARTITION BY rgxybm
            ORDER BY id DESC
        ) AS rn
    FROM dbo.uf_qysptz
    WHERE xyzt = 0
),
qytz AS (
    SELECT
        id,
        fwbm,
        rgxybm,
        khmc,
        qyzj,
        qydj,
        fkfs,
        sfkje,
        dkje
    FROM qytz_ranked
    WHERE rn = 1
),
wzrg_rcj_sk AS (
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
),
wzrg_rcj_tk AS (
    SELECT
        gs AS szgs,
        SUM(ISNULL(kckx, 0) + ISNULL(bcje, 0) + ISNULL(tkje, 0)) AS rctk
    FROM dbo.uf_tkjltz
    WHERE rgxybm IS NULL
       OR LTRIM(RTRIM(rgxybm)) = ''
    GROUP BY gs
),
wzrg_rcj AS (
    SELECT
        COALESCE(sk.szgs, tk.szgs) AS szfb,
        ISNULL(sk.rcsk, 0) AS skze,
        ISNULL(tk.rctk, 0) AS tkze
    FROM wzrg_rcj_sk AS sk
    FULL OUTER JOIN wzrg_rcj_tk AS tk
        ON sk.szgs = tk.szgs
),
house_data AS (
    SELECT
        fyxxb.id AS fyid,
        fyxxb.xmmc,
        fyxxb.zh,
        fyxxb.lc,
        fyxxb.dyh,
        fyxxb.fjh,
        fyxxb.yt,
        fyxxb.lx,
        fyxxb.ycjzmj,
        fyxxb.scjzmj,
        fyxxb.yctnmj,
        fyxxb.sctnmj,
        fyxxb.ycgtmj,
        fyxxb.gtmj,
        fyxxb.spdj,
        fyxxb.spzj,
        fyxxb.yhze,
        fyxxb.xszt,
        fyxxb.fwbh,
        qytz.khmc AS qykhmc,
        rgtz.khmc AS rgkhmc,
        fyxxb.dk,
        fyxxb.szfb,
        fyxxb.ks,
        fyxxb.jg,
        fyxxb.hx,
        qytz.qyzj,
        qytz.qydj,
        qytz.fkfs,
        qytz.sfkje,
        qytz.dkje,
        rgtz.skze,
        rgtz.tkze,
        CASE
            WHEN ISNULL(qytz.qyzj, 0) - ISNULL(rgtz.skze, 0) + ISNULL(rgtz.tkze, 0) < 0
                THEN 0
            ELSE ISNULL(qytz.qyzj, 0) - ISNULL(rgtz.skze, 0) + ISNULL(rgtz.tkze, 0)
        END AS wskje
    FROM dbo.uf_fxxxb AS fyxxb
    LEFT JOIN rgtz
        ON fyxxb.id = rgtz.fwbh
    LEFT JOIN qytz
        ON rgtz.id = qytz.rgxybm
),
base_data AS (
    SELECT
        fyid,
        xmmc,
        zh,
        lc,
        dyh,
        fjh,
        yt,
        lx,
        ycjzmj,
        scjzmj,
        yctnmj,
        sctnmj,
        ycgtmj,
        gtmj,
        spdj,
        spzj,
        yhze,
        xszt,
        fwbh,
        qykhmc,
        rgkhmc,
        dk,
        szfb,
        ks,
        jg,
        hx,
        qyzj,
        qydj,
        fkfs,
        sfkje,
        dkje,
        skze,
        tkze,
        wskje,
        N'FY' AS data_type,
        CONCAT(N'FY|', CONVERT(nvarchar(50), fyid)) AS row_key
    FROM house_data

    UNION ALL

    SELECT
        NULL AS fyid,
        NULL AS xmmc,
        NULL AS zh,
        NULL AS lc,
        NULL AS dyh,
        NULL AS fjh,
        NULL AS yt,
        NULL AS lx,
        NULL AS ycjzmj,
        NULL AS scjzmj,
        NULL AS yctnmj,
        NULL AS sctnmj,
        NULL AS ycgtmj,
        NULL AS gtmj,
        NULL AS spdj,
        NULL AS spzj,
        NULL AS yhze,
        NULL AS xszt,
        NULL AS fwbh,
        NULL AS qykhmc,
        NULL AS rgkhmc,
        NULL AS dk,
        wzrg.szfb,
        NULL AS ks,
        NULL AS jg,
        NULL AS hx,
        NULL AS qyzj,
        NULL AS qydj,
        NULL AS fkfs,
        NULL AS sfkje,
        NULL AS dkje,
        wzrg.skze,
        wzrg.tkze,
        NULL AS wskje,
        N'WZRG' AS data_type,
        CONCAT(N'WZRG|', ISNULL(wzrg.szfb, N'')) AS row_key
    FROM wzrg_rcj AS wzrg
    WHERE ISNULL(wzrg.skze, 0) <> 0
       OR ISNULL(wzrg.tkze, 0) <> 0
)
SELECT
    CONVERT(
        uniqueidentifier,
        STUFF(
            STUFF(
                STUFF(
                    STUFF(CONVERT(char(32), HASHBYTES('MD5', row_key), 2), 9, 0, '-'),
                    14,
                    0,
                    '-'
                ),
                19,
                0,
                '-'
            ),
            24,
            0,
            '-'
        )
    ) AS id,
    fyid,
    xmmc,
    zh,
    lc,
    dyh,
    fjh,
    yt,
    lx,
    ycjzmj,
    scjzmj,
    yctnmj,
    sctnmj,
    ycgtmj,
    gtmj,
    spdj,
    spzj,
    yhze,
    xszt,
    fwbh,
    qykhmc,
    rgkhmc,
    dk,
    szfb,
    ks,
    jg,
    hx,
    qyzj,
    qydj,
    fkfs,
    sfkje,
    dkje,
    skze,
    tkze,
    CASE
        WHEN fkfs IN (1, 2, 3) AND wskje > 0
            THEN CASE
                     WHEN wskje >= ISNULL(sfkje, 0) THEN ISNULL(sfkje, 0)
                     ELSE wskje
                 END
        ELSE NULL
    END AS wssfje,
    CASE
        WHEN fkfs IN (1, 2, 3) AND wskje > ISNULL(sfkje, 0)
            THEN CASE
                     WHEN wskje - ISNULL(sfkje, 0) >= ISNULL(dkje, 0) THEN ISNULL(dkje, 0)
                     ELSE wskje - ISNULL(sfkje, 0)
                 END
        ELSE NULL
    END AS wsajje,
    CASE
        WHEN fkfs IN (0, 4) AND wskje > 0 THEN wskje
        ELSE NULL
    END AS fqwfk
FROM base_data;
