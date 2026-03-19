# salesBoard 视图索引与性能建议

## 目标

这份建议用于支撑 `dbo.salesBoard` 视图查询，不直接执行 DDL，先供 DBA 或实施人员评估。

## 优先级最高的索引方向

1. `dbo.uf_rgxytz`

```sql
CREATE NONCLUSTERED INDEX IX_uf_rgxytz_xyzt_fwbh
ON dbo.uf_rgxytz (xyzt, fwbh, id)
INCLUDE (skjezzhj, ytjehj, yf);
```

2. `dbo.uf_qysptz`

```sql
CREATE NONCLUSTERED INDEX IX_uf_qysptz_xyzt_rgxybm
ON dbo.uf_qysptz (xyzt, rgxybm, id)
INCLUDE (fwbm, khxm, qyzj, qydj, fkfs, sfkje, dkje);
```

3. `dbo.uf_skjltz`

```sql
CREATE NONCLUSTERED INDEX IX_uf_skjltz_rcxybh_szgs
ON dbo.uf_skjltz (rcxybh, szgs, id);
```

4. `dbo.uf_skjltz_dt1`

```sql
CREATE NONCLUSTERED INDEX IX_uf_skjltz_dt1_mainid
ON dbo.uf_skjltz_dt1 (mainid)
INCLUDE (bcjkje);
```

5. `dbo.uf_rgxytz_dt3`

```sql
CREATE NONCLUSTERED INDEX IX_uf_rgxytz_dt3_rcbh
ON dbo.uf_rgxytz_dt3 (rcbh);
```

6. `dbo.uf_tkjltz`

```sql
CREATE NONCLUSTERED INDEX IX_uf_tkjltz_rgxybm_gs
ON dbo.uf_tkjltz (rgxybm, gs)
INCLUDE (kckx, bcje, tkje);
```

7. `dbo.uf_fxxxb`

```sql
CREATE NONCLUSTERED INDEX IX_uf_fxxxb_id
ON dbo.uf_fxxxb (id)
INCLUDE (
    xmmc, zh, lc, dyh, fjh, yt, lx, ycjzmj, scjzmj, yctnmj, sctnmj,
    ycgtmj, gtmj, spdj, spzj, yhze, xszt, fwbh, dk, szfb, ks, jg, hx
);
```

## 设计说明

- `uf_rgxytz` 和 `uf_qysptz` 当前都用了 `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY id DESC)`，所以索引顺序要尽量贴近筛选列和分组键。
- `uf_skjltz` / `uf_skjltz_dt1` / `uf_rgxytz_dt3` 这组主要服务于“未转认筹收款”的 `JOIN + NOT EXISTS`。
- `uf_tkjltz` 主要服务于“未转认筹退款”的 `GROUP BY gs` 和 `rgxybm` 过滤。
- `uf_fxxxb` 如果 `id` 已经是聚集主键，就不需要重复建索引；这条仅作为“如果没有合适覆盖索引”的建议。

## 上线前先确认

- 是否已经存在同类索引，避免重复建设。
- `id` 是否为递增主键；如果不是，`ORDER BY id DESC` 仅表示“最大 id”，不一定等于“最新业务单据”。
- `fwbh`、`rgxybm`、`rcbh` 的数据类型和长度是否一致；如果存在隐式转换，索引命中会变差。

## 进一步优化方向

1. 如果这个销控总表查询频繁、数据量大，可以考虑拆成两层：
   - 明细视图：房源认购签约
   - 汇总视图：未转认筹公司汇总

2. 如果业务允许，后续可以把“最新认购/最新签约”提前落到中间表，避免每次视图实时跑窗口函数。

3. 如果数据库开启了 `READ_COMMITTED_SNAPSHOT`，可以在不使用 `NOLOCK` 的前提下获得更稳定的读取体验。
