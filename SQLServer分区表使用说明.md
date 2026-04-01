# SQL Server 分区表使用说明

## 概述

FinFlow 已从 PostgreSQL 迁移到 SQL Server 2016 标准版，账单表使用分区表进行优化。

## 分区策略

- **分区键**: `community_id` (园区ID)
- **分区方式**: RANGE RIGHT
- **分区函数**: `pf_bills_community_id`
- **分区方案**: `ps_bills_community_id`

## 分区表列表

| 分区表名 | 园区ID | 园区名称 |
|---------|--------|---------|
| bills_proj_sbwlsl | 1 | 商博物流双流 |
| bills_proj_cmysg | 2 | 成美·誉尚国 |
| bills_proj_ztwlsl | 3 | 中通物流双流 |
| bills_proj_ztpgxz | 4 | 中通·蓬葛·新鑫 |
| bills_proj_cxthgc | 5 | 诚信通·惠公寓 |
| bills_proj_cxyhyx | 6 | 诚信银行营销中心 |
| bills_proj_cxwlsl | 7 | 诚信物流双流 |
| bills_proj_mzgz | 8 | 茂臻高值 |
| bills_proj_default | 其他 | 默认分区 |

## 分区表文件

### SQL 脚本
- **文件**: `backend/sql/setup_bills_partitions_sqlserver.sql`
- **用途**: 创建分区函数、分区方案和所有分区表
- **执行方式**: 使用 sqlcmd 或 SQL Server Management Studio

### Python 脚本
- **安装脚本**: `backend/scripts/setup_partitions_sqlserver.py`
- **验证脚本**: `backend/scripts/verify_partitions_sqlserver.py`

## 使用方法

### 1. 安装分区表

```bash
# 使用 Python 脚本安装
cd D:\FinFlow
py -3.10 backend\scripts\setup_partitions_sqlserver.py
```

或者使用 SQL Server Management Studio 执行:
```
backend\sql\setup_bills_partitions_sqlserver.sql
```

### 2. 验证分区表

```bash
# 使用 Python 脚本验证
cd D:\FinFlow
py -3.10 backend\scripts\verify_partitions_sqlserver.py
```

### 3. 查看分区信息

```sql
-- 查看所有分区表
SELECT 
    t.name AS table_name,
    p.partition_number,
    p.rows AS row_count
FROM sys.tables t
INNER JOIN sys.partitions p ON t.object_id = p.object_id
WHERE t.name LIKE 'bills_proj_%'
ORDER BY t.name, p.partition_number;

-- 查看分区函数
SELECT 
    pf.name AS partition_function,
    rv.value AS boundary_value
FROM sys.partition_functions pf
LEFT JOIN sys.partition_range_values rv ON pf.function_id = rv.function_id
WHERE pf.name = 'pf_bills_community_id';
```

## 数据库配置

数据库配置文件位于 `database.txt`:

```
数据库类型：sqlserver
服务器：fn.hyqy.group
端口：1433
账号：sa
密码：sa@sqlserver
```

## 注意事项

1. **备份**: 分区表创建前建议先备份数据库
2. **权限**: 确保数据库用户有创建分区函数和方案的权限
3. **索引**: 分区表已创建必要的索引以优化查询性能
4. **维护**: 定期检查分区表的行数分布，确保数据均衡

## 迁移说明

如果从 PostgreSQL 迁移过来:

1. 先执行 PostgreSQL 分区表脚本 `backend/sql/setup_bills_partitions.sql`
2. 导出数据
3. 在 SQL Server 中创建分区表
4. 导入数据到对应分区表

## 常见问题

### Q: 如何查询特定园区的数据?
```sql
-- 查询商博物流双流的数据
SELECT * FROM bills_proj_sbwlsl WHERE community_id = 1;

-- 或者直接查询主表
SELECT * FROM bills WHERE community_id = 1;
```

### Q: 如何添加新的园区?
1. 在 `community_mapping` 表中添加新园区记录
2. 创建新的分区表
3. 更新分区函数和方案 (需要重新创建)

### Q: 分区表和普通表性能对比?
分区表在以下场景性能更好:
- 大量数据按园区查询
- 历史数据归档
- 并行查询
