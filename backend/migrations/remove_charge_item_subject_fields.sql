-- 迁移脚本：移除收费项目表中的科目绑定字段
-- 执行前请务必备份数据库！
-- 执行时间：2026-04-03

-- 检查字段是否存在（SQL Server 语法）
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('charge_items') AND name = 'current_account_subject_id')
BEGIN
    PRINT 'Dropping column: current_account_subject_id';
    ALTER TABLE charge_items DROP COLUMN current_account_subject_id;
END
ELSE
BEGIN
    PRINT 'Column current_account_subject_id does not exist';
END

-- 检查字段是否存在（SQL Server 语法）
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('charge_items') AND name = 'profit_loss_subject_id')
BEGIN
    PRINT 'Dropping column: profit_loss_subject_id';
    ALTER TABLE charge_items DROP COLUMN profit_loss_subject_id;
END
ELSE
BEGIN
    PRINT 'Column profit_loss_subject_id does not exist';
END

-- MySQL/MariaDB 版本（如果使用 MySQL，请取消下面的注释）
-- ALTER TABLE charge_items DROP COLUMN IF EXISTS current_account_subject_id;
-- ALTER TABLE charge_items DROP COLUMN IF EXISTS profit_loss_subject_id;

-- 验证迁移结果
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'charge_items'
  AND COLUMN_NAME IN ('current_account_subject_id', 'profit_loss_subject_id', 'kingdee_tax_rate_id')
ORDER BY COLUMN_NAME;

-- 完成提示
PRINT 'Migration completed successfully!';
