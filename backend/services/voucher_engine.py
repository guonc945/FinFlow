"""
凭证生成引擎，负责解析模板表达式并生成金蝶凭证数据。

核心能力：
1. 解析模板中的 `{variable}` 表达式
2. 自动补齐金蝶关联字段
3. 构建符合金蝶 OpenAPI 结构的凭证数据
"""

import re
import json
import logging
from typing import Dict, Any, Optional, List
from sqlalchemy.orm import Session

import sys
import os
# 纭繚鍙互瀵煎叆椤圭洰鏍圭洰褰曠殑妯″潡
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import (
    House, Park, Resident, ProjectList,
    KingdeeHouse, Customer, AuxiliaryData,
    Bill, KingdeeBankAccount
)
from utils.expression_functions import evaluate_expression as evaluate_template_expression

logger = logging.getLogger(__name__)


def _normalize_id(val: Any) -> str:
    if val is None:
        return ""
    if isinstance(val, int):
        return str(val)
    if isinstance(val, float):
        if val == int(val):
            return str(int(val))
        return str(val)
    text = str(val).strip()
    if re.fullmatch(r"\d+\.0+", text):
        return text.split(".")[0]
    return text


# 金蝶衍生字段定义
# 定义从账单数据到金蝶档案的取值链路
KD_DERIVED_FIELDS = {
    # 鎴垮彿 鈫?閲戣澏鎴垮彿 (閫氳繃 houses 琛ㄧ殑 kingdee_house_id 澶栭敭)
    'kd_house_number': {
        'source_field': 'house_id',       # 账单中的源字段
        'archive_model': 'house',          # 椹厠妗ｆ绫诲瀷
        'target_prop': 'wtw8_number',      # 使用金蝶房号映射编码
    },
    'kd_house_name': {
        'source_field': 'house_id',
        'archive_model': 'house',
        'target_prop': 'name',
    },
    # 杞︿綅 鈫?閲戣澏鎴垮彿 (閫氳繃 parks 琛ㄧ殑 kingdee_house_id 澶栭敭)
    'kd_park_house_number': {
        'source_field': 'park_id',
        'archive_model': 'park',
        'target_prop': 'wtw8_number',
    },
    'kd_park_house_name': {
        'source_field': 'park_id',
        'archive_model': 'park',
        'target_prop': 'name',
    },
    # 浣忔埛 鈫?閲戣澏瀹㈡埛 (閫氳繃 residents 琛ㄧ殑 kingdee_customer_id 澶栭敭)
    'kd_customer_number': {
        'source_field': 'user_list',       # 账单中的住户信息
        'archive_model': 'resident',
        'target_prop': 'number',
    },
    'kd_customer_name': {
        'source_field': 'user_list',
        'archive_model': 'resident',
        'target_prop': 'name',
    },
    # 鍥尯 鈫?閲戣澏椤圭洰 (閫氳繃 projects_lists 琛ㄧ殑 kingdee_project_id 澶栭敭)
    'kd_project_number': {
        'source_field': 'community_id',
        'archive_model': 'project',
        'target_prop': 'number',
    },
    'kd_project_name': {
        'source_field': 'community_id',
        'archive_model': 'project',
        'target_prop': 'name',
    },
    # 银行账户（通过园区默认配置自动解析）
    'kd_receive_bank_number': {
        'source_field': 'community_id',
        'archive_model': 'bank_receive',
        'target_prop': 'bankaccountnumber',
    },
    'kd_receive_bank_name': {
        'source_field': 'community_id',
        'archive_model': 'bank_receive',
        'target_prop': 'name',
    },
    'kd_pay_bank_number': {
        'source_field': 'community_id',
        'archive_model': 'bank_pay',
        'target_prop': 'bankaccountnumber',
    },
    'kd_pay_bank_name': {
        'source_field': 'community_id',
        'archive_model': 'bank_pay',
        'target_prop': 'name',
    },
}


def resolve_kd_derived_field(
    field_name: str,
    bill_data: Dict[str, Any],
    db: Session
) -> str:
    """
    解析金蝶衍生字段，并沿外键链路取到对应的金蝶档案值。

    Args:
        field_name: 衍生字段名，例如 `kd_house_number`
        bill_data: 当前账单行数据
        db: 数据库会话

    Returns:
        str: 对应字段的值，未找到时返回空字符串
    """
    config = KD_DERIVED_FIELDS.get(field_name)
    if not config:
        return ''

    source_value = bill_data.get(config['source_field'])
    if source_value is None or source_value == '':
        # 鑻ラ渶瑕?fallback (姣斿 community_id 涓簡浣嗚兘鏌ュ埌 house_id)
        # 对 community_id 不能直接返回空字符串，这里先置为 '0' 以触发后续回溯逻辑
        if config['source_field'] == 'community_id':
            source_value = '0'
        else:
            return ''

    archive_model = config['archive_model']
    target_prop = config['target_prop']
    bill_community_id = _normalize_id(bill_data.get('community_id')) if bill_data.get('community_id') not in (None, '') else None

    try:
        if archive_model == 'house':
            # 璐﹀崟 house_id 鈫?houses 琛?鈫?kingdee_house_id 鈫?kd_houses 琛?
            house_query = db.query(House).filter(
                House.house_id == _normalize_id(source_value)
            )
            if bill_community_id:
                house_query = house_query.filter(House.community_id == bill_community_id)
            house = house_query.first()
            if house and house.kingdee_house:
                return getattr(house.kingdee_house, target_prop, '') or ''

        elif archive_model == 'park':
            # 璐﹀崟 park_id 鈫?parks 琛?鈫?kingdee_house_id 鈫?kd_houses 琛?
            park_query = db.query(Park).filter(
                Park.park_id == _normalize_id(source_value)
            )
            if bill_community_id:
                park_query = park_query.filter(Park.community_id == bill_community_id)
            park = park_query.first()
            if park and park.kingdee_house:
                return getattr(park.kingdee_house, target_prop, '') or ''

        elif archive_model == 'resident':
            # 浠庤处鍗曠殑 user_list 涓彁鍙栦綇鎴凤紝鐒跺悗鏌ユ壘 residents 鈫?kingdee_customer
            resident = _resolve_resident_from_bill(bill_data, db)
            if resident and resident.kingdee_customer:
                return getattr(resident.kingdee_customer, target_prop, '') or ''

        elif archive_model == 'project':
            # 璐﹀崟 community_id 鈫?projects_lists 琛?鈫?kingdee_project_id 鈫?auxiliary_data
            normalized_id = _normalize_id(source_value)
            
            # 鍥炴函淇: 濡傛灉璐﹀崟鍘嗗彶鏁版嵁閬楀け浜?community_id锛堜负0锛夛紝灏濊瘯閫氳繃鍏宠仈瀹炰綋鎵惧洖
            try:
                if int(normalized_id) == 0:
                    h_id = bill_data.get('house_id')
                    p_id = bill_data.get('park_id')
                    
                    if h_id:
                        house_query = db.query(House).filter(House.house_id == _normalize_id(h_id))
                        if bill_community_id:
                            house_query = house_query.filter(House.community_id == bill_community_id)
                        house = house_query.first()
                        if house and house.community_id:
                            normalized_id = str(house.community_id)
                    elif p_id:
                        park_query = db.query(Park).filter(Park.park_id == _normalize_id(p_id))
                        if bill_community_id:
                            park_query = park_query.filter(Park.community_id == bill_community_id)
                        park = park_query.first()
                        if park and park.community_id:
                            normalized_id = str(park.community_id)
            except ValueError:
                pass
            
            project = db.query(ProjectList).filter(
                ProjectList.proj_id == int(normalized_id)
            ).first()
            if project and project.kingdee_project:
                return getattr(project.kingdee_project, target_prop, '') or ''

        elif archive_model in ('bank_receive', 'bank_pay'):
            direction = 'receive' if archive_model == 'bank_receive' else 'pay'
            normalized_id = _normalize_id(source_value)
            normalized_id = _fallback_community_id(normalized_id, bill_data, bill_community_id, db)
            try:
                if int(normalized_id) > 0:
                    bank = resolve_bank_account(int(normalized_id), direction, db)
                    if bank:
                        return getattr(bank, target_prop, '') or ''
            except ValueError:
                pass
            
            # 还是找不到的话，回退全局查找
            bank = resolve_bank_account(0, direction, db)
            if bank:
                return getattr(bank, target_prop, '') or ''

    except Exception as e:
        logger.warning(f"解析金蝶衍生字段 '{field_name}' 失败: {e}")

    return ''


def _resolve_resident_from_bill(bill_data: Dict[str, Any], db: Session) -> Optional[Resident]:
    """从账单数据中解析关联住户记录。"""
    bill_community_id = _normalize_id(bill_data.get('community_id')) if bill_data.get('community_id') not in (None, '') else None

    # 灏濊瘯浠?user_list 瀛楁瑙ｆ瀽
    user_list = bill_data.get('user_list')
    if user_list:
        try:
            if isinstance(user_list, str):
                users = json.loads(user_list)
            else:
                users = user_list
            if isinstance(users, list) and len(users) > 0:
                first_resident = None
                for user in users:
                    user_id = user.get('id') or user.get('user_id')
                    if user_id:
                        resident_query = db.query(Resident).filter(Resident.resident_id == _normalize_id(user_id))
                        if bill_community_id:
                            resident_query = resident_query.filter(Resident.community_id == bill_community_id)
                        resident = resident_query.first()
                        if resident:
                            if not first_resident:
                                first_resident = resident
                            if resident.kingdee_customer:
                                return resident
                if first_resident:
                    return first_resident
        except (json.JSONDecodeError, TypeError):
            pass

    # 鍥為€€: 灏濊瘯鐢?community_id + 鍏朵粬绾跨储鎵句綇鎴?
    return None


def _fallback_community_id(
    normalized_id: str,
    bill_data: Dict[str, Any],
    bill_community_id: Optional[str],
    db: Session
) -> str:
    try:
        if int(normalized_id) == 0:
            h_id = bill_data.get('house_id')
            p_id = bill_data.get('park_id')
            if h_id:
                house_query = db.query(House).filter(House.house_id == _normalize_id(h_id))
                if bill_community_id:
                    house_query = house_query.filter(House.community_id == bill_community_id)
                house = house_query.first()
                if house and house.community_id:
                    return str(house.community_id)
            elif p_id:
                park_query = db.query(Park).filter(Park.park_id == _normalize_id(p_id))
                if bill_community_id:
                    park_query = park_query.filter(Park.community_id == bill_community_id)
                park = park_query.first()
                if park and park.community_id:
                    return str(park.community_id)
    except ValueError:
        pass
    return normalized_id


def resolve_bank_account(
    community_id: int,
    direction: str,
    db: Session
) -> Optional[KingdeeBankAccount]:
    # 1. 园区默认设置
    if community_id > 0:
        try:
            project = db.query(ProjectList).filter(
                ProjectList.proj_id == community_id
            ).first()
            if project:
                if direction == 'receive' and project.default_receive_bank_id:
                    bank = db.query(KingdeeBankAccount).filter(
                        KingdeeBankAccount.id == project.default_receive_bank_id
                    ).first()
                    if bank:
                        return bank
                if direction == 'pay' and project.default_pay_bank_id:
                    bank = db.query(KingdeeBankAccount).filter(
                        KingdeeBankAccount.id == project.default_pay_bank_id
                    ).first()
                    if bank:
                        return bank
        except Exception as e:
            logger.warning(f"查找园区银行账户失败: {e}")

    # 2. 全局默认设置
    try:
        if direction == 'receive':
            return db.query(KingdeeBankAccount).filter(
                KingdeeBankAccount.isdefaultrec == True
            ).first()
        else:
            return db.query(KingdeeBankAccount).filter(
                KingdeeBankAccount.isdefaultpay == True
            ).first()
    except Exception as e:
        logger.warning(f"查找全局银行账户失败: {e}")

    return None


def enrich_bill_data(bill_data: Dict[str, Any], db: Session) -> Dict[str, Any]:
    """
    为原始账单数据补齐所有金蝶衍生字段。

    在生成凭证前调用此方法，将普通账单数据扩展为
    包含金蝶关联字段的完整数据集。

    Args:
        bill_data: 原始账单字段字典
        db: 数据库会话

    Returns:
        Dict: 扩展后的账单数据，包含 `kd_*` 字段
    """
    enriched = dict(bill_data)

    for field_name in KD_DERIVED_FIELDS:
        if field_name not in enriched:
            enriched[field_name] = resolve_kd_derived_field(field_name, bill_data, db)

    # Inject chosen customer for template matching
    user_list = bill_data.get('user_list')
    chosen_user = None
    if user_list:
        import json
        try:
            users = json.loads(user_list) if isinstance(user_list, str) else user_list
            if isinstance(users, list) and len(users) > 0:
                chosen_user = users[0]
        except:
            pass
            
    if chosen_user:
        enriched['customer_name'] = chosen_user.get('name', '')
        enriched['customer_id'] = str(chosen_user.get('id') or chosen_user.get('user_id', ''))
    else:
        enriched['customer_name'] = ''
        enriched['customer_id'] = ''

    # Add source-bound aliases so templates can reference fields with an explicit source binding:
    # - {bills.amount}
    # - {marki.bills.amount} (module + source + field)
    # while keeping backward compatibility with legacy {amount}.
    _prefix = "bills"
    _module_prefix = "marki.bills"
    for key, val in list(enriched.items()):
        # Avoid re-prefixing keys that are already qualified.
        if not isinstance(key, str) or "." in key:
            continue
        enriched[f"{_prefix}.{key}"] = val
        enriched[f"{_module_prefix}.{key}"] = val

    return enriched


def evaluate_expression(expr: str, data: Dict[str, Any]) -> str:
    return evaluate_template_expression(expr, data)


def build_voucher_entries(
    template_rules: list,
    enriched_data: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    根据模板分录规则和扩展后的账单数据生成凭证分录。

    Args:
        template_rules: 模板中的分录规则列表
        enriched_data: 扩展后的账单数据

    Returns:
        List[Dict]: 金蝶凭证分录数据列表
    """
    entries = []

    for rule in template_rules:
        entry = {
            'line_no': rule.line_no,
            'dr_cr': rule.dr_cr,
            'account_code': evaluate_expression(rule.account_code, enriched_data),
            'amount': evaluate_expression(rule.amount_expr, enriched_data),
            'summary': evaluate_expression(rule.summary_expr, enriched_data),
            'currency': evaluate_expression(rule.currency_expr, enriched_data),
            'localrate': evaluate_expression(rule.localrate_expr, enriched_data),
        }

        # 瑙ｆ瀽杈呭姪鏍哥畻 (aux_items / assgrp)
        if rule.aux_items:
            entry['aux_items'] = _resolve_dimension_json(rule.aux_items, enriched_data)

        # 瑙ｆ瀽涓昏〃鏍哥畻 (main_cf_assgrp / maincfassgrp)
        if rule.main_cf_assgrp:
            entry['main_cf_assgrp'] = _resolve_dimension_json(rule.main_cf_assgrp, enriched_data)

        entries.append(entry)

    return entries


def _resolve_dimension_json(
    dim_json_str: str,
    data: Dict[str, Any]
) -> Dict[str, Any]:
    """
    解析辅助核算或主表核算的 JSON 配置。

    JSON 格式:
    { "客户": { "number": "{kd_customer_number}" } }

    解析后:
    { "客户": { "number": "C001" } }
    """
    try:
        dim_obj = json.loads(dim_json_str)
        resolved = {}
        for dim_key, dim_config in dim_obj.items():
            resolved[dim_key] = {}
            for prop, expr in dim_config.items():
                resolved[dim_key][prop] = evaluate_expression(str(expr), data)
        return resolved
    except (json.JSONDecodeError, TypeError) as e:
        logger.warning(f"解析辅助核算 JSON 失败: {e}")
        return {}


def batch_preload_kd_cache(
    bills_data: List[Dict[str, Any]],
    db: Session,
) -> Dict[str, Any]:
    """
    批量预加载所有 bill 需要的金蝶关联数据，避免 N+1 查询。

    返回缓存字典，包含 houses/parks/residents/projects/banks_receive/banks_pay。
    """
    from sqlalchemy.orm import joinedload as _jl

    cache: Dict[str, Any] = {
        "houses": {},
        "parks": {},
        "residents": {},
        "projects": {},
        "banks_receive": {},
        "banks_pay": {},
    }

    # 收集所有需要查询的 ID
    house_ids = set()
    park_ids = set()
    community_ids = set()
    resident_ids = set()

    for bd in bills_data:
        h_id = _normalize_id(bd.get("house_id"))
        if h_id:
            house_ids.add(h_id)
        p_id = _normalize_id(bd.get("park_id"))
        if p_id:
            park_ids.add(p_id)
        c_id = bd.get("community_id")
        if c_id not in (None, "", 0, "0"):
            try:
                community_ids.add(int(c_id))
            except (ValueError, TypeError):
                pass
        # 从 user_list 提取 resident_id
        user_list = bd.get("user_list")
        if user_list:
            try:
                users = json.loads(user_list) if isinstance(user_list, str) else user_list
                if isinstance(users, list):
                    for user in users:
                        uid = user.get("id") or user.get("user_id")
                        if uid:
                            resident_ids.add(_normalize_id(uid))
            except Exception:
                pass

    # 批量查询 House（含 kingdee_house 关联）
    if house_ids:
        rows = (
            db.query(House)
            .options(_jl(House.kingdee_house))
            .filter(House.house_id.in_(list(house_ids)))
            .all()
        )
        for row in rows:
            cache["houses"][str(row.house_id)] = row

    # 批量查询 Park（含 kingdee_house 关联）
    if park_ids:
        rows = (
            db.query(Park)
            .options(_jl(Park.kingdee_house))
            .filter(Park.park_id.in_(list(park_ids)))
            .all()
        )
        for row in rows:
            cache["parks"][str(row.park_id)] = row

    # 批量查询 Resident（含 kingdee_customer 关联）
    if resident_ids:
        rows = (
            db.query(Resident)
            .options(_jl(Resident.kingdee_customer))
            .filter(Resident.resident_id.in_(list(resident_ids)))
            .all()
        )
        for row in rows:
            cache["residents"][str(row.resident_id)] = row

    # 批量查询 ProjectList（含 kingdee_project 关联）
    if community_ids:
        rows = (
            db.query(ProjectList)
            .options(_jl(ProjectList.kingdee_project))
            .filter(ProjectList.proj_id.in_(list(community_ids)))
            .all()
        )
        for row in rows:
            cache["projects"][int(row.proj_id)] = row

    # 批量查询银行账户（按园区维度）
    if community_ids:
        bank_ids_to_query = set()
        for proj_id, project in cache["projects"].items():
            if project.default_receive_bank_id:
                bank_ids_to_query.add(int(project.default_receive_bank_id))
            if project.default_pay_bank_id:
                bank_ids_to_query.add(int(project.default_pay_bank_id))
        if bank_ids_to_query:
            bank_rows = db.query(KingdeeBankAccount).filter(
                KingdeeBankAccount.id.in_(list(bank_ids_to_query))
            ).all()
            bank_by_id = {int(b.id): b for b in bank_rows}
            for proj_id, project in cache["projects"].items():
                if project.default_receive_bank_id:
                    bank = bank_by_id.get(int(project.default_receive_bank_id))
                    if bank:
                        cache["banks_receive"][proj_id] = bank
                if project.default_pay_bank_id:
                    bank = bank_by_id.get(int(project.default_pay_bank_id))
                    if bank:
                        cache["banks_pay"][proj_id] = bank

    # 全局默认银行账户
    default_rec = db.query(KingdeeBankAccount).filter(
        KingdeeBankAccount.isdefaultrec == True
    ).first()
    if default_rec:
        cache["banks_receive"]["_default"] = default_rec
    default_pay = db.query(KingdeeBankAccount).filter(
        KingdeeBankAccount.isdefaultpay == True
    ).first()
    if default_pay:
        cache["banks_pay"]["_default"] = default_pay

    return cache


def _resolve_kd_field_cached(
    field_name: str,
    bill_data: Dict[str, Any],
    kd_cache: Dict[str, Any],
) -> str:
    """使用预加载缓存解析金蝶衍生字段，无额外 DB 查询。"""
    config = KD_DERIVED_FIELDS.get(field_name)
    if not config:
        return ""

    source_value = bill_data.get(config["source_field"])
    archive_model = config["archive_model"]
    target_prop = config["target_prop"]
    bill_community_id = bill_data.get("community_id")

    try:
        if archive_model == "house":
            nid = _normalize_id(source_value)
            house = kd_cache.get("houses", {}).get(nid)
            if house and house.kingdee_house:
                return getattr(house.kingdee_house, target_prop, "") or ""

        elif archive_model == "park":
            nid = _normalize_id(source_value)
            park = kd_cache.get("parks", {}).get(nid)
            if park and park.kingdee_house:
                return getattr(park.kingdee_house, target_prop, "") or ""

        elif archive_model == "resident":
            user_list = bill_data.get("user_list")
            if user_list:
                try:
                    users = json.loads(user_list) if isinstance(user_list, str) else user_list
                    if isinstance(users, list):
                        first_resident = None
                        for user in users:
                            uid = user.get("id") or user.get("user_id")
                            if uid:
                                resident = kd_cache.get("residents", {}).get(_normalize_id(uid))
                                if resident:
                                    if not first_resident:
                                        first_resident = resident
                                    if resident.kingdee_customer:
                                        return getattr(resident.kingdee_customer, target_prop, "") or ""
                        if first_resident and first_resident.kingdee_customer:
                            return getattr(first_resident.kingdee_customer, target_prop, "") or ""
                except Exception:
                    pass

        elif archive_model == "project":
            c_id = bill_community_id
            try:
                c_id_int = int(c_id) if c_id not in (None, "") else 0
            except (ValueError, TypeError):
                c_id_int = 0
            if c_id_int == 0:
                h_id = _normalize_id(bill_data.get("house_id"))
                if h_id:
                    house = kd_cache.get("houses", {}).get(h_id)
                    if house and house.community_id:
                        c_id_int = int(house.community_id)
                if c_id_int == 0:
                    p_id = _normalize_id(bill_data.get("park_id"))
                    if p_id:
                        park = kd_cache.get("parks", {}).get(p_id)
                        if park and park.community_id:
                            c_id_int = int(park.community_id)
            project = kd_cache.get("projects", {}).get(c_id_int)
            if project and project.kingdee_project:
                return getattr(project.kingdee_project, target_prop, "") or ""

        elif archive_model in ("bank_receive", "bank_pay"):
            bank_key = "banks_receive" if archive_model == "bank_receive" else "banks_pay"
            c_id = bill_community_id
            try:
                c_id_int = int(c_id) if c_id not in (None, "") else 0
            except (ValueError, TypeError):
                c_id_int = 0
            bank = kd_cache.get(bank_key, {}).get(c_id_int)
            if not bank:
                bank = kd_cache.get(bank_key, {}).get("_default")
            if bank:
                return getattr(bank, target_prop, "") or ""

    except Exception as e:
        logger.warning(f"缓存解析金蝶衍生字段 '{field_name}' 失败: {e}")

    return ""


def enrich_bill_data_cached(
    bill_data: Dict[str, Any],
    kd_cache: Dict[str, Any],
) -> Dict[str, Any]:
    """使用预加载缓存的 enrich_bill_data，无额外 DB 查询。"""
    enriched = dict(bill_data)

    for field_name in KD_DERIVED_FIELDS:
        if field_name not in enriched:
            enriched[field_name] = _resolve_kd_field_cached(field_name, bill_data, kd_cache)

    # 注入客户信息
    user_list = bill_data.get("user_list")
    chosen_user = None
    if user_list:
        try:
            users = json.loads(user_list) if isinstance(user_list, str) else user_list
            if isinstance(users, list) and len(users) > 0:
                chosen_user = users[0]
        except Exception:
            pass

    if chosen_user:
        enriched["customer_name"] = chosen_user.get("name", "")
        enriched["customer_id"] = str(chosen_user.get("id") or chosen_user.get("user_id", ""))
    else:
        enriched["customer_name"] = ""
        enriched["customer_id"] = ""

    # 添加带前缀的字段别名
    _prefix = "bills"
    _module_prefix = "marki.bills"
    for key, val in list(enriched.items()):
        if not isinstance(key, str) or "." in key:
            continue
        enriched[f"{_prefix}.{key}"] = val
        enriched[f"{_module_prefix}.{key}"] = val

    return enriched
