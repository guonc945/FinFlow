from sqlalchemy import Column, String, Float, DateTime, Date, Text, Boolean, Integer, BigInteger, ForeignKey, CHAR, DECIMAL, ForeignKeyConstraint, Index, Table
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from database import Base

class ApprovalFormSnapshot(Base):
    __tablename__ = "approval_form_snapshot"

    flow_id = Column(String(50), primary_key=True, index=True)
    business_type = Column(String(50), nullable=False)
    applicant_id = Column(String(20))
    applicant_name = Column(String(50))
    department_code = Column(String(20))
    total_amount = Column(DECIMAL(18, 2), nullable=False)
    approved_at = Column(DateTime)
    form_data_raw = Column(Text)  # JSON string
    created_at = Column(DateTime, server_default=func.now())

    journal = relationship("CashJournal", back_populates="snapshot", uselist=False)

class CashJournal(Base):
    __tablename__ = "cash_journal"

    id = Column(Integer, primary_key=True, index=True)
    flow_id = Column(String(50), ForeignKey("approval_form_snapshot.flow_id"), nullable=False, unique=True)
    amount = Column(DECIMAL(18, 2), nullable=False)
    direction = Column(CHAR(1))  # 'I' or 'O'
    status = Column(String(20), default="pending")
    voucher_id = Column(String(50))
    error_msg = Column(String(500))
    pushed_at = Column(DateTime)
    created_at = Column(DateTime, server_default=func.now())

    snapshot = relationship("ApprovalFormSnapshot", back_populates="journal")

class VoucherTemplate(Base):
    __tablename__ = "voucher_template"

    template_id = Column(String(50), primary_key=True)
    template_name = Column(String(100), nullable=False)
    business_type = Column(String(50), nullable=False)
    description = Column(String(255))
    active = Column(Boolean, default=True)
    priority = Column(Integer, default=100)
    category_id = Column(Integer, ForeignKey("voucher_template_categories.id"), nullable=True)
    # Business module binding (e.g. 'marki'). A module can contain multiple sources/models.
    source_module = Column(String(50))
    source_type = Column(String(50))  # e.g. 'bills'
    trigger_condition = Column(Text)  # JSON string for logic rules
    
    # Kingdee specific mapping expressions
    book_number_expr = Column(String(100), default="'BU-35256'")
    vouchertype_number_expr = Column(String(100), default="'0001'")
    attachment_expr = Column(String(100), default="0")
    bizdate_expr = Column(String(100), default="{CURRENT_DATE}")
    bookeddate_expr = Column(String(100), default="{CURRENT_DATE}")

    rules = relationship("VoucherEntryRule", back_populates="template")
    category = relationship("VoucherTemplateCategory", foreign_keys=[category_id])

class VoucherEntryRule(Base):
    __tablename__ = "voucher_entry_rule"

    rule_id = Column(Integer, primary_key=True, index=True)
    template_id = Column(String(50), ForeignKey("voucher_template.template_id"), nullable=False)
    line_no = Column(Integer, nullable=False)
    dr_cr = Column(CHAR(1))  # 'D' or 'C'
    account_code = Column(String(50), nullable=False)
    display_condition_expr = Column(Text, default="")
    amount_expr = Column(Text, nullable=False)
    summary_expr = Column(Text, nullable=False)
    currency_expr = Column(Text, default="'CNY'")
    localrate_expr = Column(Text, default="1")
    aux_items = Column(Text)  # JSON string mapping for Kingdee assgrp (Flex)
    main_cf_assgrp = Column(Text) # JSON string for maincfassgrp

    template = relationship("VoucherTemplate", back_populates="rules")

class ChargeItem(Base):
    __tablename__ = "charge_items"

    item_id = Column(Integer, primary_key=True, index=True)
    communityid = Column(String(50), nullable=False)
    item_name = Column(String(200), nullable=False)
    
    # 鏂板椹厠鑱旇繑鍥炵殑琛ュ厖瀛楁
    charge_type = Column(Integer, nullable=True)
    charge_type_str = Column(String(50), nullable=True)
    category_id = Column(Integer, nullable=True)
    category_name = Column(String(100), nullable=True)
    period_type_str = Column(String(200), nullable=True)
    remark = Column(Text, nullable=True)

    # 鏄犲皠浼氳绉戠洰
    current_account_subject_id = Column(String(50), ForeignKey("accounting_subjects.id"), nullable=True)
    profit_loss_subject_id = Column(String(50), ForeignKey("accounting_subjects.id"), nullable=True)

    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    # 鍏崇郴鏄犲皠
    current_account_subject = relationship("AccountingSubject", foreign_keys=[current_account_subject_id])
    profit_loss_subject = relationship("AccountingSubject", foreign_keys=[profit_loss_subject_id])

class ProjectList(Base):
    __tablename__ = "projects_lists"

    proj_id = Column(Integer, primary_key=True, index=True)
    proj_name = Column(String(200), nullable=False)
    kingdee_project_id = Column(String(50), ForeignKey("auxiliary_data.id"), nullable=True)
    # 榛樿鏀舵閾惰璐︽埛
    default_receive_bank_id = Column(String(50), ForeignKey("kd_bank_accounts.id"), nullable=True)
    # 榛樿浠樻閾惰璐︽埛
    default_pay_bank_id = Column(String(50), ForeignKey("kd_bank_accounts.id"), nullable=True)
    # 閲戣澏鏍哥畻璐︾翱
    kingdee_account_book_id = Column(String(50), ForeignKey("kd_account_books.id"), nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    kingdee_project = relationship("AuxiliaryData", foreign_keys=[kingdee_project_id])
    default_receive_bank = relationship("KingdeeBankAccount", foreign_keys=[default_receive_bank_id])
    default_pay_bank = relationship("KingdeeBankAccount", foreign_keys=[default_pay_bank_id])
    kingdee_account_book = relationship("KingdeeAccountBook", foreign_keys=[kingdee_account_book_id])

class House(Base):
    __tablename__ = "houses"

    id = Column(Integer, primary_key=True, index=True)
    house_id = Column(String(50), unique=True, nullable=False, index=True)
    community_id = Column(String(50), nullable=False, index=True)
    community_name = Column(String(255))
    house_name = Column(String(255), nullable=False)
    # Mark 绯荤粺瀛楁琛ラ綈锛堝弬鑰?backend/docs/1.json锛?
    building_id = Column(BigInteger, nullable=True, index=True)
    building_name = Column(String(255))
    unit_id = Column(BigInteger, nullable=True, index=True)
    unit_name = Column(String(255))
    layer = Column(Integer)
    building_size = Column(DECIMAL(10, 2))
    usable_size = Column(DECIMAL(10, 2))
    floor_name = Column(String(255))
    area = Column(DECIMAL(10, 2))
    user_num = Column(Integer)
    charge_num = Column(Integer)
    park_num = Column(Integer)
    car_num = Column(Integer)
    combina_name = Column(String(255))
    create_uid = Column(BigInteger)
    disable = Column(Boolean, default=False)
    expand = Column(Text)  # Mark 杩斿洖鐨?expand锛堥€氬父鏄?JSON 瀛楃涓诧級
    expand_info = Column(Text)  # Mark 杩斿洖鐨?ExpandInfo锛圝SON锛?
    tag_list = Column(Text)  # JSON
    attachment_list = Column(Text)  # JSON
    house_type_name = Column(String(100))
    house_status_name = Column(String(100))
    kingdee_house_id = Column(String(50), ForeignKey("kd_houses.id"), nullable=True)
    created_at = Column(DateTime, server_default=func.now())

    kingdee_house = relationship("KingdeeHouse", foreign_keys=[kingdee_house_id])
    user_list = relationship(
        "HouseUser",
        back_populates="house",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    parks = relationship(
        "Park",
        back_populates="house",
        cascade="all",
    )

    @property
    def park_list(self):
        # 鍏煎鍓嶇/鎺ュ彛瀛楁鍛藉悕锛坧ark_list锛夛紝瀹為檯鏁版嵁鏉ヨ嚜 parks 琛ㄧ殑澶栭敭鍏崇郴
        return self.parks


class HouseUser(Base):
    """House user binding records."""

    __tablename__ = "house_users"

    id = Column(Integer, primary_key=True, index=True)
    house_fk = Column(Integer, ForeignKey("houses.id", ondelete="CASCADE"), nullable=False, index=True)

    origin_id = Column(BigInteger)
    item_id = Column(BigInteger, nullable=False)
    name = Column(String(255))
    item_type = Column(Integer)
    licence = Column(String(100))
    park_name = Column(String(255))
    owner_name = Column(String(255))
    owner_phone = Column(String(50))
    charge_item_info = Column(Text)
    start_time = Column(BigInteger)
    end_time = Column(BigInteger)
    community_name = Column(String(255))
    natural_period = Column(BigInteger)
    period_type = Column(Integer)
    period_num = Column(Integer)
    created_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("udx_house_users_house_item", "house_fk", "item_id", unique=True),
        Index("ix_house_users_owner_phone", "owner_phone"),
    )

    house = relationship("House", back_populates="user_list", foreign_keys=[house_fk])



class Resident(Base):
    __tablename__ = "residents"

    id = Column(Integer, primary_key=True, index=True)
    resident_id = Column(String(50), nullable=False, index=True)
    community_id = Column(String(50), nullable=False, index=True)
    community_name = Column(String(255))
    name = Column(String(255), nullable=False)
    phone = Column(String(50))
    houses = Column(String)  # Store JSON as string or Text for Postgres
    labels = Column(String)
    kingdee_customer_id = Column(String(50), ForeignKey("customers.id"), nullable=True)
    created_at = Column(DateTime, server_default=func.now())

    kingdee_customer = relationship("Customer", foreign_keys=[kingdee_customer_id])

class Park(Base):
    __tablename__ = "parks"

    id = Column(Integer, primary_key=True, index=True)
    park_id = Column(String(50), nullable=False, index=True)
    community_id = Column(String(50), nullable=False, index=True)
    community_name = Column(String(255))
    name = Column(String(255), nullable=False)
    park_type_name = Column(String(50))
    state = Column(Integer)
    user_name = Column(String(255))
    house_name = Column(String(255))
    house_id = Column(String(50), nullable=True, index=True)  # Mark 鎴垮眿ID锛堜笌 houses.house_id 瀵瑰簲锛?
    house_fk = Column(Integer, ForeignKey("houses.id"), nullable=True, index=True)
    kingdee_house_id = Column(String(50), ForeignKey("kd_houses.id"), nullable=True)
    created_at = Column(DateTime, server_default=func.now())

    kingdee_house = relationship("KingdeeHouse", foreign_keys=[kingdee_house_id])
    house = relationship("House", back_populates="parks", foreign_keys=[house_fk])

class CommunityMapping(Base):
    """Community mapping configuration."""
    __tablename__ = "community_mapping"

    id = Column(Integer, primary_key=True, index=True)
    community_id = Column(Integer, unique=True, nullable=False)
    community_name = Column(String(100), nullable=False)
    partition_suffix = Column(String(20), nullable=False)
    description = Column(String(500))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class Bill(Base):
    """璐﹀崟琛紙鎸夊洯鍖哄垎鍖猴級"""
    __tablename__ = "bills"

    # 澶嶅悎涓婚敭锛歩d + community_id锛堝垎鍖洪敭锛?
    id = Column(BigInteger, primary_key=True)
    community_id = Column(BigInteger, primary_key=True, nullable=False)
    
    # 鏀惰垂椤圭洰淇℃伅
    charge_item_id = Column(Integer)
    ci_snapshot_id = Column(Integer)
    charge_item_name = Column(String(200))
    charge_item_type = Column(Integer)
    category_name = Column(String(100))
    
    # 璧勪骇淇℃伅
    asset_id = Column(Integer)
    asset_name = Column(String(100))
    asset_type = Column(Integer)
    asset_type_str = Column(String(50))
    
    # 鎴垮眿淇℃伅
    house_id = Column(Integer)
    full_house_name = Column(String(200))
    bind_house_id = Column(Integer)
    bind_house_name = Column(String(200))
    
    # 杞︿綅淇℃伅
    park_id = Column(Integer)
    park_name = Column(String(100))
    
    # 璐﹀崟鏃堕棿
    bill_month = Column(Date)
    in_month = Column(String(10))
    start_time = Column(BigInteger)  # Unix timestamp
    end_time = Column(BigInteger)    # Unix timestamp
    
    # 閲戦淇℃伅锛堝崟浣嶏細鍏冿級
    amount = Column(DECIMAL(12, 2))
    bill_amount = Column(DECIMAL(12, 2))
    discount_amount = Column(DECIMAL(12, 2), default=0)
    late_money_amount = Column(DECIMAL(12, 2), default=0)
    deposit_amount = Column(DECIMAL(12, 2), default=0)
    second_pay_amount = Column(DECIMAL(12, 2), default=0)
    
    # 鏀粯淇℃伅
    pay_status = Column(Integer)
    pay_status_str = Column(String(20))
    pay_type = Column(Integer)
    pay_type_str = Column(String(50))
    pay_time = Column(BigInteger)  # Unix timestamp
    receive_date = Column(DateTime)  # Derived from pay_time
    second_pay_channel = Column(Integer)
    
    # 璐﹀崟绫诲瀷
    bill_type = Column(Integer)
    bill_type_str = Column(String(50))
    
    # 涓氬姟寮曠敤
    deal_log_id = Column(BigInteger)
    receipt_id = Column(String(50))
    sub_mch_id = Column(String(50))
    sub_mch_name = Column(String(100))
    
    # 鍧忚处鍜屾媶鍒?
    bad_bill_state = Column(Integer, default=0)
    is_bad_bill = Column(Boolean, default=False)
    has_split = Column(Boolean, default=False)
    split_desc = Column(Text)
    
    # 鍙鎬?
    visible_type = Column(Integer, default=0)
    visible_desc_str = Column(String(50))
    
    # 鍏朵粬
    can_revoke = Column(Integer, default=0)
    version = Column(Integer, default=1)
    meter_type = Column(Integer, default=0)
    snapshot_size = Column(String(50))
    now_size = Column(String(50))
    remark = Column(Text)
    
    # JSONB瀛楁锛堝瓨鍌ㄥ祵濂楁暟鎹級
    bind_toll = Column(Text)  # JSON string - 鏀惰垂椤圭洰蹇収
    user_list = Column(Text)  # JSON string - 鍘熷瀹㈡埛鍒楄〃澶囦唤
    
    # 鏃堕棿鎴?
    create_time = Column(BigInteger)  # Unix timestamp
    last_op_time = Column(DateTime)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    # 鍏宠仈浠庤〃
    users = relationship("BillUser", back_populates="bill", cascade="all, delete-orphan",
                         foreign_keys="[BillUser.bill_id, BillUser.community_id]")


class BillUser(Base):
    """璐﹀崟鍏宠仈瀹㈡埛/浣忔埛锛堜粠琛級"""
    __tablename__ = "bill_users"

    id = Column(Integer, primary_key=True, index=True)
    bill_id = Column(BigInteger, nullable=False, index=True)
    community_id = Column(BigInteger, nullable=False)
    user_id = Column(Integer)           # 椹厠绯荤粺鐢ㄦ埛 ID
    user_name = Column(String(255))     # 瀹㈡埛鍚嶇О锛堝惈鎵嬫満鍙凤級
    is_system = Column(Integer, default=0)
    created_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        ForeignKeyConstraint(
            ['bill_id', 'community_id'],
            ['bills.id', 'bills.community_id'],
            ondelete='CASCADE'
        ),
        Index('ix_bill_users_bill_community', 'bill_id', 'community_id'),
        Index('ix_bill_users_user_name', 'user_name'),
    )

    bill = relationship("Bill", back_populates="users",
                        foreign_keys=[bill_id, community_id])


class ReceiptBill(Base):
    """鏀舵鏄庣粏锛堟敹娆捐处鍗曪級"""

    __tablename__ = "receipt_bills"

    # Composite primary key to keep community scoping consistent with bills.
    id = Column(BigInteger, primary_key=True)
    community_id = Column(Integer, primary_key=True, nullable=False, index=True)

    deal_type = Column(Integer)
    asset_type = Column(Integer)
    asset_name = Column(String(255))
    asset_id = Column(BigInteger, index=True)

    # Amounts in Yuan (converted from cents on ingest)
    income_amount = Column(DECIMAL(12, 2))
    amount = Column(DECIMAL(12, 2))
    discount_amount = Column(DECIMAL(12, 2), default=0)
    late_money_amount = Column(DECIMAL(12, 2), default=0)
    bill_amount = Column(DECIMAL(12, 2), default=0)
    deposit_amount = Column(DECIMAL(12, 2), default=0)

    pay_channel = Column(Integer)
    pay_channel_list = Column(Text)  # JSON string
    pay_channel_str = Column(String(100))

    deal_time = Column(Integer, index=True)  # Unix timestamp seconds
    deal_date = Column(Date, index=True)  # Derived from deal_time for filtering

    remark = Column(Text)
    fk_id = Column(BigInteger, index=True)

    receipt_id = Column(String(50), index=True)
    receipt_record_id = Column(BigInteger, index=True)
    receipt_version = Column(Integer, default=1)

    invoice_number = Column(String(100))
    invoice_urls = Column(Text)  # JSON string
    invoice_status = Column(Integer, default=0)
    open_invoice = Column(Integer, default=0)

    payee = Column(String(255))

    bind_users_raw = Column(Text)  # JSON string, backup of bindUsers

    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    users = relationship(
        "ReceiptBillUser",
        back_populates="receipt_bill",
        cascade="all, delete-orphan",
        foreign_keys="[ReceiptBillUser.receipt_bill_id, ReceiptBillUser.community_id]",
    )


class ReceiptBillUser(Base):
    """Receipt bill related users."""

    __tablename__ = "receipt_bill_users"

    id = Column(Integer, primary_key=True, index=True)
    receipt_bill_id = Column(BigInteger, nullable=False, index=True)
    community_id = Column(Integer, nullable=False, index=True)

    user_id = Column(BigInteger, index=True)
    user_name = Column(String(255))
    phone = Column(String(50))

    created_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        ForeignKeyConstraint(
            ["receipt_bill_id", "community_id"],
            ["receipt_bills.id", "receipt_bills.community_id"],
            ondelete="CASCADE",
        ),
        Index("udx_receipt_bill_users_receipt_user", "receipt_bill_id", "community_id", "user_id", unique=True),
        Index("ix_receipt_bill_users_user_name", "user_name"),
    )

    receipt_bill = relationship(
        "ReceiptBill",
        back_populates="users",
        foreign_keys=[receipt_bill_id, community_id],
    )


class DepositRecord(Base):
    __tablename__ = "deposit_records"

    id = Column(BigInteger, primary_key=True, index=True)
    community_id = Column(Integer, index=True, nullable=True)
    community_name = Column(String(255))

    house_id = Column(BigInteger, index=True)
    house_name = Column(String(255))

    amount = Column(DECIMAL(12, 2))
    operate_type = Column(Integer, index=True)
    operator = Column(BigInteger, index=True)
    operator_name = Column(String(255))
    operate_time = Column(BigInteger, index=True)
    operate_date = Column(Date, index=True)

    cash_pledge_name = Column(String(255))
    remark = Column(Text)

    pay_time = Column(BigInteger, index=True)
    pay_date = Column(Date, index=True)
    payment_id = Column(BigInteger, index=True)
    has_refund_receipt = Column(Boolean, default=False)
    refund_receipt_id = Column(BigInteger, index=True)
    pay_channel_str = Column(String(100))

    raw_data = Column(Text)

    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class PrepaymentRecord(Base):
    __tablename__ = "prepayment_records"

    id = Column(BigInteger, primary_key=True, index=True)
    community_id = Column(Integer, index=True, nullable=True)
    community_name = Column(String(255))

    account_id = Column(BigInteger, index=True)
    building_id = Column(BigInteger, index=True)
    unit_id = Column(BigInteger, index=True)
    house_id = Column(BigInteger, index=True)
    house_name = Column(String(255))

    amount = Column(DECIMAL(12, 2))
    balance_after_change = Column(DECIMAL(12, 2))

    operate_type = Column(Integer, index=True)
    operate_type_label = Column(String(100))

    pay_channel_id = Column(Integer)
    pay_channel_str = Column(String(100))

    operator = Column(BigInteger, index=True)
    operator_name = Column(String(255))

    operate_time = Column(BigInteger, index=True)
    operate_date = Column(Date, index=True)
    source_updated_time = Column(DateTime)

    remark = Column(Text)
    deposit_order_id = Column(BigInteger, index=True)

    pay_time = Column(BigInteger, index=True)
    pay_date = Column(Date, index=True)

    category_id = Column(Integer, index=True)
    category_name = Column(String(255))
    status = Column(Integer)

    payment_id = Column(BigInteger, index=True)
    has_refund_receipt = Column(Boolean, default=False)
    refund_receipt_id = Column(BigInteger, index=True)

    raw_data = Column(Text)

    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class ReceiptBillDepositRefundLink(Base):
    __tablename__ = "receipt_bill_deposit_refund_links"

    id = Column(Integer, primary_key=True, index=True)
    receipt_bill_id = Column(BigInteger, nullable=False, index=True)
    community_id = Column(Integer, nullable=False, index=True)
    deposit_record_id = Column(BigInteger, nullable=False, index=True)
    prepayment_record_id = Column(BigInteger, index=True)
    link_type = Column(String(50), nullable=False, index=True)
    match_rule = Column(String(100))
    match_confidence = Column(Float, default=0)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        ForeignKeyConstraint(
            ["receipt_bill_id", "community_id"],
            ["receipt_bills.id", "receipt_bills.community_id"],
            ondelete="CASCADE",
        ),
        Index(
            "udx_receipt_bill_deposit_refund_links_receipt",
            "receipt_bill_id",
            "community_id",
            unique=True,
        ),
        Index(
            "ix_receipt_bill_deposit_refund_links_lookup",
            "community_id",
            "link_type",
            "deposit_record_id",
        ),
    )


class BillVoucherPushRecord(Base):
    __tablename__ = "bill_voucher_push_records"

    id = Column(Integer, primary_key=True, index=True)
    bill_id = Column(BigInteger, nullable=False, index=True)
    community_id = Column(BigInteger, nullable=False, index=True)
    push_batch_no = Column(String(50), nullable=False, index=True)
    push_status = Column(String(20), nullable=False, default="pushing")
    voucher_number = Column(String(100))
    voucher_id = Column(String(100))
    account_book_id = Column(String(50))
    account_book_name = Column(String(100))
    account_book_number = Column(String(100), index=True)
    api_id = Column(Integer)
    api_name = Column(String(100))
    pushed_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    message = Column(Text)
    request_payload = Column(Text)
    response_payload = Column(Text)
    pushed_at = Column(DateTime)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        ForeignKeyConstraint(
            ['bill_id', 'community_id'],
            ['bills.id', 'bills.community_id'],
            ondelete='CASCADE'
        ),
        Index(
            'ix_bill_voucher_push_records_bill_community_created',
            'bill_id', 'community_id', 'created_at'
        ),
        Index(
            'ix_bill_voucher_push_records_batch_bill_unique',
            'push_batch_no', 'bill_id', 'community_id',
            unique=True
        ),
    )


class VoucherTemplateCategory(Base):
    __tablename__ = "voucher_template_categories"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False)
    parent_id = Column(Integer, ForeignKey("voucher_template_categories.id"), nullable=True)
    sort_order = Column(Integer, default=0)
    status = Column(Integer, default=1)  # 1=active, 0=inactive
    description = Column(String(500))
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    parent = relationship("VoucherTemplateCategory", remote_side=[id], backref="children")


class Organization(Base):
    __tablename__ = "organizations"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False)
    code = Column(String(50), unique=True, index=True)
    parent_id = Column(Integer, ForeignKey("organizations.id"), nullable=True)
    level = Column(Integer, default=1)
    sort_order = Column(Integer, default=0)
    status = Column(Integer, default=1)  # 1=active, 0=inactive
    description = Column(String(500))
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    parent = relationship("Organization", remote_side=[id], backref="children")
    users = relationship("User", back_populates="organization")


user_account_books = Table(
    'user_account_books',
    Base.metadata,
    Column('user_id', Integer, ForeignKey('users.id'), primary_key=True),
    Column('account_book_id', String(50), ForeignKey('kd_account_books.id'), primary_key=True)
)


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(50), unique=True, nullable=False, index=True)
    email = Column(String(100), index=True)
    phone = Column(String(20))
    real_name = Column(String(50))
    password_hash = Column(String(255), nullable=False)
    avatar = Column(String(500))
    org_id = Column(Integer, ForeignKey("organizations.id"), nullable=True)
    status = Column(Integer, default=1)  # 1=active, 0=inactive, 2=locked
    last_login = Column(DateTime)
    role = Column(String(20), default="user") # 'admin' or 'user'
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    organization = relationship("Organization", back_populates="users")
    account_books = relationship("KingdeeAccountBook", secondary=user_account_books, backref="users")


class UserTableColumnPreference(Base):
    __tablename__ = "user_table_column_preferences"
    __table_args__ = (
        Index("ux_user_table_column_preferences_user_table", "user_id", "table_id", unique=True),
    )

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    table_id = Column(String(100), nullable=False)
    hidden_columns = Column(Text, nullable=False, default="[]")
    column_order = Column(Text, nullable=False, default="[]")
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    user = relationship("User")


class RoleMenuPermission(Base):
    __tablename__ = "role_menu_permissions"
    __table_args__ = (
        Index("ux_role_menu_permissions_role_key", "role", "menu_key", unique=True),
    )

    id = Column(Integer, primary_key=True, index=True)
    role = Column(String(50), nullable=False, index=True)
    menu_key = Column(String(200), nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class RoleApiPermission(Base):
    __tablename__ = "role_api_permissions"
    __table_args__ = (
        Index("ux_role_api_permissions_role_key", "role", "api_key", unique=True),
    )

    id = Column(Integer, primary_key=True, index=True)
    role = Column(String(50), nullable=False, index=True)
    api_key = Column(String(200), nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class SyncSchedule(Base):
    __tablename__ = "sync_schedules"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(120), nullable=False, index=True)
    description = Column(String(500))
    target_codes = Column(Text, nullable=False, default="[]")
    community_ids = Column(Text, nullable=False, default="[]")
    account_book_number = Column(String(100))
    account_book_name = Column(String(200))
    schedule_type = Column(String(20), nullable=False, default="daily")
    interval_minutes = Column(Integer)
    daily_time = Column(String(10))
    weekly_days = Column(Text, nullable=False, default="[]")
    timezone = Column(String(64), nullable=False, default="Asia/Shanghai")
    enabled = Column(Boolean, default=True, nullable=False)
    is_running = Column(Boolean, default=False, nullable=False)
    current_execution_id = Column(Integer, ForeignKey("sync_schedule_executions.id"), nullable=True)
    last_run_at = Column(DateTime)
    last_status = Column(String(20))
    last_message = Column(Text)
    next_run_at = Column(DateTime, index=True)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    updated_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    creator = relationship("User", foreign_keys=[created_by])
    updater = relationship("User", foreign_keys=[updated_by])
    current_execution = relationship("SyncScheduleExecution", foreign_keys=[current_execution_id], post_update=True)
    executions = relationship(
        "SyncScheduleExecution",
        back_populates="schedule",
        foreign_keys="SyncScheduleExecution.schedule_id",
        cascade="all, delete-orphan",
        order_by="desc(SyncScheduleExecution.started_at)",
    )


class SyncScheduleExecution(Base):
    __tablename__ = "sync_schedule_executions"

    id = Column(Integer, primary_key=True, index=True)
    schedule_id = Column(Integer, ForeignKey("sync_schedules.id", ondelete="CASCADE"), nullable=False, index=True)
    trigger_type = Column(String(20), nullable=False, default="manual")
    triggered_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    status = Column(String(20), nullable=False, default="running")
    started_at = Column(DateTime, nullable=False, server_default=func.now(), index=True)
    finished_at = Column(DateTime)
    total_targets = Column(Integer, default=0)
    success_targets = Column(Integer, default=0)
    failed_targets = Column(Integer, default=0)
    summary = Column(Text)
    error_message = Column(Text)
    result_payload = Column(Text)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    schedule = relationship("SyncSchedule", back_populates="executions", foreign_keys=[schedule_id])
    triggered_by_user = relationship("User", foreign_keys=[triggered_by])


class ExternalService(Base):
    __tablename__ = "external_services"

    id = Column(Integer, primary_key=True, index=True)
    service_name = Column(String(50), unique=True, nullable=False)  # e.g., 'kingdee'
    display_name = Column(String(100))
    
    # Credentials
    app_id = Column(String(100))
    app_secret = Column(String(200)) # In real world, encrypt this
    auth_url = Column(String(500))
    base_url = Column(String(500))
    
    # Status
    is_active = Column(Boolean, default=True)
    auth_type = Column(String(20), default="oauth2")  # oauth2, basic, api_key, bearer
    auth_method = Column(String(10), default="POST")  # GET or POST for auth_url
    auth_headers = Column(Text) # JSON string
    auth_body = Column(Text)    # JSON string


    
    # Token (Merged from ExternalToken)
    access_token = Column(Text)
    refresh_token = Column(Text)
    expires_at = Column(DateTime)
    token_type = Column(String(50))
    scope = Column(String(255))
    extra_info = Column(Text)  # JSON string
    
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    apis = relationship("ExternalApi", back_populates="service", cascade="all, delete-orphan")


class ExternalApi(Base):
    __tablename__ = "external_apis"

    id = Column(Integer, primary_key=True, index=True)
    service_id = Column(Integer, ForeignKey("external_services.id"), nullable=False)
    name = Column(String(100), nullable=False)
    method = Column(String(10), default="POST")
    url_path = Column(String(500), nullable=False)
    description = Column(String(500))
    
    is_active = Column(Boolean, default=True)
    request_headers = Column(Text)  # JSON string
    request_body = Column(Text)
    response_example = Column(Text)
    notes = Column(Text)
    category = Column(String(50))
    
    
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    service = relationship("ExternalService", back_populates="apis")


class ReportingDbConnection(Base):
    __tablename__ = "reporting_db_connections"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), unique=True, nullable=False, index=True)
    description = Column(String(500))
    db_type = Column(String(30), nullable=False, default="sqlserver")
    host = Column(String(255))
    port = Column(Integer)
    database_name = Column(String(255), nullable=False)
    schema_name = Column(String(100))
    username = Column(String(255))
    password_enc = Column(Text)
    connection_options = Column(Text)  # JSON string
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    datasets = relationship("ReportingDataset", back_populates="connection", cascade="all, delete-orphan")


class ReportingDataset(Base):
    __tablename__ = "reporting_datasets"

    id = Column(Integer, primary_key=True, index=True)
    connection_id = Column(Integer, ForeignKey("reporting_db_connections.id"), nullable=False)
    name = Column(String(120), nullable=False, index=True)
    description = Column(String(500))
    sql_text = Column(Text, nullable=False)
    params_json = Column(Text)  # JSON string for parameter hints/defaults
    row_limit = Column(Integer, default=500)
    last_columns_json = Column(Text)  # JSON string cache of previewed columns
    last_validated_at = Column(DateTime)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    connection = relationship("ReportingDbConnection", back_populates="datasets")
    reports = relationship("ReportingReport", back_populates="dataset", cascade="all, delete-orphan")


class ReportingReport(Base):
    __tablename__ = "reporting_reports"

    id = Column(Integer, primary_key=True, index=True)
    dataset_id = Column(Integer, ForeignKey("reporting_datasets.id"), nullable=False)
    name = Column(String(120), nullable=False, index=True)
    description = Column(String(500))
    report_type = Column(String(30), nullable=False, default="table")
    config_json = Column(Text)  # JSON string: visible columns, default limit, etc.
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    dataset = relationship("ReportingDataset", back_populates="reports")


class GlobalVariable(Base):
    __tablename__ = "global_variables"

    id = Column(Integer, primary_key=True, index=True)
    key = Column(String(100), unique=True, nullable=False, index=True)
    value = Column(String(500), nullable=False)
    description = Column(String(500))
    category = Column(String(50), default="common") # e.g. 'api', 'system', 'auth'
    is_secret = Column(Boolean, default=False)
    
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class AccountingSubject(Base):
    __tablename__ = "accounting_subjects"

    id = Column(String(50), primary_key=True)  # 浣跨敤閲戣澏鐨勫唴鐮両D
    number = Column(String(50), nullable=False, index=True)
    name = Column(String(200), nullable=False)
    fullname = Column(String(500))
    level = Column(Integer)
    is_leaf = Column(Boolean)
    direction = Column(String(10))  # dc: 1(鍊? or -1(璐?
    is_active = Column(Boolean, default=True) # enable: 1 or 0
    long_number = Column(String(50))
    
    # 鎵╁睍瀛楁
    is_cash = Column(Boolean, default=False)
    is_bank = Column(Boolean, default=False)
    is_cash_equivalent = Column(Boolean, default=False)
    account_type_number = Column(String(50)) # accounttype_accounttype
    acct_currency = Column(String(50)) # acctcurrency
    
    # 鏂板瀛楁
    ac_check = Column(Boolean, default=False) # accheck
    is_qty = Column(Boolean, default=False) # isqty
    currency_entry = Column(Text) # JSON for currencyentry

    # 鏍哥畻缁村害涓庡師濮嬫暟鎹?
    check_items = Column(Text) # JSON for checkitementry
    raw_data = Column(Text) # 瀹屾暣鐨勫師濮?JSON 鏁版嵁
    
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

class Customer(Base):
    __tablename__ = "customers"

    id = Column(String(50), primary_key=True)  # 閲戣澏鍐呯爜
    number = Column(String(50), nullable=False, index=True)
    name = Column(String(200), nullable=False)
    status = Column(String(10)) # 鏁版嵁鐘舵€?A/B/C
    enable = Column(String(10)) # 浣跨敤鐘舵€?
    type = Column(String(10)) # 浼欎即绫诲瀷
    
    linkman = Column(String(100))
    bizpartner_phone = Column(String(100))
    bizpartner_address = Column(String(500))
    societycreditcode = Column(String(100))
    
    org_name = Column(String(200))
    createorg_name = Column(String(200))

    entry_bank = Column(Text) # 閾惰淇℃伅 JSON
    entry_linkman = Column(Text) # 鑱旂郴浜轰俊鎭?JSON
    raw_data = Column(Text) # 瀹屾暣鐨勫師濮?JSON 鏁版嵁
    
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

class Supplier(Base):
    __tablename__ = "suppliers"

    id = Column(String(50), primary_key=True)  # 閲戣澏鍐呯爜
    number = Column(String(50), nullable=False, index=True)
    name = Column(String(200), nullable=False)
    status = Column(String(10)) # 鏁版嵁鐘舵€?A/B/C
    enable = Column(String(10)) # 浣跨敤鐘舵€?
    type = Column(String(10)) # 浼欎即绫诲瀷
    
    createorg_number = Column(String(100))
    supplier_status_name = Column(String(100))

    raw_data = Column(Text) # 瀹屾暣鐨勫師濮?JSON 鏁版嵁
    
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

class KingdeeHouse(Base):
    __tablename__ = "kd_houses"

    id = Column(String(50), primary_key=True)  # 閲戣澏鍐呯爜
    number = Column(String(50), index=True)
    wtw8_number = Column(String(50), index=True)
    name = Column(String(255), nullable=False)
    tzqslx = Column(String(100)) # wtw8_combofield_tzqslx
    splx = Column(String(100))   # wtw8_combofield_splx
    createorg_name = Column(String(200))
    createorg_number = Column(String(100))

    raw_data = Column(Text) # 瀹屾暣鐨勫師濮?JSON 鏁版嵁
    
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

class KingdeeAccountBook(Base):
    __tablename__ = "kd_account_books"

    id = Column(String(50), primary_key=True)  # 閲戣澏鍐呯爜
    number = Column(String(50), index=True)
    name = Column(String(255), nullable=False)
    org_number = Column(String(100))
    org_name = Column(String(200))
    accountingsys_number = Column(String(100))
    accountingsys_name = Column(String(200))
    booknature = Column(String(50))   # 1 涓昏处绨? 0 鍓处绨?
    accounttable_name = Column(String(200))
    basecurrency_name = Column(String(100))
    status = Column(String(50))
    enable = Column(String(50))

    raw_data = Column(Text) # 瀹屾暣鐨勫師濮?JSON 鏁版嵁
    
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

class AuxiliaryData(Base):
    __tablename__ = "auxiliary_data"

    id = Column(String(50), primary_key=True)  # 閲戣澏鍐呯爜
    number = Column(String(50), nullable=False, index=True)
    name = Column(String(200), nullable=False)
    issyspreset = Column(Boolean)
    ctrlstrategy = Column(String(10)) # 鎺у埗绛栫暐
    enable = Column(String(10)) # 浣跨敤鐘舵€?
    
    group_number = Column(String(100))
    group_name = Column(String(100))
    parent_number = Column(String(100))
    parent_name = Column(String(100))
    createorg_number = Column(String(100))
    createorg_name = Column(String(100))

    raw_data = Column(Text) # 瀹屾暣鐨勫師濮?JSON 鏁版嵁
    
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

class AuxiliaryDataCategory(Base):
    __tablename__ = "auxiliary_data_categories"

    id = Column(String(50), primary_key=True)  # 閲戣澏鍐呯爜
    number = Column(String(50), nullable=False, index=True)
    name = Column(String(200), nullable=False)
    fissyspreset = Column(Boolean)
    description = Column(String(500))
    ctrlstrategy = Column(String(10)) # 鎺у埗绛栫暐
    
    createorg_name = Column(String(200))
    createorg_number = Column(String(100))
    createorg_id = Column(String(50))

    raw_data = Column(Text) # 瀹屾暣鐨勫師濮?JSON 鏁版嵁
    
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

class KingdeeBankAccount(Base):
    """閲戣澏閾惰璐︽埛"""
    __tablename__ = "kd_bank_accounts"

    id = Column(String(50), primary_key=True)  # 閲戣澏鍐呯爜
    bankaccountnumber = Column(String(100), index=True)  # 閾惰璐﹀彿
    name = Column(String(200))  # 璐︽埛绠€绉?
    acctname = Column(String(200))  # 璐︽埛鍚嶇О

    # 鐢宠缁勭粐
    company_number = Column(String(100))
    company_name = Column(String(200))

    # 寮€鎴风粍缁?
    openorg_number = Column(String(100))
    openorg_name = Column(String(200))

    # 甯佸埆
    defaultcurrency_number = Column(String(50))  # 榛樿甯佸埆浠ｇ爜
    defaultcurrency_name = Column(String(100))   # 榛樿甯佸埆鍚嶇О

    # 璐︽埛灞炴€?
    accttype = Column(String(50))    # 璐︽埛鎬ц川: in_out/in/out
    acctstyle = Column(String(50))   # 璐︽埛绫诲瀷: basic/normal/temp/spcl 绛?
    finorgtype = Column(String(50))  # 閲戣瀺鏈烘瀯绫诲埆: 0 閾惰, 1 缁撶畻涓績, etc.

    # 寮€鎴疯
    banktype_number = Column(String(100))  # 閾惰绫诲埆缂栫爜
    banktype_name = Column(String(200))    # 閾惰绫诲埆鍚嶇О
    bank_number = Column(String(100))      # 寮€鎴疯缂栫爜
    bank_name = Column(String(200))        # 寮€鎴疯鍚嶇О

    # 璐︽埛鐢ㄩ€?
    acctproperty_number = Column(String(100))
    acctproperty_name = Column(String(200))

    # 鐘舵€?
    status = Column(String(10))       # 鏁版嵁鐘舵€?A/B/C
    acctstatus = Column(String(50))   # 璐︽埛鐘舵€? normal/closing/changing/closed/freeze

    # 榛樿鏀朵粯娆?
    isdefaultrec = Column(Boolean, default=False)  # 榛樿鏀舵鎴?
    isdefaultpay = Column(Boolean, default=False)  # 榛樿浠樻鎴?

    comment = Column(Text)  # 澶囨敞

    raw_data = Column(Text) # 瀹屾暣鐨勫師濮?JSON 鏁版嵁
    
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


