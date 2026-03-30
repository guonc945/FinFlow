import json
from datetime import datetime
from uuid import uuid4

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from sqlalchemy.orm import Session

import models
import schemas
from api.dependencies import get_db

router = APIRouter()


@router.post("/api/oa/callback", response_model=schemas.CashJournalResponse)
def receive_oa_callback(data: schemas.OACallback, db: Session = Depends(get_db)):
    existing = db.query(models.ApprovalFormSnapshot).filter(models.ApprovalFormSnapshot.flow_id == data.flow_id).first()
    if existing:
        return existing.journal

    snapshot = models.ApprovalFormSnapshot(
        flow_id=data.flow_id,
        business_type=data.business_type,
        applicant_id=data.applicant_id,
        applicant_name=data.applicant_name,
        department_code=data.department_code,
        total_amount=data.total_amount,
        approved_at=data.approved_at,
        form_data_raw=json.dumps(data.form_data),
    )
    db.add(snapshot)

    journal = models.CashJournal(
        flow_id=data.flow_id,
        amount=data.total_amount,
        direction="O",
        status="pending",
    )
    db.add(journal)
    db.commit()
    db.refresh(journal)
    return journal


@router.get("/api/journals", response_model=list[schemas.CashJournalResponse])
def list_journals(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    journals = db.query(models.CashJournal).offset(skip).limit(limit).all()
    return journals


@router.post("/api/journals/{flow_id}/preview", response_model=schemas.VoucherPreview)
def preview_voucher(flow_id: str, db: Session = Depends(get_db)):
    journal = db.query(models.CashJournal).filter(models.CashJournal.flow_id == flow_id).first()
    if not journal:
        raise HTTPException(status_code=404, detail="Journal not found")

    snapshot = journal.snapshot
    entries = [
        schemas.VoucherEntry(
            line_no=1,
            dr_cr="D",
            account_code="6602.01",
            amount=journal.amount,
            summary=f"Reimbursement for {snapshot.applicant_name}",
            aux_items={"employee": snapshot.applicant_id},
        ),
        schemas.VoucherEntry(
            line_no=2,
            dr_cr="C",
            account_code="1002.01",
            amount=journal.amount,
            summary="Payment",
            aux_items={},
        ),
    ]

    return schemas.VoucherPreview(
        entries=entries,
        total_debit=journal.amount,
        total_credit=journal.amount,
        is_balanced=True,
    )


@router.post("/api/journals/{flow_id}/push", response_model=schemas.PushResult)
def push_to_kingdee(flow_id: str, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    journal = db.query(models.CashJournal).filter(models.CashJournal.flow_id == flow_id).first()
    if not journal:
        raise HTTPException(status_code=404, detail="Journal not found")

    if journal.status == "pushed":
        return schemas.PushResult(success=True, voucher_id=journal.voucher_id, message="Already pushed")

    mock_voucher_id = f"V_{uuid4().hex[:8].upper()}"

    journal.status = "pushed"
    journal.voucher_id = mock_voucher_id
    journal.pushed_at = datetime.now()
    db.commit()

    return schemas.PushResult(success=True, voucher_id=mock_voucher_id, message="Push successful")
