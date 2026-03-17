import { useEffect, useMemo, useState } from "react";
import {
  X,
  FileText,
  Code,
  CheckCircle2,
  AlertTriangle,
  Copy,
} from "lucide-react";
interface VoucherPreviewModalProps {
  isOpen: boolean;
  onClose: () => void;
  data: any;
  isLoading: boolean;
  error: string | null;
  onPushVoucher?: (kingdeeJson: any) => Promise<any>;
}
const VoucherPreviewModal = ({
  isOpen,
  onClose,
  data,
  isLoading,
  error,
  onPushVoucher,
}: VoucherPreviewModalProps) => {
  const uiFont =
    "'Segoe UI', 'PingFang SC', 'Microsoft YaHei', 'Helvetica Neue', sans-serif";
  const monoFont =
    "'JetBrains Mono', 'Cascadia Code', 'Consolas', 'SFMono-Regular', monospace";
  const [isJsonOpen, setIsJsonOpen] = useState(false);
  const [copied, setCopied] = useState(false);
  const [isPushing, setIsPushing] = useState(false);
  const [pushFeedback, setPushFeedback] = useState<{
    type: "success" | "error";
    text: string;
  } | null>(null);
  const [mergeEnabled, setMergeEnabled] = useState(true);
  const getAssgrpDisplayValue = (conf: any): string => {
    if (conf === null || conf === undefined) return "?";
    if (typeof conf !== "object") return String(conf);
    const preferredKeys = ["number", "name", "id", "code"];
    for (const key of preferredKeys) {
      const value = conf?.[key];
      if (
        value !== null &&
        value !== undefined &&
        String(value).trim() !== ""
      ) {
        return String(value);
      }
    }
    const fallback = Object.values(conf).find(
      (v) => v !== null && v !== undefined && String(v).trim() !== "",
    );
    return fallback !== undefined ? String(fallback) : "?";
  };
  const acctView = data?.accounting_view;
  const kdJson = data?.kingdee_json;
  const moneyToCents = (value: any): number => {
    const raw = String(value ?? "").replace(/,/g, "").trim();
    if (!raw) return 0;

    const sign = raw.startsWith("-") ? -1 : 1;
    const normalized = raw.replace(/^[+-]/, "");
    const [intPartRaw = "0", fracPartRaw = ""] = normalized.split(".");
    const intPart = intPartRaw.replace(/\D/g, "") || "0";
    const fracPart = (fracPartRaw.replace(/\D/g, "") + "00").slice(0, 2);

    return sign * (Number(intPart) * 100 + Number(fracPart));
  };
  const centsToMoney = (cents: number): number => cents / 100;
  const buildKingdeeJsonFromEntries = (entries: any[]) => {
    if (!kdJson?.data?.[0]) return kdJson;
    const header = kdJson.data[0];
    const mappedEntries = entries.map((entry: any, idx: number) => {
      const debit = centsToMoney(moneyToCents(entry.debit));
      const credit = centsToMoney(moneyToCents(entry.credit));
      const kdEntry: any = {
        seq: idx + 1,
        edescription: entry.summary || "",
        account_number: entry.account_code || "",
        currency_number: entry.currency || "CNY",
        localrate: Number(entry.localrate || 1),
        debitori: debit,
        creditori: credit,
        debitlocal: debit,
        creditlocal: credit,
      };
      if (entry.assgrp && Object.keys(entry.assgrp).length > 0) {
        kdEntry.assgrp = entry.assgrp;
      }
      if (entry.maincfassgrp && Object.keys(entry.maincfassgrp).length > 0) {
        kdEntry.maincfassgrp = entry.maincfassgrp;
      }
      return kdEntry;
    });

    return {
      ...kdJson,
      data: [
        {
          ...header,
          entries: mappedEntries,
        },
      ],
    };
  };
  const sourceBills = Array.isArray(data?.source_bills)
    ? data.source_bills
    : [];
  const sourceBillSummary = data?.source_bill_push_summary || {};
  const pushBlocked = Boolean(data?.push_blocked);
  const normalizeAssgrp = (assgrp: any): Array<[string, string]> => {
    if (!assgrp || typeof assgrp !== "object") return [];
    return Object.entries(assgrp)
      .map(
        ([key, conf]): [string, string] => [
          String(key),
          getAssgrpDisplayValue(conf),
        ],
      )
      .sort((a, b) => a[0].localeCompare(b[0]));
  };
  const getAccountNameDisplay = (entry: any): string => {
    if (entry?.account_name) return String(entry.account_name);
    if (
      entry?.account_display &&
      String(entry.account_display) !== String(entry?.account_code || "")
    ) {
      return String(entry.account_display);
    }
    return "-";
  };
  const mergeEntries = (entries: any[]) => {
    const merged: any[] = [];
    const indexMap = new Map<string, number>();
    entries.forEach((entry) => {
      const debitCents = moneyToCents(entry.debit);
      const creditCents = moneyToCents(entry.credit);
      const direction = debitCents > 0 ? "debit" : "credit";
      const accountKey = `${entry.account_display || ""}|${entry.account_code || ""}`;
      const assgrpKey = JSON.stringify(normalizeAssgrp(entry.assgrp));
      const key = `${direction}|${accountKey}|${assgrpKey}`;
      if (indexMap.has(key)) {
        const idx = indexMap.get(key) as number;
        const target = merged[idx];
        target.debit_cents = Number(target.debit_cents || 0) + debitCents;
        target.credit_cents = Number(target.credit_cents || 0) + creditCents;
        return;
      }
      indexMap.set(key, merged.length);
      merged.push({
        ...entry,
        debit_cents: debitCents,
        credit_cents: creditCents,
        localrate: Number(entry.localrate || 1),
      });
    });
    return merged.map((entry, idx) => ({
      ...entry,
      debit: centsToMoney(Number(entry.debit_cents || 0)),
      credit: centsToMoney(Number(entry.credit_cents || 0)),
      line_no: idx + 1,
    }));
  };
  const displayedEntries = useMemo(() => {
    if (!acctView?.entries) return [];
    return mergeEnabled ? mergeEntries(acctView.entries) : acctView.entries;
  }, [acctView?.entries, mergeEnabled]);
  const effectiveKingdeeJson = useMemo(() => {
    if (!acctView?.entries || !kdJson) return kdJson;
    return buildKingdeeJsonFromEntries(displayedEntries);
  }, [acctView?.entries, kdJson, displayedEntries]);
  const voucherAmountValidation = useMemo(() => {
    if (!effectiveKingdeeJson?.data?.[0]?.entries) {
      return { ok: true, message: "" };
    }

    const jsonEntries = effectiveKingdeeJson.data[0].entries as any[];
    const displayedDebitCents = displayedEntries.reduce(
      (sum: number, entry: any) => sum + moneyToCents(entry.debit),
      0,
    );
    const displayedCreditCents = displayedEntries.reduce(
      (sum: number, entry: any) => sum + moneyToCents(entry.credit),
      0,
    );
    const jsonDebitCents = jsonEntries.reduce(
      (sum: number, entry: any) => sum + moneyToCents(entry.debitori),
      0,
    );
    const jsonCreditCents = jsonEntries.reduce(
      (sum: number, entry: any) => sum + moneyToCents(entry.creditori),
      0,
    );
    const jsonLocalDebitCents = jsonEntries.reduce(
      (sum: number, entry: any) => sum + moneyToCents(entry.debitlocal),
      0,
    );
    const jsonLocalCreditCents = jsonEntries.reduce(
      (sum: number, entry: any) => sum + moneyToCents(entry.creditlocal),
      0,
    );

    if (jsonDebitCents !== jsonCreditCents) {
      return { ok: false, message: "JSON 借贷金额不平衡，已阻止推送。" };
    }
    if (jsonLocalDebitCents !== jsonLocalCreditCents) {
      return { ok: false, message: "JSON 本位币借贷金额不平衡，已阻止推送。" };
    }
    if (
      jsonDebitCents !== displayedDebitCents ||
      jsonCreditCents !== displayedCreditCents
    ) {
      return { ok: false, message: "JSON 金额与当前凭证视图不一致，已阻止推送。" };
    }

    return { ok: true, message: "" };
  }, [displayedEntries, effectiveKingdeeJson]);
  const canPush =
    !!onPushVoucher &&
    !!effectiveKingdeeJson &&
    !isLoading &&
    !error &&
    data?.matched &&
    !pushBlocked &&
    voucherAmountValidation.ok;
  const handleCopyJson = () => {
    if (effectiveKingdeeJson) {
      navigator.clipboard.writeText(JSON.stringify(effectiveKingdeeJson, null, 2));
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  };
  useEffect(() => {
    if (isOpen) {
      setPushFeedback(null);
      setIsPushing(false);
      setMergeEnabled(true);
      setIsJsonOpen(false);
      setCopied(false);
    }
  }, [isOpen, data]);
  const handlePushVoucher = async () => {
    if (!canPush || !onPushVoucher || !effectiveKingdeeJson) return;
    setIsPushing(true);
    setPushFeedback(null);
    try {
      const result = await onPushVoucher(effectiveKingdeeJson);
      if (result?.success) {
        setPushFeedback({
          type: "success",
          text: result?.message || "凭证推送成功",
        });
      } else {
        setPushFeedback({
          type: "error",
          text: result?.message || "凭证推送失败",
        });
      }
    } catch (err: any) {
      const msg =
        err?.response?.data?.detail ||
        err?.response?.data?.message ||
        err?.message ||
        "凭证推送失败";
      setPushFeedback({ type: "error", text: msg });
    } finally {
      setIsPushing(false);
    }
  };
  if (!isOpen) return null;
  return (
    <div
      onClick={onClose}
      style={{
        position: "fixed",
        inset: 0,
        zIndex: 1000,
        backgroundColor: "rgba(15, 23, 42, 0.5)",
        backdropFilter: "blur(8px)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        animation: "fadeIn 0.2s ease-out",
      }}
    >
      <div
        onClick={(e) => e.stopPropagation()}
        style={{
          background: "#fff",
          borderRadius: "1rem",
          width: "1220px",
          maxWidth: "96vw",
          maxHeight: "90vh",
          display: "flex",
          flexDirection: "column",
          boxShadow: "0 25px 50px -12px rgba(0,0,0,0.25)",
          border: "1px solid #e2e8f0",
          overflow: "hidden",
          animation: "slideUp 0.3s ease",
          fontFamily: uiFont,
        }}
      >
        {/* 标题栏*/}
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            padding: "1rem 1.5rem",
            borderBottom: "1px solid #f1f5f9",
            background: "linear-gradient(135deg, #1e293b 0%, #334155 100%)",
            color: "#fff",
          }}
        >
          <div
            style={{ display: "flex", alignItems: "center", gap: "0.75rem" }}
          >
            <FileText size={20} />
            <div>
              <h3
                style={{
                  margin: 0,
                  fontSize: "1.05rem",
                  lineHeight: 1.3,
                  fontWeight: 700,
                  letterSpacing: "0.01em",
                }}
              >
                凭证预览
              </h3>
              {data?.template_name && (
                <span
                  style={{
                    fontSize: "0.78rem",
                    opacity: 0.82,
                    lineHeight: 1.5,
                  }}
                >
                  匹配模板: {data.template_name} ({data.template_id})
                </span>
              )}
            </div>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
            {!!effectiveKingdeeJson && !isLoading && !error && data?.matched && (
              <button
                onClick={() => setIsJsonOpen(true)}
                style={{
                  background: "rgba(255,255,255,0.15)",
                  border: "1px solid rgba(255,255,255,0.2)",
                  borderRadius: "0.5rem",
                  color: "#fff",
                  cursor: "pointer",
                  padding: "0.38rem 0.75rem",
                  display: "flex",
                  alignItems: "center",
                  gap: "0.375rem",
                  fontSize: "0.75rem",
                  fontWeight: 600,
                }}
              >
                <Code size={14} /> 查看 JSON
              </button>
            )}
            {!!onPushVoucher &&
              !!effectiveKingdeeJson &&
              !isLoading &&
              !error &&
              data?.matched && (
                <button
                  onClick={handlePushVoucher}
                  disabled={isPushing || pushBlocked || !voucherAmountValidation.ok}
                  style={{
                    background:
                      isPushing || pushBlocked || !voucherAmountValidation.ok
                        ? "rgba(255,255,255,0.2)"
                        : "#16a34a",
                    border: "none",
                    borderRadius: "0.5rem",
                    color: "#fff",
                    cursor:
                      isPushing || pushBlocked || !voucherAmountValidation.ok ? "not-allowed" : "pointer",
                    padding: "0.38rem 0.75rem",
                    display: "flex",
                    alignItems: "center",
                    fontSize: "0.75rem",
                    fontWeight: 600,
                    opacity: isPushing || pushBlocked || !voucherAmountValidation.ok ? 0.8 : 1,
                  }}
                  title={
                    pushBlocked
                      ? data?.push_block_reason || "当前账单已存在推送记录"
                      : "推送当前凭证到金蝶系统"
                  }
                >
                  {isPushing
                    ? "推送中..."
                    : pushBlocked
                      ? "已锁定"
                      : "推送凭证"}
                </button>
              )}
            <button
              onClick={onClose}
              style={{
                background: "rgba(255,255,255,0.15)",
                border: "none",
                borderRadius: "0.5rem",
                color: "#fff",
                cursor: "pointer",
                padding: "0.375rem",
                display: "flex",
              }}
            >
              <X size={18} />
            </button>
          </div>
        </div>
        {/* 内容区*/}
        <div
          style={{
            flex: 1,
            minHeight: 0,
            padding: "1.25rem 1.5rem",
            display: "flex",
            flexDirection: "column",
            overflow: "hidden",
          }}
        >
          {isLoading ? (
            <div
              style={{
                display: "flex",
                flexDirection: "column",
                alignItems: "center",
                justifyContent: "center",
                padding: "4rem 0",
                gap: "1rem",
                color: "#64748b",
              }}
            >
              <div
                style={{
                  width: 40,
                  height: 40,
                  border: "3px solid #e2e8f0",
                  borderTopColor: "#3b82f6",
                  borderRadius: "50%",
                  animation: "spin 0.8s linear infinite",
                }}
              />
              <span>正在解析模板并生成凭证预览...</span>
            </div>
          ) : error ? (
            <div
              style={{
                display: "flex",
                flexDirection: "column",
                alignItems: "center",
                padding: "3rem",
                gap: "1rem",
                color: "#ef4444",
              }}
            >
              <AlertTriangle size={40} />
              <p style={{ fontSize: "0.875rem", textAlign: "center" }}>
                {error}
              </p>
            </div>
          ) : data && !data.matched ? (
            <div
              style={{
                display: "flex",
                flexDirection: "column",
                alignItems: "center",
                padding: "3rem",
                gap: "1rem",
                color: "#f59e0b",
              }}
            >
              <AlertTriangle size={40} />
              <p style={{ margin: 0, fontWeight: 600 }}>未匹配到适用模板</p>
              <p
                style={{
                  fontSize: "0.8rem",
                  color: "#94a3b8",
                  textAlign: "center",
                }}
              >
                已检查 {data.templates_checked}
                个模板，没有找到符合当前账单条件的凭证模板。
                <br /> 请前往“凭证模板中心”创建或调整模板的触发条件。
              </p>
            </div>
          ) : data ? (
            <>
              {pushFeedback && (
                <div
                  style={{
                    marginBottom: "0.75rem",
                    padding: "0.5rem 0.75rem",
                    borderRadius: "0.5rem",
                    fontSize: "0.75rem",
                    fontWeight: 600,
                    background:
                      pushFeedback.type === "success" ? "#f0fdf4" : "#fef2f2",
                    color:
                      pushFeedback.type === "success" ? "#166534" : "#991b1b",
                    border: `1px solid ${pushFeedback.type === "success" ? "#bbf7d0" : "#fecaca"}`,
                  }}
                >
                  {pushFeedback.text}
                </div>
              )}
              {!voucherAmountValidation.ok && (
                <div
                  style={{
                    marginBottom: "0.75rem",
                    padding: "0.5rem 0.75rem",
                    borderRadius: "0.5rem",
                    fontSize: "0.75rem",
                    fontWeight: 600,
                    background: "#fef2f2",
                    color: "#991b1b",
                    border: "1px solid #fecaca",
                  }}
                >
                  {voucherAmountValidation.message}
                </div>
              )}
              {sourceBills.length > 0 && (
                <div
                  style={{
                    marginBottom: "0.75rem",
                    padding: "0.75rem",
                    borderRadius: "0.75rem",
                    background: pushBlocked ? "#fff7ed" : "#f8fafc",
                    border: `1px solid ${pushBlocked ? "#fdba74" : "#e2e8f0"}`,
                  }}
                >
                  <div
                    style={{
                      display: "flex",
                      justifyContent: "space-between",
                      gap: "0.75rem",
                      flexWrap: "wrap",
                    }}
                  >
                    <div>
                      <div
                        style={{
                          fontSize: "0.82rem",
                          fontWeight: 700,
                          color: "#0f172a",
                        }}
                      >
                        本次凭证关联账单：
                        {sourceBillSummary.total || sourceBills.length} 笔
                      </div>
                      <div
                        style={{
                          fontSize: "0.75rem",
                          color: "#64748b",
                          marginTop: "0.25rem",
                        }}
                      >
                        未推送 {sourceBillSummary.not_pushed || 0} / 推送中
                        {sourceBillSummary.pushing || 0} / 已推送
                        {sourceBillSummary.success || 0} / 失败
                        {sourceBillSummary.failed || 0}
                      </div>
                    </div>
                    {pushBlocked && (
                      <div
                        style={{
                          fontSize: "0.75rem",
                          color: "#9a3412",
                          fontWeight: 600,
                          maxWidth: "420px",
                        }}
                      >
                        {data?.push_block_reason ||
                          "当前账单已存在推送记录，请先核对后再操作。"}
                      </div>
                    )}
                  </div>
                </div>
              )}
              {acctView && (
                <div
                  style={{
                    flex: 1,
                    minHeight: 0,
                    display: "flex",
                    flexDirection: "column",
                  }}
                >
                  {/* 平衡状态*/}
                  <div
                    style={{
                      display: "flex",
                      alignItems: "center",
                      gap: "0.5rem",
                      marginBottom: "0.75rem",
                      padding: "0.5rem 0.75rem",
                      borderRadius: "0.5rem",
                      fontSize: "0.8rem",
                      fontWeight: 600,
                      background: acctView.is_balanced ? "#f0fdf4" : "#fef2f2",
                      color: acctView.is_balanced ? "#166534" : "#991b1b",
                      border: `1px solid ${acctView.is_balanced ? "#bbf7d0" : "#fecaca"}`,
                      flexShrink: 0,
                    }}
                  >
                    {acctView.is_balanced ? (
                      <>
                        <CheckCircle2 size={14} /> 借贷平衡
                      </>
                    ) : (
                      <>
                        <AlertTriangle size={14} /> 借贷不平衡，差额：¥
                        {Math.abs(
                          acctView.total_debit - acctView.total_credit,
                        ).toFixed(2)}
                      </>
                    )}
                  </div>
                  <div
                    style={{
                      display: "flex",
                      justifyContent: "flex-end",
                      marginBottom: "0.75rem",
                      flexShrink: 0,
                    }}
                  >
                    <label
                      style={{
                        display: "inline-flex",
                        alignItems: "center",
                        gap: "0.5rem",
                        fontSize: "0.75rem",
                        color: "#475569",
                        cursor: "pointer",
                        background: "#f8fafc",
                        border: "1px solid #e2e8f0",
                        padding: "0.35rem 0.6rem",
                        borderRadius: "999px",
                      }}
                    >
                      <input
                        type="checkbox"
                        checked={mergeEnabled}
                        onChange={(e) => setMergeEnabled(e.target.checked)}
                      />
                      合并同核算项（借/贷方向一致）
                    </label>
                  </div>
                  {/* 凭证表格 */}
                  <div
                    style={{
                      flex: 1,
                      minHeight: 0,
                      borderRadius: "0.75rem",
                      border: "1px solid #e2e8f0",
                      overflow: "auto",
                      background:
                        "linear-gradient(180deg, #fcfdff 0%, #f8fafc 100%)",
                      boxShadow: "inset 0 1px 0 rgba(255,255,255,0.8)",
                    }}
                  >
                    <table
                      style={{
                        width: "100%",
                        borderCollapse: "collapse",
                        fontSize: "0.8rem",
                        tableLayout: "fixed",
                      }}
                    >
                      <VoucherTableColGroup />
                      <thead>
                        <tr style={{ background: "#f8fafc" }}>
                          <th style={thStyle}>行号</th>
                          <th
                            style={{
                              ...thStyle,
                              textAlign: "left",
                              minWidth: 180,
                            }}
                          >
                            摘要
                          </th>
                          <th
                            style={{
                              ...thStyle,
                              textAlign: "left",
                              minWidth: 180,
                            }}
                          >
                            会计科目
                          </th>
                          <th style={thStyle}>借方金额</th>
                          <th style={thStyle}>贷方金额</th>
                          <th style={{ ...thStyle, textAlign: "left" }}>
                            辅助核算
                          </th>
                        </tr>
                      </thead>
                      <tbody>
                        {displayedEntries.map((entry: any, idx: number) => (
                          <tr
                            key={idx}
                            style={{
                              borderBottom: "1px solid #e2e8f0",
                              background: idx % 2 === 0 ? "#ffffff" : "#fbfdff",
                            }}
                          >
                            <td
                              style={{
                                ...tdStyle,
                                textAlign: "center",
                                color: "#94a3b8",
                              }}
                            >
                              {entry.line_no}
                            </td>
                            <td
                              style={{
                                ...tdStyle,
                                color: "#1e293b",
                                lineHeight: 1.55,
                                fontSize: "0.82rem",
                              }}
                            >
                              {entry.summary}
                            </td>
                            <td style={tdStyle}>
                              <div
                                style={{
                                  display: "flex",
                                  flexDirection: "column",
                                  gap: "0.2rem",
                                  lineHeight: 1.35,
                                }}
                              >
                                <span
                                  style={{
                                    fontSize: "0.72rem",
                                    fontFamily: monoFont,
                                    color: "#64748b",
                                    letterSpacing: "0.01em",
                                  }}
                                >
                                  {entry.account_code || "-"}
                                </span>
                                <span
                                  style={{
                                    fontSize: "0.82rem",
                                    color: "#0f172a",
                                    fontWeight: 600,
                                    wordBreak: "break-word",
                                  }}
                                >
                                  {getAccountNameDisplay(entry)}
                                </span>
                              </div>
                            </td>
                            <td
                              style={{
                                ...tdStyle,
                                textAlign: "right",
                                fontWeight: 600,
                                fontFamily: monoFont,
                                fontSize: "0.82rem",
                                color: entry.debit > 0 ? "#0f172a" : "#cbd5e1",
                              }}
                            >
                              {entry.debit > 0
                                ? `¥${entry.debit.toFixed(2)}`
                                : "-"}
                            </td>
                            <td
                              style={{
                                ...tdStyle,
                                textAlign: "right",
                                fontWeight: 600,
                                fontFamily: monoFont,
                                fontSize: "0.82rem",
                                color: entry.credit > 0 ? "#0f172a" : "#cbd5e1",
                              }}
                            >
                              {entry.credit > 0
                                ? `¥${entry.credit.toFixed(2)}`
                                : "-"}
                            </td>
                            <td style={tdStyle}>
                              {entry.assgrp &&
                                Object.keys(entry.assgrp).length > 0 && (
                                  <div
                                    style={{
                                      display: "flex",
                                      flexWrap: "wrap",
                                      gap: "0.25rem",
                                    }}
                                  >
                                    {Object.entries(entry.assgrp).map(
                                      ([dim, conf]: [string, any]) => (
                                        <span
                                          key={dim}
                                          style={{
                                            fontSize: "0.65rem",
                                            background: "#eff6ff",
                                            color: "#2563eb",
                                            padding: "0.125rem 0.375rem",
                                            borderRadius: "999px",
                                            border: "1px solid #bfdbfe",
                                            whiteSpace: "nowrap",
                                          }}
                                        >
                                          {dim}:{getAssgrpDisplayValue(conf)}
                                        </span>
                                      ),
                                    )}
                                  </div>
                                )}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                      <tfoot>
                        <tr
                          style={{
                            background: "#f1f5f9",
                            fontWeight: 700,
                            fontSize: "0.85rem",
                          }}
                        >
                          <td
                            style={{
                              ...tdStyle,
                              textAlign: "center",
                              position: "sticky",
                              bottom: 0,
                              background: "#f1f5f9",
                              boxShadow: "0 -1px 0 #e2e8f0",
                            }}
                            colSpan={3}
                          >
                            合计
                          </td>
                          <td
                            style={{
                              ...tdStyle,
                              textAlign: "right",
                              fontFamily: monoFont,
                              color: "#059669",
                              position: "sticky",
                              bottom: 0,
                              background: "#f1f5f9",
                              boxShadow: "0 -1px 0 #e2e8f0",
                            }}
                          >
                            ¥{acctView.total_debit.toFixed(2)}
                          </td>
                          <td
                            style={{
                              ...tdStyle,
                              textAlign: "right",
                              fontFamily: monoFont,
                              color: "#059669",
                              position: "sticky",
                              bottom: 0,
                              background: "#f1f5f9",
                              boxShadow: "0 -1px 0 #e2e8f0",
                            }}
                          >
                            ¥{acctView.total_credit.toFixed(2)}
                          </td>
                          <td
                            style={{
                              ...tdStyle,
                              position: "sticky",
                              bottom: 0,
                              background: "#f1f5f9",
                              boxShadow: "0 -1px 0 #e2e8f0",
                            }}
                          ></td>
                        </tr>
                      </tfoot>
                    </table>
                  </div>
                </div>
              )}
            </>
          ) : null}
        </div>
      </div>
      {isJsonOpen && effectiveKingdeeJson && (
        <div
          onClick={() => setIsJsonOpen(false)}
          style={{
            position: "fixed",
            inset: 0,
            zIndex: 1001,
            backgroundColor: "rgba(15, 23, 42, 0.55)",
            backdropFilter: "blur(6px)",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            padding: "1.5rem",
          }}
        >
          <div
            onClick={(e) => e.stopPropagation()}
            style={{
              width: "960px",
              maxWidth: "92vw",
              maxHeight: "82vh",
              background: "#fff",
              borderRadius: "1rem",
              boxShadow: "0 25px 50px -12px rgba(0,0,0,0.35)",
              border: "1px solid #e2e8f0",
              overflow: "hidden",
              display: "flex",
              flexDirection: "column",
            }}
          >
            <div
              style={{
                display: "flex",
                justifyContent: "space-between",
                alignItems: "center",
                padding: "0.9rem 1.2rem",
                borderBottom: "1px solid #e2e8f0",
                background: "#f8fafc",
              }}
            >
              <div>
                <div
                  style={{
                    fontSize: "0.92rem",
                    fontWeight: 700,
                    color: "#0f172a",
                  }}
                >
                  JSON Preview
                </div>
                <div style={{ fontSize: "0.75rem", color: "#64748b" }}>
                  POST /v2/gl/gl_voucher/voucherAdd
                </div>
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
                <button
                  onClick={handleCopyJson}
                  style={{
                    display: "flex",
                    alignItems: "center",
                    gap: "0.375rem",
                    padding: "0.375rem 0.75rem",
                    borderRadius: "0.5rem",
                    border: "1px solid #e2e8f0",
                    background: copied ? "#f0fdf4" : "#fff",
                    cursor: "pointer",
                    fontSize: "0.75rem",
                    fontWeight: 500,
                    color: copied ? "#166534" : "#64748b",
                  }}
                >
                  <Copy size={12} /> {copied ? "已复制" : "复制 JSON"}
                </button>
                <button
                  onClick={() => setIsJsonOpen(false)}
                  style={{
                    background: "#fff",
                    border: "1px solid #e2e8f0",
                    borderRadius: "0.5rem",
                    color: "#475569",
                    cursor: "pointer",
                    padding: "0.375rem",
                    display: "flex",
                  }}
                >
                  <X size={16} />
                </button>
              </div>
            </div>
            <div style={{ padding: "1rem 1.2rem", overflow: "auto" }}>
              <pre
                style={{
                  margin: 0,
                  background: "#0f172a",
                  color: "#e2e8f0",
                  padding: "1.25rem",
                  borderRadius: "0.75rem",
                  fontSize: "0.78rem",
                  lineHeight: 1.6,
                  overflow: "auto",
                  maxHeight: "62vh",
                  fontFamily:
                    "'JetBrains Mono', 'Fira Code', 'Consolas', monospace",
                  border: "1px solid #1e293b",
                }}
              >
                <JsonSyntaxHighlight json={effectiveKingdeeJson} />
              </pre>
            </div>
          </div>
        </div>
      )}
      {/* 动画 keyframes */}
      <style>{`                 @keyframes fadeIn { from { opacity: 0; } to { opacity: 1; } }                 @keyframes slideUp { from { opacity: 0; transform: translateY(20px); } to { opacity: 1; transform: translateY(0); } }                 @keyframes spin { to { transform: rotate(360deg); } }             `}</style>
    </div>
  );
};

// 简单 JSON 语法高亮组件
const JsonSyntaxHighlight = ({ json }: { json: any }) => {
  const str = JSON.stringify(json, null, 2);
  const parts = str.split(/("(?:[^"\\]|\\.)*")\s*:/g);

  const elements: React.ReactElement[] = [];
  parts.forEach((part, i) => {
    if (i % 2 === 1) {
      // 这是 key
      elements.push(
        <span key={i} style={{ color: "#7dd3fc" }}>
          {part}
        </span>,
      );
      elements.push(<span key={`${i}-colon`}>:</span>);
    } else {
      // 高亮值
      const highlighted = part
        .replace(/"([^"]*)"/g, '<span style="color:#fbbf24">"$1"</span>')
        .replace(/\b(\d+\.?\d*)\b/g, '<span style="color:#a78bfa">$1</span>')
        .replace(
          /\b(true|false|null)\b/g,
          '<span style="color:#f472b6">$1</span>',
        );
      elements.push(
        <span key={i} dangerouslySetInnerHTML={{ __html: highlighted }} />,
      );
    }
  });

  return <code>{elements}</code>;
};

// 表格样式常量
const VoucherTableColGroup = () => (
  <colgroup>
    <col style={{ width: "72px" }} />
    <col style={{ width: "28%" }} />
    <col style={{ width: "240px" }} />
    <col style={{ width: "150px" }} />
    <col style={{ width: "150px" }} />
    <col />
  </colgroup>
);

const thStyle: React.CSSProperties = {
  padding: "0.75rem 0.875rem",
  fontWeight: 700,
  fontSize: "0.72rem",
  color: "#475569",
  textAlign: "center",
  borderBottom: "2px solid #e2e8f0",
  position: "sticky",
  top: 0,
  zIndex: 1,
  background: "#f8fafc",
  textTransform: "uppercase" as const,
  letterSpacing: "0.05em",
};

const tdStyle: React.CSSProperties = {
  padding: "0.8rem 0.875rem",
  verticalAlign: "middle",
  color: "#334155",
};

export default VoucherPreviewModal;
