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
  const [activeView, setActiveView] = useState<"accounting" | "json">(
    "accounting",
  );
  const [copied, setCopied] = useState(false);
  const [isPushing, setIsPushing] = useState(false);
  const [pushFeedback, setPushFeedback] = useState<{
    type: "success" | "error";
    text: string;
  } | null>(null);
  const [mergeEnabled, setMergeEnabled] = useState(true);
  const handleCopyJson = () => {
    if (data?.kingdee_json) {
      navigator.clipboard.writeText(JSON.stringify(data.kingdee_json, null, 2));
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  };
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
  const sourceBills = Array.isArray(data?.source_bills)
    ? data.source_bills
    : [];
  const sourceBillSummary = data?.source_bill_push_summary || {};
  const pushBlocked = Boolean(data?.push_blocked);
  const canPush =
    !!onPushVoucher &&
    !!kdJson &&
    !isLoading &&
    !error &&
    data?.matched &&
    !pushBlocked;
  const normalizeAssgrp = (assgrp: any): Array<[string, string]> => {
    if (!assgrp || typeof assgrp !== "object") return [];
    return Object.entries(assgrp)
      .map(([key, conf]) => [String(key), getAssgrpDisplayValue(conf)])
      .sort((a, b) => a[0].localeCompare(b[0]));
  };
  const mergeEntries = (entries: any[]) => {
    const merged: any[] = [];
    const indexMap = new Map<string, number>();
    entries.forEach((entry) => {
      const direction = entry.debit > 0 ? "debit" : "credit";
      const accountKey = `${entry.account_display || ""}|${entry.account_code || ""}`;
      const assgrpKey = JSON.stringify(normalizeAssgrp(entry.assgrp));
      const key = `${direction}|${accountKey}|${assgrpKey}`;
      if (indexMap.has(key)) {
        const idx = indexMap.get(key) as number;
        const target = merged[idx];
        target.debit = Number(target.debit || 0) + Number(entry.debit || 0);
        target.credit = Number(target.credit || 0) + Number(entry.credit || 0);
        return;
      }
      indexMap.set(key, merged.length);
      merged.push({ ...entry });
    });
    return merged.map((entry, idx) => ({ ...entry, line_no: idx + 1 }));
  };
  const displayedEntries = useMemo(() => {
    if (!acctView?.entries) return [];
    return mergeEnabled ? mergeEntries(acctView.entries) : acctView.entries;
  }, [acctView?.entries, mergeEnabled]);
  useEffect(() => {
    if (isOpen) {
      setPushFeedback(null);
      setIsPushing(false);
      setMergeEnabled(true);
    }
  }, [isOpen, data]);
  const handlePushVoucher = async () => {
    if (!canPush || !onPushVoucher) return;
    setIsPushing(true);
    setPushFeedback(null);
    try {
      const result = await onPushVoucher(kdJson);
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
          width: "960px",
          maxWidth: "95vw",
          maxHeight: "90vh",
          display: "flex",
          flexDirection: "column",
          boxShadow: "0 25px 50px -12px rgba(0,0,0,0.25)",
          border: "1px solid #e2e8f0",
          overflow: "hidden",
          animation: "slideUp 0.3s ease",
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
              <h3 style={{ margin: 0, fontSize: "1rem", fontWeight: 700 }}>
                凭证预览
              </h3>
              {data?.template_name && (
                <span style={{ fontSize: "0.75rem", opacity: 0.8 }}>
                  匹配模板: {data.template_name} ({data.template_id})
                </span>
              )}
            </div>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
            {!!onPushVoucher &&
              !!kdJson &&
              !isLoading &&
              !error &&
              data?.matched && (
                <button
                  onClick={handlePushVoucher}
                  disabled={isPushing || pushBlocked}
                  style={{
                    background:
                      isPushing || pushBlocked
                        ? "rgba(255,255,255,0.2)"
                        : "#16a34a",
                    border: "none",
                    borderRadius: "0.5rem",
                    color: "#fff",
                    cursor:
                      isPushing || pushBlocked ? "not-allowed" : "pointer",
                    padding: "0.38rem 0.75rem",
                    display: "flex",
                    alignItems: "center",
                    fontSize: "0.75rem",
                    fontWeight: 600,
                    opacity: isPushing || pushBlocked ? 0.8 : 1,
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
        <div style={{ flex: 1, overflow: "auto", padding: "1.25rem 1.5rem" }}>
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
              {/* 视图切换标签 */}
              <div
                style={{
                  display: "flex",
                  gap: "0.25rem",
                  marginBottom: "1rem",
                  background: "#f1f5f9",
                  borderRadius: "0.5rem",
                  padding: "0.25rem",
                }}
              >
                <button
                  onClick={() => setActiveView("accounting")}
                  style={{
                    flex: 1,
                    padding: "0.5rem",
                    border: "none",
                    borderRadius: "0.375rem",
                    cursor: "pointer",
                    fontSize: "0.8rem",
                    fontWeight: 600,
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                    gap: "0.375rem",
                    background:
                      activeView === "accounting" ? "#fff" : "transparent",
                    color: activeView === "accounting" ? "#1e293b" : "#64748b",
                    boxShadow:
                      activeView === "accounting"
                        ? "0 1px 3px rgba(0,0,0,0.1)"
                        : "none",
                    transition: "all 0.2s",
                  }}
                >
                  <FileText size={14} /> 会计凭证视图
                </button>
                <button
                  onClick={() => setActiveView("json")}
                  style={{
                    flex: 1,
                    padding: "0.5rem",
                    border: "none",
                    borderRadius: "0.375rem",
                    cursor: "pointer",
                    fontSize: "0.8rem",
                    fontWeight: 600,
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                    gap: "0.375rem",
                    background: activeView === "json" ? "#fff" : "transparent",
                    color: activeView === "json" ? "#1e293b" : "#64748b",
                    boxShadow:
                      activeView === "json"
                        ? "0 1px 3px rgba(0,0,0,0.1)"
                        : "none",
                    transition: "all 0.2s",
                  }}
                >
                  <Code size={14} /> 金蝶推送 JSON
                </button>
              </div>
              {/* 会计凭证视图 */}
              {activeView === "accounting" && acctView && (
                <div>
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
                      borderRadius: "0.75rem",
                      border: "1px solid #e2e8f0",
                      overflow: "hidden",
                    }}
                  >
                    <table
                      style={{
                        width: "100%",
                        borderCollapse: "collapse",
                        fontSize: "0.8rem",
                      }}
                    >
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
                              minWidth: 120,
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
                            style={{ borderBottom: "1px solid #f1f5f9" }}
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
                            <td style={tdStyle}>{entry.summary}</td>
                            <td style={tdStyle}>
                              <div
                                style={{
                                  fontSize: "0.75rem",
                                  background: "#f1f5f9",
                                  padding: "0.125rem 0.375rem",
                                  borderRadius: "0.25rem",
                                  color: "#334155",
                                  display: "inline-block",
                                }}
                              >
                                {entry.account_display}
                              </div>
                            </td>
                            <td
                              style={{
                                ...tdStyle,
                                textAlign: "right",
                                fontWeight: 600,
                                fontFamily: "'JetBrains Mono', monospace",
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
                                fontFamily: "'JetBrains Mono', monospace",
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
                            background: "#f8fafc",
                            fontWeight: 700,
                            fontSize: "0.85rem",
                          }}
                        >
                          <td
                            style={{ ...tdStyle, textAlign: "center" }}
                            colSpan={3}
                          >
                            合计
                          </td>
                          <td
                            style={{
                              ...tdStyle,
                              textAlign: "right",
                              fontFamily: "'JetBrains Mono', monospace",
                              color: "#059669",
                            }}
                          >
                            ¥{acctView.total_debit.toFixed(2)}
                          </td>
                          <td
                            style={{
                              ...tdStyle,
                              textAlign: "right",
                              fontFamily: "'JetBrains Mono', monospace",
                              color: "#059669",
                            }}
                          >
                            ¥{acctView.total_credit.toFixed(2)}
                          </td>
                          <td style={tdStyle}></td>
                        </tr>
                      </tfoot>
                    </table>
                  </div>
                </div>
              )}
              {/* 金蝶 JSON 视图 */}
              {activeView === "json" && kdJson && (
                <div>
                  <div
                    style={{
                      display: "flex",
                      justifyContent: "space-between",
                      alignItems: "center",
                      marginBottom: "0.5rem",
                    }}
                  >
                    <span style={{ fontSize: "0.75rem", color: "#64748b" }}>
                      POST /v2/gl/gl_voucher/voucherAdd
                    </span>
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
                        transition: "all 0.2s",
                      }}
                    >
                      <Copy size={12} /> {copied ? "已复制" : "复制 JSON"}
                    </button>
                  </div>
                  <pre
                    style={{
                      background: "#0f172a",
                      color: "#e2e8f0",
                      padding: "1.25rem",
                      borderRadius: "0.75rem",
                      fontSize: "0.78rem",
                      lineHeight: 1.6,
                      overflow: "auto",
                      maxHeight: "400px",
                      fontFamily:
                        "'JetBrains Mono', 'Fira Code', 'Consolas', monospace",
                      border: "1px solid #1e293b",
                    }}
                  >
                    <JsonSyntaxHighlight json={kdJson} />
                  </pre>
                </div>
              )}
            </>
          ) : null}
        </div>
      </div>
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
const thStyle: React.CSSProperties = {
  padding: "0.625rem 0.75rem",
  fontWeight: 600,
  fontSize: "0.75rem",
  color: "#64748b",
  textAlign: "center",
  borderBottom: "2px solid #e2e8f0",
  textTransform: "uppercase" as const,
  letterSpacing: "0.03em",
};

const tdStyle: React.CSSProperties = {
  padding: "0.625rem 0.75rem",
  verticalAlign: "middle",
};

export default VoucherPreviewModal;
