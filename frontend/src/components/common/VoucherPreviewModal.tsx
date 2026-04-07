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
  const [bookedDate, setBookedDate] = useState("");
  const getCurrentDateString = (): string => {
    const now = new Date();
    const year = now.getFullYear();
    const month = String(now.getMonth() + 1).padStart(2, "0");
    const day = String(now.getDate()).padStart(2, "0");
    return `${year}-${month}-${day}`;
  };
  const isValidDateString = (value: string): boolean =>
    /^\d{4}-\d{2}-\d{2}$/.test(String(value || "").trim());
  const toPeriodNumber = (value: string): string =>
    isValidDateString(value) ? value.replace(/-/g, "").slice(0, 6) : "";
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
  type DecimalValue = { int: bigint; scale: number };
  const ZERO_DECIMAL: DecimalValue = { int: 0n, scale: 0 };
  const parseDecimal = (value: any): DecimalValue => {
    const raw = String(value ?? "").replace(/,/g, "").trim();
    if (!raw) return ZERO_DECIMAL;

    const sign = raw.startsWith("-") ? -1n : 1n;
    const normalized = raw.replace(/^[+-]/, "");
    if (!/^\d*(\.\d*)?$/.test(normalized)) return ZERO_DECIMAL;

    const [intPartRaw = "0", fracPartRaw = ""] = normalized.split(".");
    const intPart = intPartRaw.replace(/^0+(?=\d)/, "") || "0";
    const fracPart = fracPartRaw.replace(/0+$/, "");
    const digits = `${intPart}${fracPart}`.replace(/^0+(?=\d)/, "") || "0";
    return { int: BigInt(digits) * sign, scale: fracPart.length };
  };
  const alignDecimal = (
    left: DecimalValue,
    right: DecimalValue,
  ): [bigint, bigint, number] => {
    const scale = Math.max(left.scale, right.scale);
    const leftFactor = 10n ** BigInt(scale - left.scale);
    const rightFactor = 10n ** BigInt(scale - right.scale);
    return [left.int * leftFactor, right.int * rightFactor, scale];
  };
  const addDecimal = (left: DecimalValue, right: DecimalValue): DecimalValue => {
    const [l, r, scale] = alignDecimal(left, right);
    return { int: l + r, scale };
  };
  const subtractDecimal = (
    left: DecimalValue,
    right: DecimalValue,
  ): DecimalValue => {
    const [l, r, scale] = alignDecimal(left, right);
    return { int: l - r, scale };
  };
  const compareDecimal = (left: DecimalValue, right: DecimalValue): number => {
    const [l, r] = alignDecimal(left, right);
    if (l === r) return 0;
    return l > r ? 1 : -1;
  };
  const absDecimal = (value: DecimalValue): DecimalValue => ({
    int: value.int < 0n ? -value.int : value.int,
    scale: value.scale,
  });
  const truncateDecimalToScale = (
    value: DecimalValue,
    scale: number,
  ): DecimalValue => {
    const normalizedScale = Math.max(scale, 0);
    if (value.scale <= normalizedScale) {
      return {
        int: value.int * 10n ** BigInt(normalizedScale - value.scale),
        scale: normalizedScale,
      };
    }
    const divisor = 10n ** BigInt(value.scale - normalizedScale);
    return {
      int: value.int / divisor,
      scale: normalizedScale,
    };
  };
  const roundDecimalToScale = (
    value: DecimalValue,
    scale: number,
  ): DecimalValue => {
    const normalizedScale = Math.max(scale, 0);
    if (value.scale <= normalizedScale) {
      return {
        int: value.int * 10n ** BigInt(normalizedScale - value.scale),
        scale: normalizedScale,
      };
    }
    const negative = value.int < 0n;
    const absInt = negative ? -value.int : value.int;
    const divisor = 10n ** BigInt(value.scale - normalizedScale);
    let quotient = absInt / divisor;
    const remainder = absInt % divisor;
    if (remainder * 2n >= divisor) {
      quotient += 1n;
    }
    return {
      int: negative ? -quotient : quotient,
      scale: normalizedScale,
    };
  };
  const decimalToString = (value: DecimalValue): string => {
    if (value.int === 0n) return "0";
    const negative = value.int < 0n;
    const absText = (negative ? -value.int : value.int).toString();
    if (value.scale === 0) return `${negative ? "-" : ""}${absText}`;
    const zeroPad = Math.max(value.scale - absText.length + 1, 0);
    const padded = `${"0".repeat(zeroPad)}${absText}`;
    const splitAt = padded.length - value.scale;
    const intPart = padded.slice(0, splitAt) || "0";
    const fracPart = padded.slice(splitAt).replace(/0+$/, "");
    if (!fracPart) return `${negative ? "-" : ""}${intPart}`;
    return `${negative ? "-" : ""}${intPart}.${fracPart}`;
  };
  const decimalToFixedString = (value: DecimalValue, scale: number): string => {
    const normalizedScale = Math.max(scale, 0);
    const text = decimalToString(value);
    const negative = text.startsWith("-");
    const unsigned = negative ? text.slice(1) : text;
    const [intPartRaw = "0", fracPartRaw = ""] = unsigned.split(".");
    const intPart = intPartRaw || "0";
    if (normalizedScale === 0) {
      return `${negative ? "-" : ""}${intPart}`;
    }
    const fracPart = fracPartRaw.padEnd(normalizedScale, "0").slice(0, normalizedScale);
    return `${negative ? "-" : ""}${intPart}.${fracPart}`;
  };
  const decimalToNumber = (value: DecimalValue): number =>
    Number(decimalToString(value));
  const decimalIsPositive = (value: DecimalValue): boolean => value.int > 0n;
  const getEntryDebitValue = (entry: any) => entry?.debit_exact ?? entry?.debit;
  const getEntryCreditValue = (entry: any) => entry?.credit_exact ?? entry?.credit;
  const getEntryDebitMergeValue = (entry: any) =>
    entry?.debit_formula_exact ?? entry?.debit_exact ?? entry?.debit;
  const getEntryCreditMergeValue = (entry: any) =>
    entry?.credit_formula_exact ?? entry?.credit_exact ?? entry?.credit;
  const getEntryLocalRateValue = (entry: any) => entry?.localrate_exact ?? entry?.localrate ?? "1";
  const getMoneyString = (value: any): string => decimalToFixedString(parseDecimal(value), 2);
  const getAmountDisplay = (value: any): string => getMoneyString(value);
  const hasPositiveAmount = (value: any): boolean => parseDecimal(value).int > 0n;
  const buildKingdeeJsonFromEntries = (entries: any[], nextBookedDate: string) => {
    if (!kdJson?.data?.[0]) return kdJson;
    const header = kdJson.data[0];
    const mappedEntries = entries.map((entry: any, idx: number) => {
      const debit = getMoneyString(getEntryDebitValue(entry));
      const credit = getMoneyString(getEntryCreditValue(entry));
      const kdEntry: any = {
        seq: idx + 1,
        edescription: entry.summary || "",
        account_number: entry.account_code || "",
        currency_number: entry.currency || "CNY",
        localrate: decimalToString(parseDecimal(getEntryLocalRateValue(entry))),
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
          bookeddate: nextBookedDate,
          period_number: toPeriodNumber(nextBookedDate),
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
  const skippedBills = Array.isArray(data?.skipped_bills)
    ? data.skipped_bills
    : [];
  const strictMatchBlocked =
    Boolean(data?.partial_matched) || skippedBills.length > 0;
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
  const normalizeEntryText = (...parts: any[]): string =>
    parts
      .map((part) => String(part || ""))
      .join("")
      .replace(/\s+/g, "")
      .trim()
      .toLowerCase();
  const isProfitLossEntry = (entry: any): boolean => {
    const accountCode = String(entry?.account_code || "").trim();
    const accountTypeNumber = String(entry?.account_type_number || "")
      .trim()
      .toLowerCase();
    const accountText = `${String(entry?.account_name || "")}${String(
      entry?.account_display || "",
    )}`
      .replace(/\s+/g, "")
      .toLowerCase();

    if (
      accountCode.startsWith("4103") ||
      accountText.includes("\u672c\u5e74\u5229\u6da6")
    ) {
      return false;
    }

    if (
      accountTypeNumber.includes("\u635f\u76ca") ||
      accountTypeNumber.includes("profit") ||
      accountTypeNumber.includes("loss")
    ) {
      return true;
    }

    if (/^(6|7)\d*$/.test(accountTypeNumber)) {
      return true;
    }

    if (/^(6|7)/.test(accountCode)) {
      return true;
    }

    return [
      "\u6536\u5165",
      "\u6210\u672c",
      "\u8d39\u7528",
      "\u635f\u76ca",
      "\u6536\u76ca",
      "\u7a0e\u91d1\u53ca\u9644\u52a0",
      "\u8425\u4e1a\u5916",
      "\u6240\u5f97\u7a0e",
    ].some((keyword) => accountText.includes(keyword));
  };
  const isCarryForwardEntry = (entry: any): boolean => {
    const summary = normalizeEntryText(entry?.summary, entry?.edescription);
    const accountCode = String(entry?.account_code || "").trim();
    const accountText = normalizeEntryText(
      entry?.account_name,
      entry?.account_display,
    );

    if (!summary && !accountCode && !accountText) return false;

    if (
      [
        "结转",
        "转结",
        "结平",
        "结清",
        "损益结转",
        "期末结转",
        "月末结转",
        "年末结转",
      ].some((keyword) => summary.includes(keyword))
    ) {
      return true;
    }

    if (accountCode.startsWith("4103") || accountText.includes("本年利润")) {
      return true;
    }

    const periodHint = ["期末", "月末", "年末", "月底", "关账"].some((keyword) =>
      summary.includes(keyword),
    );
    if (periodHint && isProfitLossEntry(entry)) {
      return true;
    }

    return false;
  };
  const isTaxEntry = (entry: any): boolean => {
    const accountCode = String(entry?.account_code || "").trim();
    const summary = normalizeEntryText(entry?.summary, entry?.edescription);
    return (
      accountCode.startsWith("2221") ||
      summary.includes("增值税") ||
      summary.includes("税费") ||
      summary.includes("销项税")
    );
  };
  const getEntryBusinessSortBucket = (entry: any): number =>
    isCarryForwardEntry(entry) ? 1 : 0;
  const normalizeMergedDirectionAmounts = (
    entries: any[],
    exactKey: "debit_decimal" | "credit_decimal",
    roundedExactKey: "debit_exact" | "credit_exact",
    numberKey: "debit" | "credit",
  ) => {
    const indexed = entries
      .map((entry, idx) => ({
        entry,
        idx,
        exact: entry?.[exactKey] || ZERO_DECIMAL,
      }))
      .filter((item) => item.exact.int > 0n);

    if (indexed.length === 0) {
      entries.forEach((entry) => {
        entry[roundedExactKey] = "0.00";
        entry[numberKey] = 0;
      });
      return;
    }

    const targetTotal = roundDecimalToScale(
      indexed.reduce(
        (sum: DecimalValue, item) => addDecimal(sum, item.exact),
        ZERO_DECIMAL,
      ),
      2,
    );

    const baseValues = indexed.map((item) =>
      truncateDecimalToScale(item.exact, 2),
    );
    let allocatedTotal = baseValues.reduce(
      (sum: DecimalValue, item) => addDecimal(sum, item),
      ZERO_DECIMAL,
    );
    const oneCent: DecimalValue = { int: 1n, scale: 2 };
    let deltaCents = Number(
      subtractDecimal(targetTotal, allocatedTotal).int,
    );

    if (deltaCents > 0) {
      const buildCandidateOrder = (positiveOnly: boolean) =>
        indexed
          .map((item, idx) => ({
            idx,
            isTax: isTaxEntry(item.entry),
            magnitude: absDecimal(item.exact),
            remainder: subtractDecimal(item.exact, baseValues[idx]),
          }))
          .filter((item) =>
            positiveOnly ? compareDecimal(item.remainder, ZERO_DECIMAL) > 0 : true,
          )
          .sort((left, right) => {
            if (left.isTax !== right.isTax) {
              return Number(left.isTax) - Number(right.isTax);
            }
            const magnitudeDiff = compareDecimal(right.magnitude, left.magnitude);
            if (magnitudeDiff !== 0) return magnitudeDiff;
            const remainderDiff = compareDecimal(right.remainder, left.remainder);
            if (remainderDiff !== 0) return remainderDiff;
            return left.idx - right.idx;
          });

      const candidateOrder =
        buildCandidateOrder(true).length > 0
          ? buildCandidateOrder(true)
          : buildCandidateOrder(false);

      for (let i = 0; i < deltaCents; i += 1) {
        const target = candidateOrder[i % candidateOrder.length];
        baseValues[target.idx] = addDecimal(baseValues[target.idx], oneCent);
      }
      allocatedTotal = baseValues.reduce(
        (sum: DecimalValue, item) => addDecimal(sum, item),
        ZERO_DECIMAL,
      );
    }

    entries.forEach((entry) => {
      entry[roundedExactKey] = "0.00";
      entry[numberKey] = 0;
    });

    indexed.forEach((item, idx) => {
      item.entry[roundedExactKey] = decimalToFixedString(baseValues[idx], 2);
      item.entry[numberKey] = decimalToNumber(baseValues[idx]);
    });
  };
  const mergeEntries = (entries: any[]) => {
    const merged: any[] = [];
    const indexMap = new Map<string, number>();
    entries.forEach((entry, sourceIndex) => {
      const debitDecimal = parseDecimal(getEntryDebitMergeValue(entry));
      const creditDecimal = parseDecimal(getEntryCreditMergeValue(entry));
      const direction = decimalIsPositive(debitDecimal) ? "debit" : "credit";
      const accountKey = `${entry.account_display || ""}|${entry.account_code || ""}`;
      const assgrpKey = JSON.stringify(normalizeAssgrp(entry.assgrp));
      const businessSortBucket = getEntryBusinessSortBucket(entry);
      const sourceLineNo = Number(entry.line_no || sourceIndex + 1);
      const key = `${direction}|${accountKey}|${assgrpKey}`;
      if (indexMap.has(key)) {
        const idx = indexMap.get(key) as number;
        const target = merged[idx];
        target.debit_decimal = addDecimal(
          target.debit_decimal || ZERO_DECIMAL,
          debitDecimal,
        );
        target.credit_decimal = addDecimal(
          target.credit_decimal || ZERO_DECIMAL,
          creditDecimal,
        );
        target.source_line_no_min = Math.min(
          Number(target.source_line_no_min || sourceLineNo),
          sourceLineNo,
        );
        target.source_index_min = Math.min(
          Number(target.source_index_min || sourceIndex),
          sourceIndex,
        );
        target.business_sort_bucket = Math.min(
          Number(target.business_sort_bucket || businessSortBucket),
          businessSortBucket,
        );
        return;
      }
      indexMap.set(key, merged.length);
      merged.push({
        ...entry,
        debit_decimal: debitDecimal,
        credit_decimal: creditDecimal,
        localrate_exact: decimalToString(parseDecimal(getEntryLocalRateValue(entry))),
        localrate: decimalToNumber(parseDecimal(getEntryLocalRateValue(entry))),
        source_line_no_min: sourceLineNo,
        source_index_min: sourceIndex,
        business_sort_bucket: businessSortBucket,
      });
    });
    const sortedMerged = [...merged].sort((a, b) => {
      const bucketDiff =
        Number(a.business_sort_bucket || 0) - Number(b.business_sort_bucket || 0);
      if (bucketDiff !== 0) return bucketDiff;

      if (Number(a.business_sort_bucket || 0) === 1) {
        const profitLossDiff =
          Number(isProfitLossEntry(a)) - Number(isProfitLossEntry(b));
        if (profitLossDiff !== 0) return profitLossDiff;
      }

      const sourceLineDiff =
        Number(a.source_line_no_min || 0) - Number(b.source_line_no_min || 0);
      if (sourceLineDiff !== 0) return sourceLineDiff;

      const sourceIndexDiff =
        Number(a.source_index_min || 0) - Number(b.source_index_min || 0);
      if (sourceIndexDiff !== 0) return sourceIndexDiff;

      const summaryDiff = String(a.summary || "").localeCompare(
        String(b.summary || ""),
        "zh-CN",
        { numeric: true },
      );
      if (summaryDiff !== 0) return summaryDiff;

      const accountDiff = String(a.account_code || "").localeCompare(
        String(b.account_code || ""),
        "zh-CN",
        { numeric: true },
      );
      if (accountDiff !== 0) return accountDiff;

      return JSON.stringify(normalizeAssgrp(a.assgrp)).localeCompare(
        JSON.stringify(normalizeAssgrp(b.assgrp)),
        "zh-CN",
        { numeric: true },
      );
    });

    const normalizedMerged = sortedMerged.map((entry, idx) => ({
      ...entry,
      debit_formula_exact: decimalToString(entry.debit_decimal || ZERO_DECIMAL),
      credit_formula_exact: decimalToString(entry.credit_decimal || ZERO_DECIMAL),
      debit_exact: decimalToFixedString(entry.debit_decimal || ZERO_DECIMAL, 2),
      credit_exact: decimalToFixedString(entry.credit_decimal || ZERO_DECIMAL, 2),
      debit: decimalToNumber(entry.debit_decimal || ZERO_DECIMAL),
      credit: decimalToNumber(entry.credit_decimal || ZERO_DECIMAL),
      line_no: idx + 1,
    }));

    normalizeMergedDirectionAmounts(
      normalizedMerged,
      "debit_decimal",
      "debit_exact",
      "debit",
    );
    normalizeMergedDirectionAmounts(
      normalizedMerged,
      "credit_decimal",
      "credit_exact",
      "credit",
    );

    return normalizedMerged;
  };
  const displayedEntries = useMemo(() => {
    if (!acctView?.entries) return [];
    return mergeEnabled ? mergeEntries(acctView.entries) : acctView.entries;
  }, [acctView?.entries, mergeEnabled]);
  const displayedDebitTotal = useMemo(
    () =>
      displayedEntries.reduce(
        (sum: DecimalValue, entry: any) =>
          addDecimal(sum, parseDecimal(getEntryDebitValue(entry))),
        ZERO_DECIMAL,
      ),
    [displayedEntries],
  );
  const displayedCreditTotal = useMemo(
    () =>
      displayedEntries.reduce(
        (sum: DecimalValue, entry: any) =>
          addDecimal(sum, parseDecimal(getEntryCreditValue(entry))),
        ZERO_DECIMAL,
      ),
    [displayedEntries],
  );
  const accountingViewBalanced = useMemo(
    () => compareDecimal(displayedDebitTotal, displayedCreditTotal) === 0,
    [displayedDebitTotal, displayedCreditTotal],
  );
  const accountingViewDiffDisplay = useMemo(
    () =>
      decimalToFixedString({
        int: alignDecimal(displayedDebitTotal, displayedCreditTotal)[0] -
          alignDecimal(displayedDebitTotal, displayedCreditTotal)[1],
        scale: Math.max(displayedDebitTotal.scale, displayedCreditTotal.scale),
      }, 2),
    [displayedDebitTotal, displayedCreditTotal],
  );
  const bookedDateValidation = useMemo(() => {
    const normalized = String(bookedDate || "").trim();
    if (!normalized) {
      return { ok: false, message: "记账日期不能为空" };
    }
    if (!isValidDateString(normalized)) {
      return { ok: false, message: "记账日期格式不正确" };
    }
    return { ok: true, message: "" };
  }, [bookedDate]);
  const effectiveKingdeeJson = useMemo(() => {
    if (!acctView?.entries || !kdJson) return kdJson;
    return buildKingdeeJsonFromEntries(displayedEntries, bookedDate);
  }, [acctView?.entries, bookedDate, kdJson, displayedEntries]);
  const voucherAmountValidation = useMemo(() => {
    if (!effectiveKingdeeJson?.data?.[0]?.entries) {
      return { ok: true, message: "" };
    }

    const jsonEntries = effectiveKingdeeJson.data[0].entries as any[];
    const displayedDebit = displayedDebitTotal;
    const displayedCredit = displayedCreditTotal;
    const jsonDebit = jsonEntries.reduce(
      (sum: DecimalValue, entry: any) =>
        addDecimal(sum, parseDecimal(entry.debitori)),
      ZERO_DECIMAL,
    );
    const jsonCredit = jsonEntries.reduce(
      (sum: DecimalValue, entry: any) =>
        addDecimal(sum, parseDecimal(entry.creditori)),
      ZERO_DECIMAL,
    );
    const jsonLocalDebit = jsonEntries.reduce(
      (sum: DecimalValue, entry: any) =>
        addDecimal(sum, parseDecimal(entry.debitlocal)),
      ZERO_DECIMAL,
    );
    const jsonLocalCredit = jsonEntries.reduce(
      (sum: DecimalValue, entry: any) =>
        addDecimal(sum, parseDecimal(entry.creditlocal)),
      ZERO_DECIMAL,
    );

    if (compareDecimal(jsonDebit, jsonCredit) !== 0) {
      return { ok: false, message: "JSON 借贷金额不平衡，已阻止推送。" };
    }

    if (compareDecimal(jsonLocalDebit, jsonLocalCredit) !== 0) {
      return { ok: false, message: "JSON 本位币借贷金额不平衡，已阻止推送。" };
    }

    if (
      compareDecimal(jsonDebit, displayedDebit) !== 0 ||
      compareDecimal(jsonCredit, displayedCredit) !== 0
    ) {
      return { ok: false, message: "JSON 金额与当前凭证视图不一致，已阻止推送。" };
    }

    return { ok: true, message: "" };
  }, [displayedCreditTotal, displayedDebitTotal, effectiveKingdeeJson]);
  const canPush =
    !!onPushVoucher &&
    !!effectiveKingdeeJson &&
    !isLoading &&
    !error &&
    data?.matched &&
    !strictMatchBlocked &&
    !pushBlocked &&
    bookedDateValidation.ok &&
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
      setBookedDate(getCurrentDateString());
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
        {/* Header */}
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
            {!!effectiveKingdeeJson &&
              !isLoading &&
              !error &&
              data?.matched &&
              !strictMatchBlocked && (
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
              data?.matched &&
              !strictMatchBlocked && (
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
        {/* Content */}
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
          ) : data && (!data.matched || strictMatchBlocked) ? (
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
              {strictMatchBlocked ? (
                <>
                  <p style={{ margin: 0, fontWeight: 600 }}>
                    存在未完整匹配的关联数据，已阻止生成和推送
                  </p>
                  <p
                    style={{
                      fontSize: "0.8rem",
                      color: "#94a3b8",
                      textAlign: "center",
                    }}
                  >
                    {data?.message ||
                      "当前收款单存在未匹配或无法合并的关联数据，必须全部成功匹配后才能继续。"}
                  </p>
                  {skippedBills.length > 0 && (
                    <div
                      style={{
                        width: "100%",
                        maxWidth: "760px",
                        padding: "0.9rem 1rem",
                        borderRadius: "0.75rem",
                        border: "1px solid #fed7aa",
                        background: "#fff7ed",
                        color: "#9a3412",
                      }}
                    >
                      <div
                        style={{
                          fontSize: "0.78rem",
                          fontWeight: 700,
                          marginBottom: "0.5rem",
                        }}
                      >
                        未通过校验的关联数据
                      </div>
                      <div
                        style={{
                          display: "flex",
                          flexDirection: "column",
                          gap: "0.35rem",
                          fontSize: "0.76rem",
                          lineHeight: 1.5,
                        }}
                      >
                        {skippedBills.slice(0, 10).map((item: any, index: number) => (
                          <div
                            key={`${item?.community_id ?? "0"}-${item?.bill_id ?? index}-${index}`}
                          >
                            {`${item?.community_id ?? "-"}:${item?.bill_id ?? "-"} - ${
                              item?.reason || "template not matched"
                            }`}
                          </div>
                        ))}
                        {skippedBills.length > 10 && (
                          <div>{`另有 ${skippedBills.length - 10} 条未展示`}</div>
                        )}
                      </div>
                    </div>
                  )}
                </>
              ) : (
                <>
                  <p style={{ margin: 0, fontWeight: 600 }}>未匹配到适用模板</p>
                  <p
                    style={{
                      fontSize: "0.8rem",
                      color: "#94a3b8",
                      textAlign: "center",
                    }}
                  >
                    已检查 {data.templates_checked} 个模板，没有找到符合当前账单条件的凭证模板。
                    <br /> 请前往“凭证模板中心”创建或调整模板的触发条件。
                  </p>
                </>
              )}
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
              {!bookedDateValidation.ok && (
                <div
                  style={{
                    marginBottom: "0.75rem",
                    padding: "0.5rem 0.75rem",
                    borderRadius: "0.5rem",
                    fontSize: "0.75rem",
                    fontWeight: 600,
                    background: "#fff7ed",
                    color: "#9a3412",
                    border: "1px solid #fdba74",
                  }}
                >
                  {bookedDateValidation.message}
                </div>
              )}
              <div
                style={{
                  marginBottom: "0.75rem",
                  padding: "0.75rem",
                  borderRadius: "0.75rem",
                  background: "#f8fafc",
                  border: "1px solid #e2e8f0",
                  display: "flex",
                  flexWrap: "wrap",
                  gap: "0.75rem 1rem",
                  alignItems: "end",
                }}
              >
                <label
                  style={{
                    display: "flex",
                    flexDirection: "column",
                    gap: "0.35rem",
                    minWidth: "220px",
                  }}
                >
                  <span
                    style={{
                      fontSize: "0.78rem",
                      fontWeight: 700,
                      color: "#0f172a",
                    }}
                  >
                    记账日期
                  </span>
                  <input
                    type="date"
                    required
                    value={bookedDate}
                    onChange={(e) => setBookedDate(e.target.value)}
                    style={{
                      height: "2.3rem",
                      padding: "0 0.75rem",
                      borderRadius: "0.6rem",
                      border: `1px solid ${bookedDateValidation.ok ? "#cbd5e1" : "#fb923c"}`,
                      outline: "none",
                      fontSize: "0.84rem",
                      color: "#0f172a",
                      background: "#fff",
                    }}
                  />
                </label>
                <div
                  style={{
                    display: "flex",
                    flexDirection: "column",
                    gap: "0.35rem",
                    minWidth: "180px",
                  }}
                >
                  <span
                    style={{
                      fontSize: "0.78rem",
                      fontWeight: 700,
                      color: "#475569",
                    }}
                  >
                    所属会计期间
                  </span>
                  <div
                    style={{
                      height: "2.3rem",
                      padding: "0 0.75rem",
                      borderRadius: "0.6rem",
                      border: "1px solid #e2e8f0",
                      background: "#fff",
                      display: "flex",
                      alignItems: "center",
                      fontFamily: monoFont,
                      fontSize: "0.84rem",
                      color: "#0f172a",
                    }}
                  >
                    {toPeriodNumber(bookedDate) || "-"}
                  </div>
                </div>
                <div
                  style={{
                    fontSize: "0.75rem",
                    color: "#64748b",
                    lineHeight: 1.6,
                  }}
                >
                  该日期将写入 JSON 的 `bookeddate`，并同步生成 `period_number`
                </div>
              </div>
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
                        {sourceBillSummary.success || 0} / 失败 {sourceBillSummary.failed || 0}
                        
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
                  {/* Balance Status */}
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
                      background: accountingViewBalanced ? "#f0fdf4" : "#fef2f2",
                      color: accountingViewBalanced ? "#166534" : "#991b1b",
                      border: `1px solid ${accountingViewBalanced ? "#bbf7d0" : "#fecaca"}`,
                      flexShrink: 0,
                    }}
                  >
                    {accountingViewBalanced ? (
                      <>
                        <CheckCircle2 size={14} /> 借贷平衡
                      </>
                    ) : (
                      <>
                        <AlertTriangle size={14} /> 借贷不平衡，差额：¥
                        {accountingViewDiffDisplay}
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
                                color: hasPositiveAmount(getEntryDebitValue(entry)) ? "#0f172a" : "#cbd5e1",
                              }}
                            >
                              {hasPositiveAmount(getEntryDebitValue(entry))
                                ? `¥${getAmountDisplay(getEntryDebitValue(entry))}`
                                : "-"}
                            </td>
                            <td
                              style={{
                                ...tdStyle,
                                textAlign: "right",
                                fontWeight: 600,
                                fontFamily: monoFont,
                                fontSize: "0.82rem",
                                color: hasPositiveAmount(getEntryCreditValue(entry)) ? "#0f172a" : "#cbd5e1",
                              }}
                            >
                              {hasPositiveAmount(getEntryCreditValue(entry))
                                ? `¥${getAmountDisplay(getEntryCreditValue(entry))}`
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
                            ¥{decimalToFixedString(displayedDebitTotal, 2)}
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
                            ¥{decimalToFixedString(displayedCreditTotal, 2)}
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
      {/* keyframes */}
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
