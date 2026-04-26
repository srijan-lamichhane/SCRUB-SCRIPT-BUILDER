import { TYPE_ORDER } from '../styles/scrubStyles.js';

const ALLOWED_TYPES = new Set(TYPE_ORDER);

/**
 * Merge imported mapping rows into the current template by `target`.
 * Rejects unknown targets, duplicate targets, and invalid types.
 *
 * @param {Array<{ target: string, type: string, value?: string, insertExpr?: string }>} currentMaps
 * @param {unknown} importedRows
 * @returns {{ ok: true, maps: typeof currentMaps, appliedCount: number } | { ok: false, error: string }}
 */
export function mergeMappingsFromJson(currentMaps, importedRows) {
  if (!Array.isArray(importedRows)) {
    return {
      ok: false,
      error:
        'JSON must be an array of objects. Each object needs target, type, and value (same shape as mapping presets).',
    };
  }

  const allowedTargets = new Set(currentMaps.map((m) => m.target));
  const seen = new Set();
  /** @type {Map<string, { type: string, value: string, insertExpr?: string }>} */
  const byTarget = new Map();

  for (let i = 0; i < importedRows.length; i++) {
    const row = importedRows[i];
    if (!row || typeof row !== 'object' || Array.isArray(row)) {
      return { ok: false, error: `Entry at index ${i} must be a plain object (not an array).` };
    }

    const target = row.target;
    if (typeof target !== 'string' || !target.trim()) {
      return { ok: false, error: `Entry at index ${i} is missing a non-empty string "target".` };
    }

    if (!allowedTargets.has(target)) {
      return {
        ok: false,
        error: `Unknown target field: "${target}". Every target in the file must exist in this mode's mapping table.`,
      };
    }

    if (seen.has(target)) {
      return { ok: false, error: `Duplicate target in file: "${target}".` };
    }
    seen.add(target);

    const type = row.type;
    if (!ALLOWED_TYPES.has(type)) {
      return {
        ok: false,
        error: `Invalid type for "${target}": "${String(type)}". Use one of: ${TYPE_ORDER.join(', ')}.`,
      };
    }

    const value = row.value != null ? String(row.value) : '';
    const templateRow = currentMaps.find((m) => m.target === target);
    /** @type {{ type: string, value: string, insertExpr?: string }} */
    const patch = { type, value };
    if (templateRow && Object.prototype.hasOwnProperty.call(templateRow, 'insertExpr')) {
      patch.insertExpr = row.insertExpr != null ? String(row.insertExpr) : '';
    }
    byTarget.set(target, patch);
  }

  const maps = currentMaps.map((m) => {
    const imp = byTarget.get(m.target);
    if (!imp) return { ...m };
    const next = { ...m, type: imp.type, value: imp.value };
    if ('insertExpr' in m && imp.insertExpr !== undefined) {
      next.insertExpr = imp.insertExpr;
    }
    return next;
  });

  return { ok: true, maps, appliedCount: byTarget.size };
}
