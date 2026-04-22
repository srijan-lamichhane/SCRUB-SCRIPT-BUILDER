import { useEffect, useMemo, useRef, useState } from 'react';
import { ScrubBuilder } from './ScrubBuilder.jsx';
import {
  VENDOR_PRESETS,
  PHARMACY_VENDOR_ORDER,
  MEDICAL_VENDOR_ORDER,
  ELIGIBILITY_VENDOR_ORDER,
} from '../constants/vendorPresets.js';
import { MODE_META, S } from '../styles/scrubStyles.js';

function getModeVendors(mode) {
  const order =
    mode === 'pharmacy'
      ? PHARMACY_VENDOR_ORDER
      : mode === 'medical'
        ? MEDICAL_VENDOR_ORDER
        : ELIGIBILITY_VENDOR_ORDER;
  const bucket =
    mode === 'pharmacy'
      ? VENDOR_PRESETS.pharmacy
      : mode === 'medical'
        ? VENDOR_PRESETS.medical
        : VENDOR_PRESETS.eligibility;
  return { order, bucket };
}

function PresetSelector({ mode, scrubRef, accent }) {
  const { order, bucket } = useMemo(() => getModeVendors(mode), [mode]);
  const [selectedPreset, setSelectedPreset] = useState(order[0] ?? '');

  useEffect(() => {
    setSelectedPreset(order[0] ?? '');
  }, [order]);

  const handleApplyPreset = () => {
    if (!selectedPreset || !bucket[selectedPreset]) return;
    const label = bucket[selectedPreset].label;
    const confirmed = window.confirm(
      `Load ${label} preset? This will overwrite current raw fields and mappings for ${MODE_META[mode].label}.`
    );
    if (!confirmed) return;
    scrubRef.current?.applyVendorPreset(selectedPreset);
  };

  return (
    <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
      <select
        value={selectedPreset}
        onChange={(e) => setSelectedPreset(e.target.value)}
        style={{
          ...S.typeSelect,
          minWidth: 140,
          borderColor: `${accent}55`,
          color: '#e2e8f0',
        }}
      >
        {order.map((id) => (
          <option key={id} value={id}>
            {bucket[id].label}
          </option>
        ))}
      </select>
      <button
        type="button"
        style={{
          ...S.btn('ghost'),
          borderColor: `${accent}55`,
          color: accent,
        }}
        onClick={handleApplyPreset}
        disabled={!selectedPreset}
      >
        Load Preset
      </button>
    </div>
  );
}

export function MapperPanel({ mode }) {
  const m = MODE_META[mode];
  const scrubRef = useRef(null);
  const hasPresets = getModeVendors(mode).order.length > 0;

  const handleClearLayout = () => {
    const confirmed = window.confirm(
      `Clear current ${m.label} layout? This will remove raw fields and reset mappings to NULL.`
    );
    if (!confirmed) return;
    scrubRef.current?.clearLayout();
  };

  return (
    <>
      <header style={S.header}>
        <div style={S.logo}>
          <div style={{ ...S.logoIcon, background: `${m.color}22`, border: `1px solid ${m.border}` }}>
            {m.icon}
          </div>
          <div>
            <div style={{ fontSize: 15, fontWeight: 700, letterSpacing: '-0.3px' }}>
              {m.label} Scrub Builder
            </div>
            <div style={{ fontSize: 11, color: '#6b7280', letterSpacing: '0.5px' }}>
              SNOWFLAKE SP GENERATOR - {mode.toUpperCase()} CLAIMS
            </div>
          </div>
        </div>

        <div style={{ display: 'flex', gap: 10, alignItems: 'center', flexWrap: 'wrap', justifyContent: 'flex-end' }}>
        {hasPresets && <PresetSelector mode={mode} scrubRef={scrubRef} accent={m.color} />}
          <button type="button" style={{...S.btn('ghost'), 
          borderColor: '#FF7F7F',
          color: '#FF7F7F'}} 
          onClick={handleClearLayout}>
            Clear layout
          </button>
          
          <button
            type="button"
            style={{ ...S.btn('primary'), background: m.color }}
            onClick={() => scrubRef.current?.generate()}
          >
            Generate SQL
          </button>
        </div>
      </header>

      <ScrubBuilder ref={scrubRef} mode={mode} accent={m.color} />
    </>
  );
}
