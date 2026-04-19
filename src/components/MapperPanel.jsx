import { useRef } from 'react';
import { ScrubBuilder } from './ScrubBuilder.jsx';
import {
  VENDOR_PRESETS,
  PHARMACY_VENDOR_ORDER,
  MEDICAL_VENDOR_ORDER,
  ELIGIBILITY_VENDOR_ORDER,
} from '../constants/vendorPresets.js';
import { MODE_META, S } from '../styles/scrubStyles.js';

function VendorButtons({ mode, scrubRef, accent }) {
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

  return (
    <>
      {order.map(id => (
        <button
          key={id}
          type="button"
          style={{
            ...S.btn('ghost'),
            borderColor: `${accent}55`,
            color: accent,
          }}
          onClick={() => scrubRef.current?.applyVendorPreset(id)}
        >
          {bucket[id].label}
        </button>
      ))}
    </>
  );
}

export function MapperPanel({ mode }) {
  const m = MODE_META[mode];
  const scrubRef = useRef(null);

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
          <button type="button" style={S.btn('ghost')} onClick={() => scrubRef.current?.clearLayout()}>
            Clear layout
          </button>
          <VendorButtons mode={mode} scrubRef={scrubRef} accent={m.color} />
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
