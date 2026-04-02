import { useRef } from 'react';
import { ScrubBuilder } from './ScrubBuilder.jsx';
import { MODE_META, S } from '../styles/scrubStyles.js';

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

        <div style={{ display: 'flex', gap: 10, alignItems: 'center' }}>
          <button type="button" style={S.btn('ghost')} onClick={() => scrubRef.current?.reset()}>
            Reset to Default
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
