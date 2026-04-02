import { useState } from 'react';
import { PharmacyMapper } from './components/PharmacyMapper.jsx';
import { MedicalMapper } from './components/MedicalMapper.jsx';
import { EligibilityMapper } from './components/EligibilityMapper.jsx';
import { GLOBAL_STYLE, MODE_META, S } from './styles/scrubStyles.js';

export default function PharmacyMapperApp() {
  const [mode, setMode] = useState('pharmacy');

  return (
    <div style={S.app}>
      <style>{GLOBAL_STYLE}</style>

      <div style={{ padding: '14px 24px', borderBottom: '1px solid #1f2937', background: '#0f1524', display: 'flex' }}>
        <div style={S.modeSwitcher}>
          {Object.keys(MODE_META).map(k => (
            <button key={k} type="button" style={S.modeBtn(mode === k, k)} onClick={() => setMode(k)}>
              <span>{MODE_META[k].icon}</span>
              <span>{MODE_META[k].label}</span>
            </button>
          ))}
        </div>
      </div>

      {mode === 'pharmacy' && <PharmacyMapper />}
      {mode === 'medical' && <MedicalMapper />}
      {mode === 'eligibility' && <EligibilityMapper />}
    </div>
  );
}
