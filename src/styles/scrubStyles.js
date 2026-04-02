export const GLOBAL_STYLE = `
  @import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Sora:wght@400;500;600;700&display=swap');
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: #0b0f1a; }
  ::-webkit-scrollbar { width: 6px; height: 6px; }
  ::-webkit-scrollbar-track { background: #111827; }
  ::-webkit-scrollbar-thumb { background: #2d3748; border-radius: 3px; }
  ::-webkit-scrollbar-thumb:hover { background: #4a5568; }
`;

export const TYPE_META = {
  null:       {label:'NULL',       color:'#ef4444', bg:'rgba(239,68,68,0.12)',   border:'rgba(239,68,68,0.3)'},
  direct:     {label:'DIRECT',     color:'#22c55e', bg:'rgba(34,197,94,0.12)',   border:'rgba(34,197,94,0.3)'},
  hardcoded:  {label:'HARDCODED',  color:'#f59e0b', bg:'rgba(245,158,11,0.12)', border:'rgba(245,158,11,0.3)'},
  expression: {label:'EXPRESSION', color:'#a855f7', bg:'rgba(168,85,247,0.12)', border:'rgba(168,85,247,0.3)'},
};
export const TYPE_ORDER = ['null','direct','hardcoded','expression'];

export const MODE_META = {
  pharmacy: { label:'Pharmacy', icon:'💊', color:'#3b82f6', bg:'rgba(59,130,246,0.15)', border:'rgba(59,130,246,0.4)' },
  medical:  { label:'Medical',  icon:'🏥', color:'#10b981', bg:'rgba(16,185,129,0.15)', border:'rgba(16,185,129,0.4)' },
  eligibility: { label:'Eligibility', icon:'🧾', color:'#f59e0b', bg:'rgba(245,158,11,0.15)', border:'rgba(245,158,11,0.4)' },
};

export const S = {
  app: {fontFamily:"'Sora',sans-serif", background:'#0b0f1a', minHeight:'100vh', color:'#e2e8f0'},
  header: {background:'#111827', borderBottom:'1px solid #1f2937', padding:'0 24px', height:60,
    display:'flex', alignItems:'center', justifyContent:'space-between', position:'sticky', top:0, zIndex:100},
  logo: {display:'flex', alignItems:'center', gap:10},
  logoIcon: {width:34, height:34, borderRadius:8, display:'flex', alignItems:'center',
    justifyContent:'center', fontSize:18},
  modeSwitcher: {display:'flex', background:'#0d1320', borderRadius:10, padding:4, gap:4, border:'1px solid #1f2937'},
  modeBtn: (active, m) => ({
    display:'flex', alignItems:'center', gap:7, padding:'7px 16px', borderRadius:7,
    cursor:'pointer', border:'none', fontFamily:"'Sora',sans-serif",
    fontSize:12, fontWeight:600, letterSpacing:'0.3px', transition:'all 0.15s',
    background: active ? MODE_META[m].bg : 'transparent',
    color: active ? MODE_META[m].color : '#4b5563',
    outline: active ? `1px solid ${MODE_META[m].border}` : 'none',
  }),
  nav: {display:'flex', background:'#111827', borderBottom:'1px solid #1f2937', padding:'0 24px'},
  navTab: (active, color='#3b82f6') => ({
    padding:'12px 20px', fontSize:11, fontWeight:600, letterSpacing:'0.5px',
    cursor:'pointer', border:'none', background:'transparent',
    color: active ? color : '#6b7280',
    borderBottom: active ? `2px solid ${color}` : `2px solid transparent`,
    textTransform:'uppercase', transition:'all 0.15s',
  }),
  main: {padding:24, maxWidth:1440, margin:'0 auto'},
  card: {background:'#111827', border:'1px solid #1f2937', borderRadius:12, padding:24},
  label: {fontSize:11, fontWeight:600, letterSpacing:'0.8px', color:'#6b7280', textTransform:'uppercase', marginBottom:6, display:'block'},
  input: {width:'100%', background:'#1a2035', border:'1px solid #2d3748', borderRadius:8,
    padding:'8px 12px', color:'#e2e8f0', fontSize:13, fontFamily:"'DM Mono',monospace", outline:'none'},
  row: {display:'flex', gap:16, marginBottom:16},
  col: (flex=1) => ({flex}),
  badge: (type) => ({
    display:'inline-block', padding:'2px 8px', borderRadius:4, fontSize:10, fontWeight:700,
    letterSpacing:'0.6px', cursor:'pointer', border:`1px solid ${TYPE_META[type].border}`,
    background:TYPE_META[type].bg, color:TYPE_META[type].color, whiteSpace:'nowrap',
    userSelect:'none', transition:'opacity 0.1s',
  }),
  typeSelect: {background:'#1a2035', border:'1px solid #2d3748', borderRadius:6,
    padding:'4px 8px', color:'#e2e8f0', fontSize:12, cursor:'pointer', outline:'none'},
  tableContainer: {overflowY:'auto', maxHeight:'calc(100vh - 280px)', borderRadius:10, border:'1px solid #1f2937'},
  table: {width:'100%', borderCollapse:'collapse', fontSize:12},
  th: {background:'#0d1320', padding:'10px 14px', textAlign:'left', fontSize:10, fontWeight:700,
    letterSpacing:'0.8px', color:'#4b5563', textTransform:'uppercase', position:'sticky', top:0, zIndex:10},
  tdBase: {padding:'7px 14px', borderBottom:'1px solid #151d2e', verticalAlign:'middle'},
  codeBlock: {background:'#0d1320', borderRadius:10, border:'1px solid #1f2937', padding:20,
    fontFamily:"'DM Mono',monospace", fontSize:12, color:'#c9d1d9', overflowX:'auto',
    overflowY:'auto', maxHeight:'calc(100vh - 260px)', whiteSpace:'pre', lineHeight:1.7},
  btn: (variant='primary', accent='#3b82f6') => ({
    padding:'9px 18px', borderRadius:8, border:'none', cursor:'pointer', fontSize:12,
    fontWeight:600, letterSpacing:'0.3px', fontFamily:"'Sora',sans-serif",
    ...(variant==='primary' ? {background: accent, color:'#fff'} :
        variant==='success' ? {background:'rgba(34,197,94,0.15)', color:'#22c55e', border:'1px solid rgba(34,197,94,0.3)'} :
        variant==='danger'  ? {background:'rgba(239,68,68,0.15)', color:'#ef4444', border:'1px solid rgba(239,68,68,0.3)'} :
                              {background:'#1a2035', color:'#9ca3af', border:'1px solid #2d3748'}),
  }),
  pillFilter: (active, accent='#3b82f6') => ({
    padding:'5px 14px', borderRadius:20, fontSize:11, fontWeight:600, cursor:'pointer',
    border:`1px solid ${active ? accent : '#2d3748'}`,
    background: active ? `${accent}22` : 'transparent',
    color: active ? accent : '#6b7280', letterSpacing:'0.3px', transition:'all 0.15s',
  }),
  divider: {height:1, background:'#1f2937', margin:'20px 0'},
  sectionTitle: {fontSize:13, fontWeight:600, color:'#9ca3af', marginBottom:12},
};
