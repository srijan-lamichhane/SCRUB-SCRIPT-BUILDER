import {
  useState,
  useEffect,
  useCallback,
  useRef,
  useMemo,
  forwardRef,
  useImperativeHandle,
} from 'react';
import { getModeDefaults } from '../constants/modeDefaults.js';
import {
  VENDOR_PRESETS,
  createEmptyPharmacyMaps,
  createEmptyMedicalMaps,
  createEmptyEligibilityMemberMaps,
  createEmptyEligibilityEligMaps,
} from '../constants/vendorPresets.js';
import { generatePharmacySQL } from '../sql/generatePharmacySQL.js';
import { generateMedicalSQL } from '../sql/generateMedicalSQL.js';
import { generateEligibilitySQL } from '../sql/generateEligibilitySQL.js';
import { S, TYPE_META, TYPE_ORDER } from '../styles/scrubStyles.js';

export const ScrubBuilder = forwardRef(function ScrubBuilder({ mode, accent }, ref) {
  const [tab, setTab] = useState('config');
  const { storePfx, isPx, defaultRaw, defaultMaps, defaultMapsMember, defaultMapsEligibility, defaultCfg } = useMemo(
    () => getModeDefaults(mode),
    [mode]
  );
  const isMedical = mode === 'medical';
  const isEligibility = mode === 'eligibility';

  const [cfg, setCfg]             = useState(defaultCfg);
  const [rawFields, setRawFields] = useState(defaultRaw);
  const [maps, setMaps]           = useState(() => defaultMaps.map(m => ({ ...m })));
  const [mapsMember, setMapsMember] = useState(() => (defaultMapsMember || []).map(m => ({ ...m })));
  const [mapsElig, setMapsElig]   = useState(() => (defaultMapsEligibility || []).map(m => ({ ...m })));
  const [eligMapLayer, setEligMapLayer] = useState('member');
  const [typeFilter, setTypeFilter] = useState('all');
  const [search, setSearch]       = useState('');
  const [newField, setNewField]   = useState('');
  const [pasteText, setPasteText] = useState('');
  const [dragIdx, setDragIdx]     = useState(null);
  const [sqlOutput, setSqlOutput] = useState('');
  const [copied, setCopied]       = useState(false);
  const textareaRefs              = useRef({});

  useEffect(() => {
    (async () => {
      try {
        const c = await window.storage.get(`${storePfx}_cfg`);
        const f = await window.storage.get(`${storePfx}_fields`);
        const m = await window.storage.get(`${storePfx}_maps`);
        const mm = await window.storage.get(`${storePfx}_maps_member`);
        const me = await window.storage.get(`${storePfx}_maps_eligibility`);
        if (c) setCfg(JSON.parse(c.value));
        if (f) setRawFields(JSON.parse(f.value));
        if (m && storePfx !== 'elig') setMaps(JSON.parse(m.value));
        if (mm && storePfx === 'elig') setMapsMember(JSON.parse(mm.value));
        if (me && storePfx === 'elig') setMapsElig(JSON.parse(me.value));
      } catch {}
    })();
  }, [storePfx]);

  const persist = useCallback(async (c, f, m, mm, me) => {
    try {
      await window.storage.set(`${storePfx}_cfg`, JSON.stringify(c));
      await window.storage.set(`${storePfx}_fields`, JSON.stringify(f));
      await window.storage.set(`${storePfx}_maps`, JSON.stringify(m));
      if (storePfx === 'elig') {
        await window.storage.set(`${storePfx}_maps_member`, JSON.stringify(mm));
        await window.storage.set(`${storePfx}_maps_eligibility`, JSON.stringify(me));
      }
    } catch {}
  }, [storePfx]);

  useEffect(() => {
    if (storePfx === 'elig') persist(cfg, rawFields, [], mapsMember, mapsElig);
    else persist(cfg, rawFields, maps, null, null);
  }, [cfg, rawFields, maps, mapsMember, mapsElig, storePfx, persist]);

  const doGenerate = useCallback(() => {
    const sql = isPx
      ? generatePharmacySQL(cfg, rawFields, maps)
      : isMedical
        ? generateMedicalSQL(cfg, rawFields, maps)
        : generateEligibilitySQL(cfg, rawFields, mapsMember, mapsElig);
    setSqlOutput(sql);
    setTab('generate');
  }, [cfg, rawFields, mapsMember, mapsElig, isPx, isMedical]);

  /** Empty raw list + all-null mappings (same row count as templates). Client config unchanged. */
  const clearLayout = useCallback(() => {
    setRawFields([]);
    if (isPx) setMaps(createEmptyPharmacyMaps().map(m => ({ ...m })));
    else if (isMedical) setMaps(createEmptyMedicalMaps().map(m => ({ ...m })));
    else {
      setMapsMember(createEmptyEligibilityMemberMaps().map(m => ({ ...m })));
      setMapsElig(createEmptyEligibilityEligMaps().map(m => ({ ...m })));
    }
    setEligMapLayer('member');
  }, [isPx, isMedical, isEligibility]);

  const applyVendorPreset = useCallback((presetId) => {
    if (isPx) {
      const p = VENDOR_PRESETS.pharmacy[presetId];
      if (!p) return;
      setRawFields([...p.rawFields]);
      setMaps(p.maps.map(m => ({ ...m })));
      return;
    }
    if (isMedical) {
      const p = VENDOR_PRESETS.medical[presetId];
      if (!p) return;
      setRawFields([...p.rawFields]);
      setMaps(p.maps.map(m => ({ ...m })));
      return;
    }
    const p = VENDOR_PRESETS.eligibility[presetId];
    if (!p) return;
    setRawFields([...p.rawFields]);
    setMapsMember(p.mapsMember.map(m => ({ ...m })));
    setMapsElig(p.mapsEligibility.map(m => ({ ...m })));
  }, [isPx, isMedical, isEligibility]);

  useImperativeHandle(ref, () => ({
    generate: doGenerate,
    clearLayout,
    applyVendorPreset,
  }), [doGenerate, clearLayout, applyVendorPreset]);

  const patchMapAt = (setter, idx, key, val) =>
    setter(prev => {
      const n = [...prev];
      n[idx] = { ...n[idx], [key]: val };
      return n;
    });

  const updateMap = (idx, key, val) => {
    if (!isEligibility) patchMapAt(setMaps, idx, key, val);
    else if (eligMapLayer === 'member') patchMapAt(setMapsMember, idx, key, val);
    else patchMapAt(setMapsElig, idx, key, val);
  };

  const cycleType = (idx) => {
    const spin = prev => {
      const n = [...prev];
      const c = TYPE_ORDER.indexOf(n[idx].type);
      n[idx] = { ...n[idx], type: TYPE_ORDER[(c + 1) % 4], value: '' };
      return n;
    };
    if (!isEligibility) setMaps(spin);
    else if (eligMapLayer === 'member') setMapsMember(spin);
    else setMapsElig(spin);
  };

  const handleCopy = () => {
    navigator.clipboard.writeText(sqlOutput).then(()=>{
      setCopied(true); setTimeout(()=>setCopied(false),2000);
    });
  };

  const handleDownload = () => {
    const blob = new Blob([sqlOutput], {type:'text/plain'});
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    const typeName = isPx ? 'PHARMACY' : isMedical ? 'MEDICAL' : 'ELIGIBILITY';
    a.download = `SP_SCRUB_STREAM_${cfg.clientName}_${typeName}.sql`;
    a.click(); URL.revokeObjectURL(url);
  };

  const handleAddField = () => {
    const f = newField.trim().toUpperCase();
    if (f && !rawFields.includes(f)) { setRawFields(p=>[...p,f]); setNewField(''); }
  };

  const handlePasteAdd = () => {
    const fields = pasteText.split(/[\n\t,;| ]+/).map(f=>f.trim().toUpperCase())
      .filter(f=>f && !rawFields.includes(f));
    if (fields.length) { setRawFields(p=>[...p,...fields]); setPasteText(''); }
  };

  const handleDragStart = i  => setDragIdx(i);
  const handleDragOver  = (e,over) => {
    e.preventDefault();
    if (dragIdx===null||dragIdx===over) return;
    setRawFields(p=>{ const n=[...p]; const [mv]=n.splice(dragIdx,1); n.splice(over,0,mv); return n; });
    setDragIdx(over);
  };
  const handleDragEnd = () => setDragIdx(null);

  const activeMaps = useMemo(() => {
    if (!isEligibility) return maps;
    return eligMapLayer === 'member' ? mapsMember : mapsElig;
  }, [isEligibility, eligMapLayer, maps, mapsMember, mapsElig]);

  const memberStagingTargets = useMemo(() => mapsMember.map(m => m.target), [mapsMember]);

  const filteredMaps = useMemo(
    () =>
      activeMaps
        .map((m, i) => ({ ...m, _i: i }))
        .filter(m => {
          const typeOk = typeFilter === 'all' || m.type === typeFilter;
          const q = search.toLowerCase();
          const val = String(m.value ?? '').toLowerCase();
          const ins = String(m.insertExpr ?? '').toLowerCase();
          const searchOk =
            !search ||
            m.target.toLowerCase().includes(q) ||
            val.includes(q) ||
            ins.includes(q);
          return typeOk && searchOk;
        }),
    [activeMaps, typeFilter, search]
  );

  const layerCounts = useMemo(() => {
    const c = { all: activeMaps.length };
    TYPE_ORDER.forEach(t => {
      c[t] = activeMaps.filter(m => m.type === t).length;
    });
    return c;
  }, [activeMaps]);

  const allMapsCounts = useMemo(() => {
    const src = isEligibility ? [...mapsMember, ...mapsElig] : maps;
    const c = { all: src.length };
    TYPE_ORDER.forEach(t => {
      c[t] = src.filter(m => m.type === t).length;
    });
    return c;
  }, [isEligibility, maps, mapsMember, mapsElig]);

  const activeMapTotal = useMemo(() => {
    if (!isEligibility) return maps.filter(m => m.type !== 'null').length;
    return [mapsMember, mapsElig].reduce(
      (n, arr) => n + arr.filter(m => m.type !== 'null').length,
      0
    );
  }, [isEligibility, maps, mapsMember, mapsElig]);

  const cfgInput = (key, label, ph='') => (
    <div style={S.col()}>
      <label style={S.label}>{label}</label>
      <input style={S.input} value={cfg[key]||''} placeholder={ph}
        onChange={e=>setCfg(p=>({...p,[key]:e.target.value.toUpperCase()}))} />
    </div>
  );

  const cfgInputPreserve = (key, label, ph = '') => (
    <div style={S.col()}>
      <label style={S.label}>{label}</label>
      <input style={S.input} value={cfg[key] ?? ''} placeholder={ph}
        onChange={e => setCfg(p => ({ ...p, [key]: e.target.value }))} />
    </div>
  );

  const spType = isPx ? 'PHARMACY' : isMedical ? 'MEDICAL' : 'ELIGIBILITY';
  const spName = `${cfg.schema}.SP_SCRUB_STREAM_${cfg.clientName}_${spType}()`;

  return (
    <div>
      <nav style={S.nav}>
        {['config','fields','mappings','generate'].map(t=>(
          <button key={t} style={S.navTab(tab===t,accent)} onClick={()=>{
            if(t==='generate') doGenerate(); else setTab(t);
          }}>
            {t==='config'   && '01 · Client Config'}
            {t==='fields'   && `02 · Raw Fields (${rawFields.length})`}
            {t==='mappings' && `03 · Mappings (${activeMapTotal} active)`}
            {t==='generate' && '04 · Generate SQL'}
          </button>
        ))}
      </nav>

      {tab==='config' && (
        <div style={S.main}>
          <div style={S.card}>
            <div style={{marginBottom:20}}>
              <div style={{fontSize:16,fontWeight:700,marginBottom:4}}>Client Configuration</div>
              <div style={{fontSize:12,color:'#6b7280'}}>Values injected into the generated stored procedure.</div>
            </div>
            <div style={S.row}>{cfgInput('clientName','Client Name')}{cfgInput('warehouse','Warehouse')}</div>
            <div style={S.row}>{cfgInput('database','Database')}{cfgInput('schema','Schema')}</div>
            <div style={S.row}>{cfgInput('streamTable','Stream Table')}<div style={S.col()}/></div>
            <div style={S.divider}/>
            <div style={S.sectionTitle}>Reference Tables</div>
            {isPx && (
              <div style={S.row}>
                {cfgInput('masterProviders','Master Providers')}
                {cfgInput('masterDrug','Master Drug')}
              </div>
            )}
            {isMedical && (<>
              <div style={S.row}>
                {cfgInput('masterProviders','Master Providers')}
                {cfgInput('masterProcedure','Master Procedure')}
              </div>
              <div style={S.row}>
                {cfgInput('masterProcedureModifier','Master Procedure Modifier')}
                {cfgInput('masterDiagnosis','Master Diagnosis')}
              </div>
              <div style={S.row}>{cfgInput('masterPos','Master POS')}<div style={S.col()}/></div>
            </>)}
            {isEligibility && (<>
              <div style={S.row}>
                {cfgInput('memberTable','Member Table','MAP_MEMBER_PLATFORM')}
                {cfgInput('eligibilityTable','Eligibility Table','MAP_ELIGIBILITY_PLATFORM')}
              </div>
              <div style={S.row}>
                {cfgInput('masterClientGroup','Master Client Group')}
                {cfgInput('masterZip','Master ZIP Codes')}
              </div>
              <div style={S.row}>
                {cfgInputPreserve('controlFileFilter','Control file exclude pattern','%CONTROLS%')}
                <div style={S.col()} />
              </div>
              <div style={S.divider} />
              <div style={S.sectionTitle}>Reconciliation (member key &amp; dependents)</div>
              <div style={{ fontSize: 12, color: '#6b7280', marginBottom: 12 }}>
                Uses columns produced by the stream → staging mapping. Defaults match typical Cigna-style feeds.
              </div>
              <div style={S.row}>
                {cfgInputPreserve('memberKeyExpression','MEMBER_KEY expression')}
                {cfgInputPreserve('memberSecondaryKeyExpression','Secondary member key (LISTAGG)')}
              </div>
              <div style={S.row}>
                {cfgInputPreserve('employeeRelationshipValue','Employee relationship value (data)','EMPLOYEE')}
                {cfgInputPreserve('listaggDelimiter','LISTAGG delimiter',',')}
              </div>
            </>)}
          </div>
          <div style={{marginTop:16,padding:14,background:`${accent}0d`,border:`1px solid ${accent}33`,borderRadius:10}}>
            <div style={{fontSize:11,fontWeight:600,color:accent,marginBottom:5}}>📋 Generated Procedure Name</div>
            <div style={{fontFamily:"'DM Mono',monospace",fontSize:13,color:'#e2e8f0'}}>{spName}</div>
          </div>
        </div>
      )}

      {tab==='fields' && (
        <div style={S.main}>
          <div style={S.card}>
            <div style={{display:'flex',justifyContent:'space-between',alignItems:'flex-start',marginBottom:20}}>
              <div>
                <div style={{fontSize:16,fontWeight:700,marginBottom:4}}>Raw Source Fields</div>
                <div style={{fontSize:12,color:'#6b7280'}}>
                  Drag to reorder · Generates the UPPER/TRIM dedup block
                  {isEligibility && ' · Member mapping references these raw column names (same idea as pharmacy / medical)'}
                </div>
              </div>
              <div style={{display:'flex',gap:8,alignItems:'center'}}>
                <span style={{fontSize:11,color:'#6b7280',fontFamily:"'DM Mono',monospace"}}>{rawFields.length} fields</span>
                <button style={S.btn('danger')} onClick={()=>setRawFields([])}>🗑 Clear All</button>
              </div>
            </div>

            <label style={S.label}>Add single field</label>
            <div style={{display:'flex',gap:10,marginBottom:20}}>
              <input style={{...S.input,flex:1}} placeholder="FIELDNAME"
                value={newField} onChange={e=>setNewField(e.target.value.toUpperCase())}
                onKeyDown={e=>e.key==='Enter'&&handleAddField()} />
              <button style={S.btn('primary',accent)} onClick={handleAddField}>+ Add</button>
            </div>

            <label style={S.label}>Paste multiple fields</label>
            <div style={{display:'flex',gap:10,marginBottom:24}}>
              <textarea rows={3} style={{...S.input,flex:1,resize:'vertical'}}
                placeholder="FIELD1  FIELD2  FIELD3  or paste a header row"
                value={pasteText} onChange={e=>setPasteText(e.target.value)} />
              <div style={{display:'flex',flexDirection:'column',gap:8}}>
                <button style={{...S.btn('primary',accent),whiteSpace:'nowrap'}} onClick={handlePasteAdd}>+ Add Fields</button>
                <button style={{...S.btn('ghost'),whiteSpace:'nowrap'}} onClick={()=>setPasteText('')}>Clear</button>
              </div>
            </div>

            <div style={{display:'grid',gridTemplateColumns:'repeat(auto-fill,minmax(220px,1fr))',gap:8}}>
              {rawFields.map((f,idx)=>(
                <div key={f} draggable
                  onDragStart={()=>handleDragStart(idx)}
                  onDragOver={e=>handleDragOver(e,idx)}
                  onDragEnd={handleDragEnd}
                  style={{display:'flex',alignItems:'center',justifyContent:'space-between',
                    background:dragIdx===idx?'#1e2d4a':'#1a2035',
                    border:`1px solid ${dragIdx===idx?accent:'#2d3748'}`,
                    borderRadius:6,padding:'6px 10px',cursor:'grab',
                    opacity:dragIdx===idx?0.5:1,transition:'all 0.1s'}}>
                  <div style={{display:'flex',alignItems:'center',gap:8,minWidth:0}}>
                    <span style={{color:'#374151',fontSize:13,flexShrink:0}}>⠿</span>
                    <span style={{fontSize:10,color:'#4b5563',fontFamily:"'DM Mono',monospace",flexShrink:0}}>
                      {String(idx+1).padStart(2,'0')}
                    </span>
                    <span style={{fontFamily:"'DM Mono',monospace",fontSize:11,color:'#a5b4fc',
                      overflow:'hidden',textOverflow:'ellipsis',whiteSpace:'nowrap'}}>{f}</span>
                  </div>
                  <span style={{cursor:'pointer',color:'#4b5563',fontSize:16,lineHeight:1,flexShrink:0,marginLeft:6}}
                    onClick={()=>setRawFields(p=>p.filter(x=>x!==f))}>×</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {tab==='mappings' && (
        <div style={S.main}>
          {isEligibility && (
            <div style={{ display: 'flex', gap: 8, marginBottom: 16, flexWrap: 'wrap' }}>
              {[
                { id: 'member', label: '01 · MAP_MEMBER_PLATFORM' },
                { id: 'eligibility', label: '02 · MAP_ELIGIBILITY_PLATFORM' },
              ].map(({ id, label }) => (
                <button
                  key={id}
                  type="button"
                  style={S.pillFilter(eligMapLayer === id, accent)}
                  onClick={() => { setEligMapLayer(id); setTypeFilter('all'); setSearch(''); }}
                >
                  {label}
                </button>
              ))}
            </div>
          )}
          <div style={{display:'flex',gap:10,alignItems:'center',marginBottom:14,flexWrap:'wrap'}}>
            <input style={{...S.input,width:240}} placeholder="Search target field or value..."
              value={search} onChange={e=>setSearch(e.target.value)} />
            <div style={{display:'flex',gap:6,flexWrap:'wrap'}}>
              {['all',...TYPE_ORDER].map(t=>(
                <button key={t} style={S.pillFilter(typeFilter===t,accent)} onClick={()=>setTypeFilter(t)}>
                  {t.toUpperCase()} {layerCounts[t]||''}
                </button>
              ))}
            </div>
            <div style={{marginLeft:'auto',fontSize:11,color:'#4b5563',maxWidth:340,textAlign:'right'}}>
              {isEligibility && eligMapLayer === 'member' && 'Staging from raw (like pharmacy). Optional insert override uses A/B/C after reconciliation.'}
              {isEligibility && eligMapLayer === 'eligibility' && 'Final eligibility load: A = latest member row, B = enrollment, C = client group.'}
              {!isEligibility && 'Click badge to cycle type'}
            </div>
          </div>
          <div style={S.tableContainer}>
            <table style={S.table}>
              <thead>
                <tr>
                  <th style={{...S.th,width:40}}>#</th>
                  <th style={S.th}>Target Field</th>
                  <th style={{...S.th,width:110}}>Type</th>
                  {isEligibility && eligMapLayer === 'member' ? (
                    <>
                      <th style={S.th}>From raw (staging)</th>
                      <th style={S.th}>Insert override (optional)</th>
                    </>
                  ) : (
                    <th style={S.th}>Expression / Value</th>
                  )}
                </tr>
              </thead>
              <tbody>
                {filteredMaps.map(m=>{
                  const i=m._i;
                  const rowKey = `${isEligibility ? eligMapLayer : 'px'}:${m.target}:${i}`;
                  const isEligMember = isEligibility && eligMapLayer === 'member';
                  const directOptions = isEligibility && eligMapLayer === 'eligibility' ? memberStagingTargets : rawFields;
                  const directLabel = isEligibility && eligMapLayer === 'eligibility'
                    ? '— staging column —'
                    : '— select raw field —';
                  const exprPh = isEligibility && eligMapLayer === 'member'
                    ? 'SQL over raw columns (staging)'
                    : isEligibility && eligMapLayer === 'eligibility'
                      ? 'SQL (A=latest, B=enrollment, C=master group)'
                      : 'SQL expression';
                  const stagingCell = (
                    <>
                      {m.type==='null' ? (
                        <span style={{fontFamily:"'DM Mono',monospace",fontSize:11,color:'#374151'}}>— NULL —</span>
                      ) : m.type==='direct' ? (
                        <select style={{...S.typeSelect,width:'100%'}} value={m.value}
                          onChange={e=>updateMap(i,'value',e.target.value)}>
                          <option value="">{directLabel}</option>
                          {directOptions.map(f=><option key={f} value={f}>{f}</option>)}
                        </select>
                      ) : m.type==='hardcoded' ? (
                        <input style={{...S.input,width:'100%',padding:'5px 10px'}}
                          value={m.value} onChange={e=>updateMap(i,'value',e.target.value)}
                          placeholder="Literal value (no quotes needed)" />
                      ) : (
                        <textarea ref={el=>{ textareaRefs.current[rowKey]=el; }}
                          style={{...S.input,width:'100%',resize:'vertical',minHeight:36,padding:'5px 10px',lineHeight:1.5}}
                          value={m.value} rows={m.value.length>80?3:1}
                          onChange={e=>updateMap(i,'value',e.target.value)}
                          placeholder={exprPh} />
                      )}
                    </>
                  );
                  return (
                    <tr key={rowKey} style={{background:i%2===0?'#0f1521':'transparent'}}>
                      <td style={{...S.tdBase,color:'#374151',fontFamily:"'DM Mono',monospace",fontSize:10}}>{i+1}</td>
                      <td style={S.tdBase}>
                        <span style={{fontFamily:"'DM Mono',monospace",fontSize:11,color:m.type==='null'?'#4b5563':'#e2e8f0'}}>
                          {m.target}
                        </span>
                      </td>
                      <td style={S.tdBase}>
                        <span style={S.badge(m.type)} onClick={()=>cycleType(i)}>
                          {TYPE_META[m.type].label}
                        </span>
                      </td>
                      {isEligMember ? (
                        <>
                          <td style={{...S.tdBase,width:'28%'}}>{stagingCell}</td>
                          <td style={{...S.tdBase,width:'28%'}}>
                            <textarea
                              style={{...S.input,width:'100%',resize:'vertical',minHeight:36,padding:'5px 10px',lineHeight:1.5}}
                              value={m.insertExpr ?? ''}
                              rows={(m.insertExpr || '').length > 60 ? 3 : 1}
                              onChange={e => updateMap(i, 'insertExpr', e.target.value)}
                              placeholder="Blank → A.<target> (or NULL)"
                            />
                          </td>
                        </>
                      ) : (
                        <td style={{...S.tdBase,width:'55%'}}>{stagingCell}</td>
                      )}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {tab==='generate' && (
        <div style={S.main}>
          <div style={{display:'flex',gap:10,alignItems:'center',marginBottom:16}}>
            <div style={{flex:1}}>
              <div style={{fontSize:15,fontWeight:700}}>Generated Stored Procedure</div>
              <div style={{fontSize:11,color:'#6b7280',marginTop:3,fontFamily:"'DM Mono',monospace"}}>{spName}</div>
            </div>
            <div style={{display:'flex',gap:8}}>
              <button style={S.btn(copied?'success':'ghost')} onClick={handleCopy}>
                {copied?'✓ Copied!':'📋 Copy All'}
              </button>
              <button style={{...S.btn('primary'),background:accent}} onClick={handleDownload}>⬇ Download .sql</button>
            </div>
          </div>
          <div style={{display:'flex',gap:10,marginBottom:16,flexWrap:'wrap'}}>
            {TYPE_ORDER.map(t=>(
              <div key={t} style={{padding:'8px 14px',borderRadius:8,background:TYPE_META[t].bg,
                border:`1px solid ${TYPE_META[t].border}`,display:'flex',gap:8,alignItems:'center'}}>
                <span style={{fontSize:18,fontWeight:800,color:TYPE_META[t].color}}>{allMapsCounts[t]}</span>
                <span style={{fontSize:10,fontWeight:700,color:TYPE_META[t].color,letterSpacing:'0.5px'}}>{t.toUpperCase()}</span>
              </div>
            ))}
            <div style={{padding:'8px 14px',borderRadius:8,background:'rgba(99,102,241,0.1)',
              border:'1px solid rgba(99,102,241,0.3)',display:'flex',gap:8,alignItems:'center'}}>
              <span style={{fontSize:18,fontWeight:800,color:'#818cf8'}}>{rawFields.length}</span>
              <span style={{fontSize:10,fontWeight:700,color:'#818cf8'}}>RAW FIELDS</span>
            </div>
          </div>
          {!sqlOutput
            ? <div style={{...S.codeBlock,textAlign:'center',padding:40,color:'#4b5563'}}>Click "Generate SQL" to build</div>
            : <div style={S.codeBlock}>{sqlOutput}</div>
          }
        </div>
      )}
    </div>
  );
});
