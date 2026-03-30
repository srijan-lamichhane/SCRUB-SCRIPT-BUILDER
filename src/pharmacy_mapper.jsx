import { useState, useEffect, useCallback, useRef } from "react";

// ── STYLES ────────────────────────────────────────────────────────────────────
const GLOBAL_STYLE = `
  @import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Sora:wght@400;500;600;700&display=swap');
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: #0b0f1a; }
  ::-webkit-scrollbar { width: 6px; height: 6px; }
  ::-webkit-scrollbar-track { background: #111827; }
  ::-webkit-scrollbar-thumb { background: #2d3748; border-radius: 3px; }
  ::-webkit-scrollbar-thumb:hover { background: #4a5568; }
`;

// ── CONSTANTS ─────────────────────────────────────────────────────────────────
const SLATERX_RAW_FIELDS = [
  'TRANSACTIONID','PCN','PHARMACYID','PHARMACYNAME','PHARMACYADDRESS','PHARMACYCITY',
  'PHARMACYSTATE','PHARMACYZIPCODE','PHARMACYPHONE','PHARMACYFAX','RELATIONSHIPID',
  'PHARMACYSERVICETYPE','MEMBERID','MEMBERFIRSTNAME','MEMBERLASTNAME','MEMBERGENDER',
  'BIRTHDATE','OTHERCOVERAGECODE','OTHERPAYERAMOUNT','SERVICEDATE','TRANSACTIONDATE',
  'RXNUMBER','GCN','GPICODE','TRANSTATUS','GNN','TRANCODE','COSTBASIS','NDC',
  'DRUGNAME','LABELNAME','DRUGSTRENGTH','DOSAGEFORM','GPINAME','RXOTCINDICATION',
  'UNITDOSE','COMPOUNDCODE','QUANTITYDISPENSED','DRUGTYPE','TIERCODE','SPECIALTY',
  'DAWCODE','FORMULARYINDICATOR','PRESCRIBINGPHYSICIAN','DOCTORDEA','DOCTORZIPCODE',
  'GROSSAMTDUE','USUALANDCUST','BASISPAID','INGREDIENTCOST','DISPFEEPAID','SALESTAX',
  'INCENTIVEFEE','PLANPAID','PATIENTPAID','DEDUCTIBLEAMT','OOP','COPAY','COINSURANCE',
  'AWP','DAYSSUPPLY','CARRIERID','ACCOUNTID','GROUPID','LINEOFBUSINESS','REVERSEDTRANID',
  'PRESCRIBERID','PHARMACYNPI','PRESCRIPTIONORIGINCODE','FILLNUMBER','PRESCRIPTIONWRITTENDATE',
  'DAWPENALTY','PERSONCODE','GROUPCODE','SUBSCRIBERSSN','SOURCEFILENAME',
  'FILE_RECEIVED_DATE','IMPORT_DATE'
];

// raw fields that go to MAX() aggregation (non-dedup)
const RAW_MAX_FIELDS = new Set(['SOURCEFILENAME','FILE_RECEIVED_DATE','IMPORT_DATE']);

const STAGE_DDL_COLS = [
  'CH_PHARMACY_CLAIM_ID VARCHAR(16777216)','RECORD_ID VARCHAR(16777216)',
  'MEMBER_ID VARCHAR(16777216)','CLAIM_ID VARCHAR(16777216)',
  'THERAPEUTIC_EQUIVALENCE_INDICATOR VARCHAR(16777216)',
  'THERAPEUTIC_EQUIVALENCE_INDICATOR_DESCRIPTION VARCHAR(16777216)',
  'GROUP_ID VARCHAR(16777216)','GCN_SEQUENCE_NUMBER VARCHAR(16777216)',
  'CH_MEMBER_ID VARCHAR(16777216)','CH_EMPLOYER_ID VARCHAR(16777216)',
  'COMPOUND_FLAG VARCHAR(16777216)','OVER_THE_COUNTER_FLAG VARCHAR(16777216)',
  'DRUG_TYPE VARCHAR(16777216)','PRESCRIBER_ID VARCHAR(16777216)',
  'PRESCRIBER_FIRST_NAME VARCHAR(16777216)','MAINTENANCE_DRUG_FLAG VARCHAR(16777216)',
  'CATEGORY_NUMBER VARCHAR(16777216)','PRESCRIBER_MIDDLE_NAME VARCHAR(16777216)',
  'PRESCRIBER_LAST_NAME VARCHAR(16777216)','CATEGORY_DESCRIPTION VARCHAR(16777216)',
  'PROVIDER_DEA_NUMBER VARCHAR(16777216)','PROVIDER_NPI VARCHAR(16777216)',
  'PROVIDER_NABP_NUMBER VARCHAR(16777216)','PHARMACY_NAME VARCHAR(16777216)',
  'WRITTEN_DATE DATE','FILLED_DATE DATE','PAID_DATE DATE',
  'DIAGNOSIS_CODE VARCHAR(16777216)','DIAGNOSIS_DESCRIPTION VARCHAR(16777216)',
  'NDC_CODE VARCHAR(16777216)','NDC_DESCRIPTION VARCHAR(16777216)',
  'SPECIALTY_DRUG_FLAG VARCHAR(16777216)','PRESCRIPTION_CLASS_CODE VARCHAR(16777216)',
  'PRESCRIPTION_CLASS_DESCRIPTION VARCHAR(16777216)','BRAND_CODE VARCHAR(16777216)',
  'DRUG_NAME VARCHAR(16777216)','DRUG_DOSAGE VARCHAR(16777216)',
  'DRUG_STRENGTH VARCHAR(16777216)','UNIT_QUANTITY FLOAT','DAYS_OF_SUPPLY VARCHAR(16777216)',
  'PRESCRIPTION_LABEL_NAME VARCHAR(16777216)','FORMULARY_FLAG VARCHAR(16777216)',
  'GENERIC_FLAG VARCHAR(16777216)','MAIL_ORDER_FLAG VARCHAR(16777216)',
  'UNIT_PRICE FLOAT','PACKAGE_PRICE FLOAT','DUPLICATE_THERAPY_CLASS_ID VARCHAR(16777216)',
  'DUPLICATE_THERAPY_DESCRIPTION VARCHAR(16777216)',
  'DUPLICATE_THERAPY_ALLOWANCE_NUMBER VARCHAR(16777216)',
  'HIC3_CODE VARCHAR(16777216)','HIC3_DESCRIPTION VARCHAR(16777216)',
  'ROUTE_OF_ADMINISTRATION VARCHAR(16777216)','REFILL_QUANTITY FLOAT',
  'REFILLS_ALLOWED_NUMBER FLOAT','DISPENSED_AS_WRITTEN_CODE VARCHAR(16777216)',
  'DISPENSED_AS_WRITTEN_DESCRIPTION VARCHAR(16777216)',
  'ALLOWED_AMOUNT FLOAT','BILLED_AMOUNT FLOAT','COINSURANCE_AMOUNT FLOAT',
  'COPAY_AMOUNT FLOAT','DEDUCTIBLE_AMOUNT FLOAT','DISPENSING_FEE_AMOUNT FLOAT',
  'INGREDIENT_COST_AMOUNT FLOAT','STATE_TAX_AMOUNT FLOAT',
  'USUAL_CUSTOMARY_FEE_AMOUNT FLOAT','PAID_AMOUNT FLOAT',
  'PHARMACY_NPI VARCHAR(16777216)','PHARMACY_STATE VARCHAR(16777216)',
  'PHARMACY_ZIP_CODE VARCHAR(16777216)',
  'PRESCRIPTION_GENERIC_BRAND_INDICATOR VARCHAR(16777216)',
  'ADJUDICATION_DESCRIPTION VARCHAR(16777216)','GPI10_CODE VARCHAR(16777216)',
  'CARDHOLDER_ID VARCHAR(16777216)','CLIENT_ACCOUNT_ID VARCHAR(16777216)',
  'DAYS_SUPPLY_INTENDED_TO_BE_DISPENSED FLOAT','DISPENSING_STATUS VARCHAR(16777216)',
  'DRUG_CATEGORY_CODE VARCHAR(16777216)','OTHER_PAYER_AMOUNT FLOAT',
  'PROFESSIONAL_SERVICE_CODE_1 VARCHAR(16777216)','PROFESSIONAL_SERVICE_CODE_2 VARCHAR(16777216)',
  'PROFESSIONAL_SERVICE_CODE_3 VARCHAR(16777216)',
  'QUANTITY_INTENDED_TO_BE_DISPENSED FLOAT',
  'REASON_FOR_SERVICE_CODE_1 VARCHAR(16777216)','REASON_FOR_SERVICE_CODE_2 VARCHAR(16777216)',
  'REASON_FOR_SERVICE_CODE_3 VARCHAR(16777216)',
  'RESULT_OF_SERVICE_CODE_1 VARCHAR(16777216)','RESULT_OF_SERVICE_CODE_2 VARCHAR(16777216)',
  'RESULT_OF_SERVICE_CODE_3 VARCHAR(16777216)','SERVICE_PROVIDER_ID VARCHAR(16777216)',
  'UNIT_OF_MEASURE VARCHAR(16777216)','MERGE_DT DATE',
  'MERGED_FROM_CH_MEMBER_ID VARCHAR(16777216)','MERGED_FROM_MEMBER_ID VARCHAR(16777216)',
  'SOURCEFILENAME VARCHAR(16777216)','RECEIVED_DATE DATE','IMPORT_DATE DATE',
  'SCRUBBED_DATE DATE','AWP FLOAT',
  'UDF1 VARCHAR(16777216)','UDF2 VARCHAR(16777216)','UDF3 VARCHAR(16777216)',
  'UDF4 VARCHAR(16777216)','UDF5 VARCHAR(16777216)','UDF6 VARCHAR(16777216)',
  'UDF7 VARCHAR(16777216)','UDF8 VARCHAR(16777216)','UDF9 VARCHAR(16777216)',
  'UDF10 VARCHAR(16777216)'
];

const SLATERX_DEFAULTS = [
  {target:'CH_PHARMACY_CLAIM_ID', type:'null', value:''},
  {target:'RECORD_ID', type:'null', value:''},
  {target:'MEMBER_ID', type:'expression', value:"MEMBERID||PERSONCODE"},
  {target:'CLAIM_ID', type:'direct', value:'TRANSACTIONID'},
  {target:'THERAPEUTIC_EQUIVALENCE_INDICATOR', type:'null', value:''},
  {target:'THERAPEUTIC_EQUIVALENCE_INDICATOR_DESCRIPTION', type:'null', value:''},
  {target:'GROUP_ID', type:'direct', value:'GROUPID'},
  {target:'GCN_SEQUENCE_NUMBER', type:'direct', value:'GCN'},
  {target:'CH_MEMBER_ID', type:'null', value:''},
  {target:'CH_EMPLOYER_ID', type:'hardcoded', value:'810'},
  {target:'COMPOUND_FLAG', type:'expression', value:"CASE WHEN COMPOUNDCODE IN ('2') THEN 'Y' WHEN COMPOUNDCODE = '1' THEN 'N' ELSE 'U' END"},
  {target:'OVER_THE_COUNTER_FLAG', type:'expression', value:"CASE WHEN RXOTCINDICATION = 'YES' THEN 'Y' WHEN RXOTCINDICATION = 'NO' THEN 'N' ELSE 'U' END"},
  {target:'DRUG_TYPE', type:'expression', value:"CASE WHEN DRUGTYPE IN ('GEN') THEN 'GENERIC' WHEN DRUGTYPE IN ('MSB','SSB','Branded Generic') THEN 'BRANDED' ELSE 'UNKNOWN' END"},
  {target:'PRESCRIBER_ID', type:'direct', value:'PRESCRIBERID'},
  {target:'PRESCRIBER_FIRST_NAME', type:'direct', value:'PRESCRIBINGPHYSICIAN'},
  {target:'MAINTENANCE_DRUG_FLAG', type:'null', value:''},
  {target:'CATEGORY_NUMBER', type:'null', value:''},
  {target:'PRESCRIBER_MIDDLE_NAME', type:'null', value:''},
  {target:'PRESCRIBER_LAST_NAME', type:'null', value:''},
  {target:'CATEGORY_DESCRIPTION', type:'null', value:''},
  {target:'PROVIDER_DEA_NUMBER', type:'direct', value:'DOCTORDEA'},
  {target:'PROVIDER_NPI', type:'direct', value:'PRESCRIBERID'},
  {target:'PROVIDER_NABP_NUMBER', type:'null', value:''},
  {target:'PHARMACY_NAME', type:'direct', value:'PHARMACYNAME'},
  {target:'WRITTEN_DATE', type:'expression', value:"TO_DATE(PRESCRIPTIONWRITTENDATE::VARCHAR,'MM/DD/YYYY')"},
  {target:'FILLED_DATE', type:'expression', value:"TO_DATE(SERVICEDATE::VARCHAR,'MM/DD/YYYY')"},
  {target:'PAID_DATE', type:'expression', value:"TO_DATE(TRANSACTIONDATE::VARCHAR,'MM/DD/YYYY')"},
  {target:'DIAGNOSIS_CODE', type:'null', value:''},
  {target:'DIAGNOSIS_DESCRIPTION', type:'null', value:''},
  {target:'NDC_CODE', type:'expression', value:"LPAD(NDC,11,'0')"},
  {target:'NDC_DESCRIPTION', type:'direct', value:'LABELNAME'},
  {target:'SPECIALTY_DRUG_FLAG', type:'expression', value:"CASE WHEN SPECIALTY = 'YES' THEN 'Y' WHEN SPECIALTY = 'NO' THEN 'N' ELSE 'U' END"},
  {target:'PRESCRIPTION_CLASS_CODE', type:'null', value:''},
  {target:'PRESCRIPTION_CLASS_DESCRIPTION', type:'null', value:''},
  {target:'BRAND_CODE', type:'null', value:''},
  {target:'DRUG_NAME', type:'direct', value:'DRUGNAME'},
  {target:'DRUG_DOSAGE', type:'direct', value:'DOSAGEFORM'},
  {target:'DRUG_STRENGTH', type:'direct', value:'DRUGSTRENGTH'},
  {target:'UNIT_QUANTITY', type:'direct', value:'QUANTITYDISPENSED'},
  {target:'DAYS_OF_SUPPLY', type:'expression', value:'CAST(DAYSSUPPLY AS INT)'},
  {target:'PRESCRIPTION_LABEL_NAME', type:'null', value:''},
  {target:'FORMULARY_FLAG', type:'expression', value:"CASE WHEN FORMULARYINDICATOR = 'YES' THEN 'Y' WHEN FORMULARYINDICATOR = 'NO' THEN 'N' ELSE 'N' END"},
  {target:'GENERIC_FLAG', type:'expression', value:"CASE WHEN DRUGTYPE = 'GEN' THEN 'Y' ELSE 'N' END"},
  {target:'MAIL_ORDER_FLAG', type:'null', value:''},
  {target:'UNIT_PRICE', type:'null', value:''},
  {target:'PACKAGE_PRICE', type:'null', value:''},
  {target:'DUPLICATE_THERAPY_CLASS_ID', type:'null', value:''},
  {target:'DUPLICATE_THERAPY_DESCRIPTION', type:'null', value:''},
  {target:'DUPLICATE_THERAPY_ALLOWANCE_NUMBER', type:'null', value:''},
  {target:'HIC3_CODE', type:'null', value:''},
  {target:'HIC3_DESCRIPTION', type:'null', value:''},
  {target:'ROUTE_OF_ADMINISTRATION', type:'null', value:''},
  {target:'REFILL_QUANTITY', type:'direct', value:'FILLNUMBER'},
  {target:'REFILLS_ALLOWED_NUMBER', type:'null', value:''},
  {target:'DISPENSED_AS_WRITTEN_CODE', type:'direct', value:'DAWCODE'},
  {target:'DISPENSED_AS_WRITTEN_DESCRIPTION', type:'expression', value:"CASE WHEN DAWCODE='0' THEN 'No Product Selection Indicated' WHEN DAWCODE='1' THEN 'Substitution Not Allowed by Prescriber' WHEN DAWCODE='2' THEN 'Substitution Allowed - Patient Requested Product Dispensed' WHEN DAWCODE='3' THEN 'Substitution Allowed - Pharmacist Selected Product Dispensed' WHEN DAWCODE='4' THEN 'Substitution Allowed - Generic Drug Not in Stock' WHEN DAWCODE='5' THEN 'Substitution Allowed - Brand Drug Dispensed as Generic' WHEN DAWCODE='6' THEN 'Override' WHEN DAWCODE='7' THEN 'Substitution Not Allowed - Brand Drug Mandated by Law' WHEN DAWCODE='8' THEN 'Substitution Allowed - Generic Drug Not Available in Marketplace' WHEN DAWCODE='9' THEN 'Substitution Allowed By Prescriber but Plan Requests Brand - Patients Plan Requested Brand Product To Be Dispensed' ELSE DAWCODE END"},
  {target:'ALLOWED_AMOUNT', type:'direct', value:'GROSSAMTDUE'},
  {target:'BILLED_AMOUNT', type:'direct', value:'GROSSAMTDUE'},
  {target:'COINSURANCE_AMOUNT', type:'direct', value:'COINSURANCE'},
  {target:'COPAY_AMOUNT', type:'direct', value:'COPAY'},
  {target:'DEDUCTIBLE_AMOUNT', type:'direct', value:'DEDUCTIBLEAMT'},
  {target:'DISPENSING_FEE_AMOUNT', type:'direct', value:'DISPFEEPAID'},
  {target:'INGREDIENT_COST_AMOUNT', type:'direct', value:'INGREDIENTCOST'},
  {target:'STATE_TAX_AMOUNT', type:'direct', value:'SALESTAX'},
  {target:'USUAL_CUSTOMARY_FEE_AMOUNT', type:'direct', value:'USUALANDCUST'},
  {target:'PAID_AMOUNT', type:'expression', value:'PLANPAID+PATIENTPAID'},
  {target:'PHARMACY_NPI', type:'direct', value:'PHARMACYNPI'},
  {target:'PHARMACY_STATE', type:'direct', value:'PHARMACYSTATE'},
  {target:'PHARMACY_ZIP_CODE', type:'direct', value:'PHARMACYZIPCODE'},
  {target:'PRESCRIPTION_GENERIC_BRAND_INDICATOR', type:'null', value:''},
  {target:'ADJUDICATION_DESCRIPTION', type:'null', value:''},
  {target:'GPI10_CODE', type:'direct', value:'GPICODE'},
  {target:'CARDHOLDER_ID', type:'null', value:''},
  {target:'CLIENT_ACCOUNT_ID', type:'direct', value:'ACCOUNTID'},
  {target:'DAYS_SUPPLY_INTENDED_TO_BE_DISPENSED', type:'null', value:''},
  {target:'DISPENSING_STATUS', type:'null', value:''},
  {target:'DRUG_CATEGORY_CODE', type:'expression', value:"CASE WHEN DRUGTYPE = 'GEN' THEN 'GENERIC' WHEN DRUGTYPE = 'MSB' THEN 'MULTI SOURCE BRAND' WHEN DRUGTYPE = 'SSB' THEN 'SINGLE SOURCE BRAND' WHEN DRUGTYPE = 'BRANDED GENERIC' THEN 'BRANDED GENERIC' ELSE 'UNKNOWN' END"},
  {target:'OTHER_PAYER_AMOUNT', type:'null', value:''},
  {target:'PROFESSIONAL_SERVICE_CODE_1', type:'null', value:''},
  {target:'PROFESSIONAL_SERVICE_CODE_2', type:'null', value:''},
  {target:'PROFESSIONAL_SERVICE_CODE_3', type:'null', value:''},
  {target:'QUANTITY_INTENDED_TO_BE_DISPENSED', type:'null', value:''},
  {target:'REASON_FOR_SERVICE_CODE_1', type:'null', value:''},
  {target:'REASON_FOR_SERVICE_CODE_2', type:'null', value:''},
  {target:'REASON_FOR_SERVICE_CODE_3', type:'null', value:''},
  {target:'RESULT_OF_SERVICE_CODE_1', type:'null', value:''},
  {target:'RESULT_OF_SERVICE_CODE_2', type:'null', value:''},
  {target:'RESULT_OF_SERVICE_CODE_3', type:'null', value:''},
  {target:'SERVICE_PROVIDER_ID', type:'null', value:''},
  {target:'UNIT_OF_MEASURE', type:'direct', value:'DOSAGEFORM'},
  {target:'MERGE_DT', type:'null', value:''},
  {target:'MERGED_FROM_CH_MEMBER_ID', type:'null', value:''},
  {target:'MERGED_FROM_MEMBER_ID', type:'null', value:''},
  {target:'SOURCEFILENAME', type:'direct', value:'SOURCEFILENAME'},
  {target:'RECEIVED_DATE', type:'direct', value:'FILE_RECEIVED_DATE'},
  {target:'IMPORT_DATE', type:'direct', value:'IMPORT_DATE'},
  {target:'SCRUBBED_DATE', type:'expression', value:'CURRENT_DATE()'},
  {target:'AWP', type:'direct', value:'AWP'},
  {target:'UDF1', type:'expression', value:"IFNULL(MEMBERFIRSTNAME,'X')||'_'||IFNULL(MEMBERLASTNAME,'X')||'_'||COALESCE(TO_DATE(BIRTHDATE::VARCHAR,'MM/DD/YYYY'),'99991231')||'_'||IFNULL(MEMBERGENDER,'U')"},
  {target:'UDF2', type:'direct', value:'TRANSTATUS'},
  {target:'UDF3', type:'direct', value:'SUBSCRIBERSSN'},
  {target:'UDF4', type:'direct', value:'PLANPAID'},
  {target:'UDF5', type:'direct', value:'PATIENTPAID'},
  {target:'UDF6', type:'direct', value:'GNN'},
  {target:'UDF7', type:'direct', value:'OOP'},
  {target:'UDF8', type:'direct', value:'SPECIALTY'},
  {target:'UDF9', type:'direct', value:'PCN'},
  {target:'UDF10', type:'null', value:''},
];

const TYPE_META = {
  null:       {label:'NULL',       color:'#ef4444', bg:'rgba(239,68,68,0.12)',   border:'rgba(239,68,68,0.3)'},
  direct:     {label:'DIRECT',     color:'#22c55e', bg:'rgba(34,197,94,0.12)',   border:'rgba(34,197,94,0.3)'},
  hardcoded:  {label:'HARDCODED',  color:'#f59e0b', bg:'rgba(245,158,11,0.12)', border:'rgba(245,158,11,0.3)'},
  expression: {label:'EXPRESSION', color:'#a855f7', bg:'rgba(168,85,247,0.12)', border:'rgba(168,85,247,0.3)'},
};
const TYPE_ORDER = ['null','direct','hardcoded','expression'];

// ── SQL GENERATOR ─────────────────────────────────────────────────────────────
function generateSQL(cfg, rawFields, mappings) {
  const esc = s => s.replace(/'/g, "''");
  const pad = (n, s) => ' '.repeat(n) + s;

  const dedupCols = rawFields
    .filter(f => !RAW_MAX_FIELDS.has(f))
    .map(f => pad(20, `,UPPER(NULLIF(TRIM(${f}), '')) AS ${f}`))
    .join('\n');

  const maxCols = [...RAW_MAX_FIELDS]
    .filter(f => rawFields.includes(f))
    .map(f => pad(20, `,MAX(${f}) AS ${f}`))
    .join('\n');

  const ddlBody = STAGE_DDL_COLS
    .map((c,i) => pad(22, (i===0?'':',') + c))
    .join('\n');

  const insertBody = mappings.map((m, i) => {
    let expr;
    if (m.type === 'null')       expr = 'NULL';
    else if (m.type === 'direct') expr = m.value;
    else if (m.type === 'hardcoded') expr = `''${esc(m.value)}''`;
    else expr = esc(m.value);
    const prefix = i === 0 ? pad(21,'') : pad(21,', ');
    return `${prefix}${expr} AS ${m.target}`;
  }).join('\n');

  return `CREATE OR REPLACE PROCEDURE ${cfg.schema}.SP_SCRUB_STREAM_${cfg.clientName}_PHARMACY()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE

  PHARMACY_STAGE_TABLE TEXT DEFAULT 'MAP_PHARMACY_PLATFORM_STAGE';
  PHARMACY_TABLE       TEXT DEFAULT 'MAP_PHARMACY_PLATFORM';

  MASTER_DRUG       TEXT DEFAULT '${cfg.masterDrug}';
  MASTER_PROVIDERS  TEXT DEFAULT '${cfg.masterProviders}';

  CURRENTTIMESTAMP  TEXT;
  SOURCE_FILE_NAME  TEXT;
  SOURCE_FILE_ARRAY ARRAY;
  ERROR_MESSAGE     TEXT;

BEGIN

  EXECUTE IMMEDIATE 'USE WAREHOUSE ${cfg.warehouse}';
  EXECUTE IMMEDIATE 'USE DATABASE ${cfg.database}';
  EXECUTE IMMEDIATE 'USE SCHEMA ${cfg.schema}';

  EXECUTE IMMEDIATE 'CREATE OR REPLACE TEMPORARY TABLE RAW_PHARMACY_STREAM_CONSUMED AS SELECT * FROM ${cfg.streamTable}';

  SELECT ARRAY_AGG(DISTINCT SOURCEFILENAME) INTO SOURCE_FILE_ARRAY FROM RAW_PHARMACY_STREAM_CONSUMED;
  SELECT TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDD_HH24MISS') INTO CURRENTTIMESTAMP FROM DUAL;

  EXECUTE IMMEDIATE 'CREATE OR REPLACE TEMPORARY TABLE RECEIVED_RAW_PHARMACY_'||CURRENTTIMESTAMP||'
                     AS SELECT DISTINCT
${dedupCols}
${maxCols}
                     FROM RAW_PHARMACY_STREAM_CONSUMED GROUP BY ALL';

  EXECUTE IMMEDIATE 'CREATE OR REPLACE TABLE '||PHARMACY_STAGE_TABLE||'
                     (
${ddlBody}
                     )';

  EXECUTE IMMEDIATE 'INSERT INTO '||PHARMACY_STAGE_TABLE||'
                     SELECT
${insertBody}
                     FROM RECEIVED_RAW_PHARMACY_'||CURRENTTIMESTAMP||' A';

  EXECUTE IMMEDIATE 'UPDATE '||PHARMACY_STAGE_TABLE||' A
                        SET
                          A.PHARMACY_NAME     = UPPER(B.PROVIDER_NAME)
                        , A.PHARMACY_STATE    = UPPER(B.PROVIDER_BUSINESS_PRACTICE_STATE)
                        , A.PHARMACY_ZIP_CODE = UPPER(B.PROVIDER_BUSINESS_PRACTICE_POSTAL_CODE)
                        FROM '||MASTER_PROVIDERS||' B
                        WHERE TRIM(A.PHARMACY_NPI) = TRIM(B.NPI)';

  EXECUTE IMMEDIATE 'UPDATE '||PHARMACY_STAGE_TABLE||' A
                        SET
                          A.PRESCRIBER_FIRST_NAME  = COALESCE(UPPER(B.INDIVIDUAL_PROVIDER_FIRST_NAME), UPPER(B.PROVIDER_NAME))
                        , A.PRESCRIBER_MIDDLE_NAME = UPPER(B.INDIVIDUAL_PROVIDER_MIDDLE_NAME)
                        , A.PRESCRIBER_LAST_NAME   = UPPER(B.INDIVIDUAL_PROVIDER_LAST_NAME)
                        FROM '||MASTER_PROVIDERS||' B
                        WHERE TRIM(A.PRESCRIBER_ID) = TRIM(B.NPI)';

  EXECUTE IMMEDIATE 'UPDATE '||PHARMACY_STAGE_TABLE||' A
                        SET
                          A.DRUG_NAME       = COALESCE(UPPER(B.PROPRIETARY_NAME_UPDATED),UPPER(B.PROPRIETARYNAME),UPPER(B.NONPROPRIETARYNAME),UPPER(B.SUBSTANCENAME))
                        , A.DRUG_DOSAGE     = UPPER(B.DOSAGEFORMNAME)
                        , A.NDC_DESCRIPTION = UPPER(TRIM(B.PACKAGEDESCRIPTION))
                        FROM '||MASTER_DRUG||' B
                        WHERE TRIM(A.NDC_CODE) = TRIM(B.NDC_CODE)';

  EXECUTE IMMEDIATE 'INSERT INTO '||PHARMACY_TABLE||'
                     SELECT * FROM '||PHARMACY_STAGE_TABLE||' A
                     WHERE NOT EXISTS (
                       SELECT 1 FROM '||PHARMACY_TABLE||' B WHERE
                       A.UDF1||A.CLAIM_ID||IFNULL(A.PHARMACY_NPI,''0'')||IFNULL(A.FILLED_DATE,''9999-12-31'')||IFNULL(A.PAID_AMOUNT,''0'')
                       =
                       B.UDF1||B.CLAIM_ID||IFNULL(B.PHARMACY_NPI,''0'')||IFNULL(B.FILLED_DATE,''9999-12-31'')||IFNULL(B.PAID_AMOUNT,''0'')
                     )';

  SOURCE_FILE_NAME := ARRAY_TO_STRING(SOURCE_FILE_ARRAY, CHR(10) || CHR(13));

  CALL REFDATA.DATA_DEV.SP_SEND_EMAIL(
    'Success - ${cfg.clientName} Pharmacy Stream Data Processing.',
    '${cfg.clientName} Pharmacy Stream Data has been processed successfully.'
    || CHR(10) || CHR(13) || 'SourceFileName: ' || :SOURCE_FILE_NAME
  );

  EXCEPTION
    WHEN OTHER THEN
      ERROR_MESSAGE := REGEXP_REPLACE(SQLERRM, '[^[:alnum:]]', ' ');
      CALL REFDATA.DATA_DEV.SP_SEND_EMAIL(
        'Error in ${cfg.clientName} Pharmacy Stream Data Processing.',
        'Issue encountered in ${cfg.clientName} pharmacy stream data processing.'
        || CHR(10) || CHR(13) || 'SourceFileName(s): ' || :SOURCE_FILE_NAME
        || CHR(10) || CHR(13) || 'Error Code: '    || :SQLCODE
        || CHR(10) || CHR(13) || 'Error Message: ' || :ERROR_MESSAGE
      );

  RETURN 'Pharmacy Stream Data Scrub for ${cfg.clientName} Completed.';

END;
$$;`;
}

// ── COMPONENT ─────────────────────────────────────────────────────────────────
const S = {
  app: {fontFamily:"'Sora',sans-serif", background:'#0b0f1a', minHeight:'100vh', color:'#e2e8f0'},
  header: {background:'#111827', borderBottom:'1px solid #1f2937', padding:'0 24px', height:56, display:'flex', alignItems:'center', justifyContent:'space-between', position:'sticky', top:0, zIndex:100},
  logo: {display:'flex', alignItems:'center', gap:10},
  logoIcon: {width:32, height:32, borderRadius:8, background:'linear-gradient(135deg,#3b82f6,#8b5cf6)', display:'flex', alignItems:'center', justifyContent:'center', fontSize:16},
  logoText: {fontSize:15, fontWeight:700, letterSpacing:'-0.3px'},
  logoSub: {fontSize:11, color:'#6b7280', letterSpacing:'0.5px'},
  nav: {display:'flex', background:'#111827', borderBottom:'1px solid #1f2937', padding:'0 24px'},
  navTab: (active) => ({
    padding:'12px 20px', fontSize:12, fontWeight:600, letterSpacing:'0.5px',
    cursor:'pointer', border:'none', background:'transparent',
    color: active ? '#3b82f6' : '#6b7280',
    borderBottom: active ? '2px solid #3b82f6' : '2px solid transparent',
    textTransform:'uppercase', transition:'all 0.15s',
  }),
  main: {padding:24, maxWidth:1400, margin:'0 auto'},
  card: {background:'#111827', border:'1px solid #1f2937', borderRadius:12, padding:24},
  label: {fontSize:11, fontWeight:600, letterSpacing:'0.8px', color:'#6b7280', textTransform:'uppercase', marginBottom:6},
  input: {width:'100%', background:'#1a2035', border:'1px solid #2d3748', borderRadius:8, padding:'8px 12px', color:'#e2e8f0', fontSize:13, fontFamily:"'DM Mono',monospace", outline:'none'},
  row: {display:'flex', gap:16, marginBottom:16},
  col: (flex=1) => ({flex}),
  badge: (type) => ({
    display:'inline-block', padding:'2px 8px', borderRadius:4, fontSize:10, fontWeight:700,
    letterSpacing:'0.6px', cursor:'pointer', border:`1px solid ${TYPE_META[type].border}`,
    background:TYPE_META[type].bg, color:TYPE_META[type].color, whiteSpace:'nowrap',
    userSelect:'none', transition:'opacity 0.1s',
  }),
  typeSelect: {background:'#1a2035', border:'1px solid #2d3748', borderRadius:6, padding:'4px 8px', color:'#e2e8f0', fontSize:12, cursor:'pointer', outline:'none'},
  tableContainer: {overflowY:'auto', maxHeight:'calc(100vh - 280px)', borderRadius:10, border:'1px solid #1f2937'},
  table: {width:'100%', borderCollapse:'collapse', fontSize:12},
  th: {background:'#0d1320', padding:'10px 14px', textAlign:'left', fontSize:10, fontWeight:700, letterSpacing:'0.8px', color:'#4b5563', textTransform:'uppercase', position:'sticky', top:0, zIndex:10},
  tdBase: {padding:'7px 14px', borderBottom:'1px solid #151d2e', verticalAlign:'middle'},
  codeBlock: {background:'#0d1320', borderRadius:10, border:'1px solid #1f2937', padding:20, fontFamily:"'DM Mono',monospace", fontSize:12, color:'#c9d1d9', overflowX:'auto', overflowY:'auto', maxHeight:'calc(100vh - 250px)', whiteSpace:'pre', lineHeight:1.7},
  btn: (variant='primary') => ({
    padding:'9px 18px', borderRadius:8, border:'none', cursor:'pointer', fontSize:12,
    fontWeight:600, letterSpacing:'0.3px', fontFamily:"'Sora',sans-serif",
    ...(variant==='primary' ? {background:'#3b82f6', color:'#fff'} :
        variant==='success' ? {background:'rgba(34,197,94,0.15)', color:'#22c55e', border:'1px solid rgba(34,197,94,0.3)'} :
        variant==='danger'  ? {background:'rgba(239,68,68,0.15)', color:'#ef4444', border:'1px solid rgba(239,68,68,0.3)'} :
                              {background:'#1a2035', color:'#9ca3af', border:'1px solid #2d3748'}),
  }),
  pillFilter: (active) => ({
    padding:'5px 14px', borderRadius:20, fontSize:11, fontWeight:600, cursor:'pointer',
    border:`1px solid ${active ? '#3b82f6' : '#2d3748'}`,
    background: active ? 'rgba(59,130,246,0.15)' : 'transparent',
    color: active ? '#3b82f6' : '#6b7280',
    letterSpacing:'0.3px', transition:'all 0.15s',
  }),
};

export default function PharmacyMapper() {
  const [tab, setTab] = useState('config');
  const [cfg, setCfg] = useState({
    clientName:'MJHLIFESCIENCES', warehouse:'MJHLIFESCIENCES',
    database:'DATA_WAREHOUSE_DEV', schema:'MJHLIFESCIENCES_SCRUB',
    streamTable:'RAW_PHARMACY_STREAM',
    masterDrug:'REFDATA.DATA_DEV.MASTER_DRUG',
    masterProviders:'REFDATA.GLOBAL_PROD.MASTER_PROVIDERS',
  });
  const [rawFields, setRawFields] = useState(SLATERX_RAW_FIELDS);
  const [maps, setMaps] = useState(SLATERX_DEFAULTS.map(m=>({...m})));
  const [typeFilter, setTypeFilter] = useState('all');
  const [search, setSearch] = useState('');
  const [newField, setNewField] = useState('');
  const [pasteText, setPasteText] = useState('');
  const [dragIdx, setDragIdx] = useState(null);
  const [sqlOutput, setSqlOutput] = useState('');
  const [copied, setCopied] = useState(false);
  const [saved, setSaved] = useState(false);
  const textareaRefs = useRef({});

  // Storage persist/load
  useEffect(() => {
    (async () => {
      try {
        const c = await window.storage.get('px_cfg');
        const f = await window.storage.get('px_fields');
        const m = await window.storage.get('px_maps');
        if (c) setCfg(JSON.parse(c.value));
        if (f) setRawFields(JSON.parse(f.value));
        if (m) setMaps(JSON.parse(m.value));
      } catch {}
    })();
  }, []);

  const persist = useCallback(async (c,f,m) => {
    try {
      await window.storage.set('px_cfg', JSON.stringify(c));
      await window.storage.set('px_fields', JSON.stringify(f));
      await window.storage.set('px_maps', JSON.stringify(m));
    } catch {}
  },[]);

  useEffect(() => { persist(cfg, rawFields, maps); }, [cfg, rawFields, maps]);

  const updateMap = (idx, key, val) => {
    setMaps(prev => {
      const next = [...prev];
      next[idx] = {...next[idx], [key]:val};
      return next;
    });
  };

  const cycleType = (idx) => {
    setMaps(prev => {
      const next = [...prev];
      const cur = TYPE_ORDER.indexOf(next[idx].type);
      next[idx] = {...next[idx], type: TYPE_ORDER[(cur+1)%4], value:''};
      return next;
    });
  };

  const handleGenerate = () => {
    const sql = generateSQL(cfg, rawFields, maps);
    setSqlOutput(sql);
    setTab('generate');
  };

  const handleCopy = () => {
    navigator.clipboard.writeText(sqlOutput).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  };

  const handleDownload = () => {
    const blob = new Blob([sqlOutput], {type:'text/plain'});
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `SP_SCRUB_STREAM_${cfg.clientName}_PHARMACY.sql`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const handleResetToSlaterx = () => {
    setRawFields(SLATERX_RAW_FIELDS);
    setMaps(SLATERX_DEFAULTS.map(m=>({...m})));
    setCfg(prev => ({...prev, clientName:'MJHLIFESCIENCES', warehouse:'MJHLIFESCIENCES',
      database:'DATA_WAREHOUSE_DEV', schema:'MJHLIFESCIENCES_SCRUB',
      streamTable:'RAW_PHARMACY_STREAM'}));
  };

  const handleAddField = () => {
    const f = newField.trim().toUpperCase();
    if (f && !rawFields.includes(f)) {
      setRawFields(prev => [...prev, f]);
      setNewField('');
    }
  };

  const handlePasteAdd = () => {
    const fields = pasteText
      .split(/[\n\t,;| ]+/)
      .map(f => f.trim().toUpperCase())
      .filter(f => f && !rawFields.includes(f));
    if (fields.length) {
      setRawFields(prev => [...prev, ...fields]);
      setPasteText('');
    }
  };

  const handleDragStart = (idx) => setDragIdx(idx);

  const handleDragOver = (e, overIdx) => {
    e.preventDefault();
    if (dragIdx === null || dragIdx === overIdx) return;
    setRawFields(prev => {
      const next = [...prev];
      const [moved] = next.splice(dragIdx, 1);
      next.splice(overIdx, 0, moved);
      return next;
    });
    setDragIdx(overIdx);
  };

  const handleDragEnd = () => setDragIdx(null);

  const filteredMaps = maps.map((m,i) => ({...m,_i:i})).filter(m => {
    const matchType = typeFilter === 'all' || m.type === typeFilter;
    const matchSearch = !search || m.target.toLowerCase().includes(search.toLowerCase()) ||
                        m.value.toLowerCase().includes(search.toLowerCase());
    return matchType && matchSearch;
  });

  const counts = {all: maps.length};
  TYPE_ORDER.forEach(t => { counts[t] = maps.filter(m=>m.type===t).length; });

  const cfgField = (key, label, placeholder='') => (
    <div style={S.col()}>
      <div style={S.label}>{label}</div>
      <input style={S.input} value={cfg[key]} placeholder={placeholder}
        onChange={e => setCfg(p => ({...p,[key]:e.target.value.toUpperCase()}))} />
    </div>
  );

  return (
    <div style={S.app}>
      <style>{GLOBAL_STYLE}</style>

      {/* Header */}
      <header style={S.header}>
        <div style={S.logo}>
          <div style={S.logoIcon}>⚡</div>
          <div>
            <div style={S.logoText}>Pharmacy Scrub Builder</div>
            <div style={S.logoSub}>SNOWFLAKE SP GENERATOR · PHARMACY CLAIMS</div>
          </div>
        </div>
        <div style={{display:'flex', gap:10, alignItems:'center'}}>
          <div style={{fontSize:11, color:'#4b5563', fontFamily:"'DM Mono',monospace"}}>
            {maps.filter(m=>m.type!=='null').length}/{maps.length} fields mapped
          </div>
          <button style={S.btn('ghost')} onClick={handleResetToSlaterx}>↺ Reset to SlateRx</button>
          <button style={S.btn('primary')} onClick={handleGenerate}>⚙ Generate SQL</button>
        </div>
      </header>

      {/* Nav */}
      <nav style={S.nav}>
        {['config','fields','mappings','generate'].map(t => (
          <button key={t} style={S.navTab(tab===t)} onClick={() => {
            if (t === 'generate') handleGenerate(); else setTab(t);
          }}>
            {t==='config' && '01 · Client Config'}
            {t==='fields' && `02 · Raw Fields (${rawFields.length})`}
            {t==='mappings' && `03 · Mappings (${maps.filter(m=>m.type!=='null').length} active)`}
            {t==='generate' && '04 · Generate SQL'}
          </button>
        ))}
      </nav>

      {/* ── TAB: CONFIG ─────────────────────────────── */}
      {tab === 'config' && (
        <div style={S.main}>
          <div style={S.card}>
            <div style={{marginBottom:20}}>
              <div style={{fontSize:16, fontWeight:700, marginBottom:4}}>Client Configuration</div>
              <div style={{fontSize:12, color:'#6b7280'}}>These values are injected directly into the generated stored procedure.</div>
            </div>
            <div style={S.row}>
              {cfgField('clientName','Client Name','MJHLIFESCIENCES')}
              {cfgField('warehouse','Warehouse','MJHLIFESCIENCES')}
            </div>
            <div style={S.row}>
              {cfgField('database','Database','DATA_WAREHOUSE_DEV')}
              {cfgField('schema','Schema','MJHLIFESCIENCES_SCRUB')}
            </div>
            <div style={S.row}>
              {cfgField('streamTable','Stream Table','RAW_PHARMACY_STREAM')}
              <div style={S.col()}>{/* spacer */}</div>
            </div>
            <div style={{height:1, background:'#1f2937', margin:'20px 0'}}/>
            <div style={{marginBottom:12, fontSize:13, fontWeight:600, color:'#9ca3af'}}>Reference Tables</div>
            <div style={S.row}>
              {cfgField('masterDrug','Master Drug Table','REFDATA.DATA_DEV.MASTER_DRUG')}
              {cfgField('masterProviders','Master Providers Table','REFDATA.GLOBAL_PROD.MASTER_PROVIDERS')}
            </div>
          </div>

          <div style={{marginTop:20, padding:16, background:'rgba(59,130,246,0.06)', border:'1px solid rgba(59,130,246,0.2)', borderRadius:10}}>
            <div style={{fontSize:12, fontWeight:600, color:'#3b82f6', marginBottom:6}}>📋 Generated Procedure Name</div>
            <div style={{fontFamily:"'DM Mono',monospace", fontSize:13, color:'#e2e8f0'}}>
              {cfg.schema}.SP_SCRUB_STREAM_{cfg.clientName}_PHARMACY()
            </div>
          </div>
        </div>
      )}

      {/* ── TAB: RAW FIELDS ─────────────────────────── */}
      {tab === 'fields' && (
        <div style={S.main}>
          <div style={S.card}>
            <div style={{display:'flex', justifyContent:'space-between', alignItems:'flex-start', marginBottom:20}}>
              <div>
                <div style={{fontSize:16, fontWeight:700, marginBottom:4}}>Raw Source Fields</div>
                <div style={{fontSize:12, color:'#6b7280'}}>Drag cards to reorder · Fields generate the UPPER/TRIM dedup block.</div>
              </div>
              <div style={{display:'flex', gap:8, alignItems:'center'}}>
                <span style={{fontSize:11, color:'#6b7280', fontFamily:"'DM Mono',monospace"}}>{rawFields.length} fields</span>
                <button style={S.btn('danger')} onClick={() => setRawFields([])}>
                  🗑 Clear All
                </button>
              </div>
            </div>

            {/* Single field add */}
            <div style={{...S.label}}>Add single field</div>
            <div style={{display:'flex', gap:10, marginBottom:20}}>
              <input style={{...S.input, flex:1}} placeholder="Type field name e.g. TRANSACTIONID"
                value={newField} onChange={e => setNewField(e.target.value.toUpperCase())}
                onKeyDown={e => e.key === 'Enter' && handleAddField()} />
              <button style={S.btn('primary')} onClick={handleAddField}>+ Add</button>
            </div>

            {/* Paste block */}
            <div style={{...S.label}}>Paste multiple fields (tab / newline / comma / pipe separated)</div>
            <div style={{display:'flex', gap:10, marginBottom:24}}>
              <textarea rows={3} style={{...S.input, flex:1, resize:'vertical'}}
                placeholder={"TRANSACTIONID\tMEMBERID\tNDC\t...  or paste a full header row here"}
                value={pasteText}
                onChange={e => setPasteText(e.target.value)}
              />
              <div style={{display:'flex', flexDirection:'column', gap:8}}>
                <button style={{...S.btn('primary'), whiteSpace:'nowrap'}} onClick={handlePasteAdd}>+ Add Fields</button>
                <button style={{...S.btn('ghost'), whiteSpace:'nowrap'}} onClick={() => setPasteText('')}>Clear</button>
              </div>
            </div>

            {/* Fields grid — draggable */}
            <div style={{display:'grid', gridTemplateColumns:'repeat(auto-fill,minmax(220px,1fr))', gap:8}}>
              {rawFields.map((f, idx) => (
                <div key={f}
                  draggable
                  onDragStart={() => handleDragStart(idx)}
                  onDragOver={e => handleDragOver(e, idx)}
                  onDragEnd={handleDragEnd}
                  style={{
                    display:'flex', alignItems:'center', justifyContent:'space-between',
                    background: dragIdx === idx ? '#1e2d4a' : '#1a2035',
                    border: `1px solid ${dragIdx === idx ? '#3b82f6' : '#2d3748'}`,
                    borderRadius:6, padding:'6px 10px', cursor:'grab',
                    opacity: dragIdx === idx ? 0.5 : 1, transition:'border-color 0.1s, background 0.1s',
                  }}>
                  <div style={{display:'flex', alignItems:'center', gap:8, minWidth:0}}>
                    <span style={{color:'#374151', fontSize:13, flexShrink:0}}>⠿</span>
                    <span style={{fontSize:10, color:'#4b5563', fontFamily:"'DM Mono',monospace", flexShrink:0}}>
                      {String(idx+1).padStart(2,'0')}
                    </span>
                    <span style={{fontFamily:"'DM Mono',monospace", fontSize:11, color:'#a5b4fc',
                      overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap'}}>{f}</span>
                  </div>
                  <span style={{cursor:'pointer', color:'#4b5563', fontSize:16, lineHeight:1, flexShrink:0, marginLeft:6}}
                    onClick={() => setRawFields(prev => prev.filter(x => x !== f))}>×</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* ── TAB: MAPPINGS ───────────────────────────── */}
      {tab === 'mappings' && (
        <div style={S.main}>
          {/* Toolbar */}
          <div style={{display:'flex', gap:10, alignItems:'center', marginBottom:14, flexWrap:'wrap'}}>
            <input style={{...S.input, width:240}} placeholder="Search target field or expression..."
              value={search} onChange={e=>setSearch(e.target.value)} />
            <div style={{display:'flex', gap:6, flexWrap:'wrap'}}>
              {['all',...TYPE_ORDER].map(t => (
                <button key={t} style={S.pillFilter(typeFilter===t)} onClick={()=>setTypeFilter(t)}>
                  {t.toUpperCase()} {counts[t] || ''}
                </button>
              ))}
            </div>
            <div style={{marginLeft:'auto', fontSize:11, color:'#4b5563'}}>
              Click type badge to cycle · Edit expression inline
            </div>
          </div>

          {/* Table */}
          <div style={S.tableContainer}>
            <table style={S.table}>
              <thead>
                <tr>
                  <th style={{...S.th, width:40}}>#</th>
                  <th style={S.th}>Target Field</th>
                  <th style={{...S.th, width:110}}>Type</th>
                  <th style={S.th}>Expression / Value</th>
                </tr>
              </thead>
              <tbody>
                {filteredMaps.map((m) => {
                  const i = m._i;
                  const isNull = m.type === 'null';
                  return (
                    <tr key={m.target} style={{background: i%2===0 ? '#0f1521' : 'transparent'}}>
                      <td style={{...S.tdBase, color:'#374151', fontFamily:"'DM Mono',monospace", fontSize:10}}>{i+1}</td>
                      <td style={{...S.tdBase}}>
                        <span style={{fontFamily:"'DM Mono',monospace", fontSize:11, color: isNull ? '#4b5563' : '#e2e8f0'}}>
                          {m.target}
                        </span>
                      </td>
                      <td style={S.tdBase}>
                        <span style={S.badge(m.type)} onClick={() => cycleType(i)}
                          title="Click to cycle type">
                          {TYPE_META[m.type].label}
                        </span>
                      </td>
                      <td style={{...S.tdBase, width:'55%'}}>
                        {isNull ? (
                          <span style={{fontFamily:"'DM Mono',monospace", fontSize:11, color:'#374151'}}>— NULL —</span>
                        ) : m.type === 'direct' ? (
                          <select style={{...S.typeSelect, width:'100%'}} value={m.value}
                            onChange={e => updateMap(i,'value',e.target.value)}>
                            <option value="">— select raw field —</option>
                            {rawFields.map(f => <option key={f} value={f}>{f}</option>)}
                          </select>
                        ) : m.type === 'hardcoded' ? (
                          <input style={{...S.input, width:'100%', padding:'5px 10px'}}
                            placeholder="Literal value (no quotes needed)"
                            value={m.value} onChange={e=>updateMap(i,'value',e.target.value)} />
                        ) : (
                          <textarea
                            ref={el => textareaRefs.current[i] = el}
                            style={{...S.input, width:'100%', resize:'vertical', minHeight:36, padding:'5px 10px', lineHeight:1.5}}
                            value={m.value} rows={m.value.length > 80 ? 3 : 1}
                            onChange={e=>updateMap(i,'value',e.target.value)}
                            placeholder="SQL expression (single quotes as-is, they'll be auto-escaped)" />
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* ── TAB: GENERATE ───────────────────────────── */}
      {tab === 'generate' && (
        <div style={S.main}>
          <div style={{display:'flex', gap:10, alignItems:'center', marginBottom:16}}>
            <div style={{flex:1}}>
              <div style={{fontSize:15, fontWeight:700}}>Generated Stored Procedure</div>
              <div style={{fontSize:11, color:'#6b7280', marginTop:3, fontFamily:"'DM Mono',monospace"}}>
                {cfg.schema}.SP_SCRUB_STREAM_{cfg.clientName}_PHARMACY.sql
              </div>
            </div>
            <div style={{display:'flex', gap:8}}>
              <button style={S.btn(copied ? 'success' : 'ghost')} onClick={handleCopy}>
                {copied ? '✓ Copied!' : '📋 Copy All'}
              </button>
              <button style={S.btn('primary')} onClick={handleDownload}>⬇ Download .sql</button>
            </div>
          </div>

          {/* Stats row */}
          <div style={{display:'flex', gap:10, marginBottom:16, flexWrap:'wrap'}}>
            {TYPE_ORDER.map(t => (
              <div key={t} style={{padding:'8px 14px', borderRadius:8, background:TYPE_META[t].bg,
                border:`1px solid ${TYPE_META[t].border}`, display:'flex', gap:8, alignItems:'center'}}>
                <span style={{fontSize:18, fontWeight:800, color:TYPE_META[t].color}}>{counts[t]}</span>
                <span style={{fontSize:10, fontWeight:700, color:TYPE_META[t].color, letterSpacing:'0.5px'}}>{t.toUpperCase()}</span>
              </div>
            ))}
            <div style={{padding:'8px 14px', borderRadius:8, background:'rgba(99,102,241,0.1)',
              border:'1px solid rgba(99,102,241,0.3)', display:'flex', gap:8, alignItems:'center'}}>
              <span style={{fontSize:18, fontWeight:800, color:'#818cf8'}}>{rawFields.length}</span>
              <span style={{fontSize:10, fontWeight:700, color:'#818cf8', letterSpacing:'0.5px'}}>RAW FIELDS</span>
            </div>
          </div>

          {!sqlOutput ? (
            <div style={{...S.codeBlock, textAlign:'center', padding:40, color:'#4b5563'}}>
              Click "Generate SQL" above or in the header to build the script
            </div>
          ) : (
            <div style={S.codeBlock}>{sqlOutput}</div>
          )}
        </div>
      )}
    </div>
  );
}