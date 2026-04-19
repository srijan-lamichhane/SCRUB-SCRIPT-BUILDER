import { PX_MAX_FIELDS, PX_STAGE_DDL } from '../constants/pharmacy.js';

export function generatePharmacySQL(cfg, rawFields, maps) {

  const esc = s => s.replace(/'/g, "''");
  const pad = (n, s) => ' '.repeat(n) + s;
  const dedupCols = rawFields.filter(f => !PX_MAX_FIELDS.has(f))
    .map(f => pad(20, `,UPPER(NULLIF(TRIM(${f}), '''')) AS ${f}`)).join('\n');
  const maxCols = [...PX_MAX_FIELDS].filter(f => rawFields.includes(f))
    .map(f => pad(20, `,MAX(${f}) AS ${f}`)).join('\n');
  const ddlBody = PX_STAGE_DDL.map((c,i) => pad(22, (i===0?'':',')+c)).join('\n');
  const insertBody = maps.map((m,i) => {
    let expr = m.type==='null' ? 'NULL' : m.type==='direct' ? m.value
      : m.type==='hardcoded' ? `''${esc(m.value)}''` : esc(m.value);
    return `${i===0 ? pad(21,'') : pad(21,', ')}${expr} AS ${m.target}`;
  }).join('\n');

  return `CREATE OR REPLACE PROCEDURE REFDATA.DATA_DEV.SP_SCRUB_STREAM_${cfg.clientName}_PHARMACY()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  PHARMACY_STAGE_TABLE TEXT DEFAULT 'MAP_PHARMACY_PLATFORM_STAGE';
  PHARMACY_TABLE       TEXT DEFAULT 'MAP_PHARMACY_PLATFORM';
  MASTER_DRUG          TEXT DEFAULT '${cfg.masterDrug}';
  MASTER_PROVIDERS     TEXT DEFAULT '${cfg.masterProviders}';
  CURRENTTIMESTAMP     TEXT;
  SOURCE_FILE_NAME     TEXT;
  SOURCE_FILE_ARRAY    ARRAY;
  ERROR_MESSAGE        TEXT;
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
                        SET A.PHARMACY_NAME     = UPPER(B.PROVIDER_NAME)
                          , A.PHARMACY_STATE    = UPPER(B.PROVIDER_BUSINESS_PRACTICE_STATE)
                          , A.PHARMACY_ZIP_CODE = UPPER(B.PROVIDER_BUSINESS_PRACTICE_POSTAL_CODE)
                        FROM '||MASTER_PROVIDERS||' B
                        WHERE TRIM(A.PHARMACY_NPI) = TRIM(B.NPI)';

  EXECUTE IMMEDIATE 'UPDATE '||PHARMACY_STAGE_TABLE||' A
                        SET A.PRESCRIBER_FIRST_NAME  = COALESCE(UPPER(B.INDIVIDUAL_PROVIDER_FIRST_NAME), UPPER(B.PROVIDER_NAME))
                          , A.PRESCRIBER_MIDDLE_NAME = UPPER(B.INDIVIDUAL_PROVIDER_MIDDLE_NAME)
                          , A.PRESCRIBER_LAST_NAME   = UPPER(B.INDIVIDUAL_PROVIDER_LAST_NAME)
                        FROM '||MASTER_PROVIDERS||' B
                        WHERE TRIM(A.PRESCRIBER_ID) = TRIM(B.NPI)';

  EXECUTE IMMEDIATE 'UPDATE '||PHARMACY_STAGE_TABLE||' A
                        SET A.DRUG_NAME       = COALESCE(UPPER(B.PROPRIETARY_NAME_UPDATED),UPPER(B.PROPRIETARYNAME),UPPER(B.NONPROPRIETARYNAME),UPPER(B.SUBSTANCENAME))
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
