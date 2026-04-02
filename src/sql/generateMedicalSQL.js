import {
  MED_MAX_FIELDS,
  MED_FIELD_ALIAS,
  MED_HARDCODED_DEDUP,
  MED_STAGE_DDL,
} from '../constants/medical.js';

export function generateMedicalSQL(cfg, rawFields, maps) {

  const esc = s => s.replace(/'/g, "''");
  const pad = (n, s) => ' '.repeat(n) + s;
  const stageTable = `${cfg.clientName}_MEDICAL_STAGE_TABLE`;
  const cleanedTable = `RAW_${cfg.clientName}_MED_CLEANED_`;

  const dedupCols = rawFields.filter(f => !MED_MAX_FIELDS.has(f)).map(f => {
    const alias = MED_FIELD_ALIAS[f] ? ` AS ${MED_FIELD_ALIAS[f]}` : ` AS ${f}`;
    return pad(21, `, UPPER(REPLACE(TRIM(${f}),''))${alias}`);
  }).join('\n');

  const hardcodedDedup = MED_HARDCODED_DEDUP
    .map(f => pad(21, `, '0.00' AS ${f}`)).join('\n');

  const maxCols = [...MED_MAX_FIELDS].filter(f => rawFields.includes(f))
    .map(f => pad(21, `, MAX(${f}) AS ${f}`)).join('\n');

  const ddlBody = MED_STAGE_DDL.map((c,i) => pad(22, (i===0?'':',')+c)).join('\n');

  const insertBody = maps.map((m,i) => {
    let expr = m.type==='null' ? 'NULL' : m.type==='direct' ? m.value
      : m.type==='hardcoded' ? `''${esc(m.value)}''` : esc(m.value);
    return `${i===0 ? pad(21,'') : pad(21,', ')}${expr} AS ${m.target}`;
  }).join('\n');

  return `CREATE OR REPLACE PROCEDURE ${cfg.schema}.SP_SCRUB_STREAM_${cfg.clientName}_MEDICAL()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  MEDICAL_TABLE              TEXT DEFAULT '${cfg.schema}.MAP_MEDICAL_PLATFORM';
  MASTER_PROCEDURE           TEXT DEFAULT '${cfg.masterProcedure}';
  MASTER_PROCEDURE_MODIFIER  TEXT DEFAULT '${cfg.masterProcedureModifier}';
  MASTER_DIAGNOSIS           TEXT DEFAULT '${cfg.masterDiagnosis}';
  MASTER_POS                 TEXT DEFAULT '${cfg.masterPos}';
  MASTER_PROVIDERS           TEXT DEFAULT '${cfg.masterProviders}';
  CURRENTTIMESTAMP           TEXT;
  SOURCE_FILE_NAME           TEXT;
  SOURCE_FILE_ARRAY          ARRAY;
  ERROR_MESSAGE              TEXT;
  query_string               TEXT;
  counter1 INTEGER DEFAULT 0;
  counter2 INTEGER DEFAULT 0;
  counter3 INTEGER DEFAULT 0;
  i INT;
BEGIN
  EXECUTE IMMEDIATE 'USE WAREHOUSE ${cfg.warehouse}';
  EXECUTE IMMEDIATE 'USE DATABASE ${cfg.database}';
  EXECUTE IMMEDIATE 'USE SCHEMA ${cfg.schema}';

  EXECUTE IMMEDIATE 'CREATE OR REPLACE TEMPORARY TABLE RECEIVED_RAW_MEDICAL AS SELECT * FROM ${cfg.streamTable}';
  SELECT ARRAY_AGG(DISTINCT SOURCEFILENAME) INTO SOURCE_FILE_ARRAY FROM RECEIVED_RAW_MEDICAL;
  SELECT TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDD_HH24MISS') INTO CURRENTTIMESTAMP FROM DUAL;

  EXECUTE IMMEDIATE 'CREATE OR REPLACE TEMPORARY TABLE ${cleanedTable}'||CURRENTTIMESTAMP||'
                     AS SELECT DISTINCT
${dedupCols}
${hardcodedDedup}
${maxCols}
                     FROM RECEIVED_RAW_MEDICAL
                     GROUP BY ALL';

  EXECUTE IMMEDIATE 'CREATE OR REPLACE TEMPORARY TABLE ${stageTable}
                     (
${ddlBody}
                     )';

  EXECUTE IMMEDIATE 'INSERT INTO ${stageTable}
                     SELECT
${insertBody}
                     FROM ${cleanedTable}'||CURRENTTIMESTAMP||' A';

  -- UPDATE PROVIDER INFORMATION
  EXECUTE IMMEDIATE 'UPDATE ${stageTable} A
                        SET A.PROVIDER_TYPE                = UPPER(B.PROVIDER_ENTITY_TYPE)
                          , A.PROVIDER_FIRST_NAME          = UPPER(B.INDIVIDUAL_PROVIDER_FIRST_NAME)
                          , A.PROVIDER_MIDDLE_NAME         = UPPER(B.INDIVIDUAL_PROVIDER_MIDDLE_NAME)
                          , A.PROVIDER_LAST_NAME           = UPPER(B.INDIVIDUAL_PROVIDER_LAST_NAME)
                          , A.PROVIDER_NAME                = UPPER(B.PROVIDER_NAME)
                          , A.PROVIDER_SPECIALTY_1_CODE    = UPPER(B.PRIMARY_TAXONOMY)
                          , A.PROVIDER_SPECIALTY_1_DESCRIPTION = UPPER(B.DISPLAY_NAME)
                          , A.PROVIDER_ADDRESS_1           = UPPER(B.PROVIDER_BUSINESS_MAILING_ADDRESS_1)
                          , A.PROVIDER_ADDRESS_2           = UPPER(B.PROVIDER_BUSINESS_MAILING_ADDRESS_2)
                          , A.PROVIDER_CITY                = UPPER(B.PROVIDER_BUSINESS_PRACTICE_CITY)
                          , A.PROVIDER_STATE               = UPPER(B.PROVIDER_BUSINESS_MAILING_STATE)
                          , A.PROVIDER_ZIP_CODE            = UPPER(B.PROVIDER_BUSINESS_MAILING_POSTAL_CODE)
                        FROM '||MASTER_PROVIDERS||' B
                        WHERE TRIM(A.PROVIDER_NPI) = TRIM(B.NPI)';

  EXECUTE IMMEDIATE 'UPDATE ${stageTable} SET FACILITY_NPI = PROVIDER_NPI WHERE PROVIDER_TYPE=''ORGANIZATION'' AND FACILITY_NPI IS NULL';

  EXECUTE IMMEDIATE 'UPDATE ${stageTable} A
                        SET A.CPT_DESCRIPTION = UPPER(B.SHORT_DESCRIPTION), A.CPT_LONG_DESCRIPTION = UPPER(B.LONG_DESCRIPTION)
                        FROM '||MASTER_PROCEDURE||' B
                        WHERE TRIM(UPPER(A.CPT_CODE)) = TRIM(UPPER(B.CODE)) AND UPPER(TRIM(B.PROCEDURE_TYPE))=''CPT4''';

  EXECUTE IMMEDIATE 'UPDATE ${stageTable} A
                        SET A.HCPCS_DESCRIPTION = UPPER(B.SHORT_DESCRIPTION), A.HCPCS_LONG_DESCRIPTION = UPPER(B.LONG_DESCRIPTION)
                        FROM '||MASTER_PROCEDURE||' B
                        WHERE TRIM(UPPER(A.HCPCS_CODE)) = TRIM(UPPER(B.CODE)) AND UPPER(TRIM(B.PROCEDURE_TYPE))=''HCPCS''';

  EXECUTE IMMEDIATE 'UPDATE ${stageTable} A
                        SET A.REVENUE_DESCRIPTION = UPPER(B.SHORT_DESCRIPTION)
                        FROM '||MASTER_PROCEDURE||' B
                        WHERE TRIM(UPPER(REPLACE(A.REVENUE_CODE,''R''))) = TRIM(UPPER(REPLACE(B.CODE,''R''))) AND UPPER(TRIM(B.PROCEDURE_TYPE))=''REV CODE''';

  EXECUTE IMMEDIATE 'UPDATE ${stageTable} A
                        SET A.DRG_DESCRIPTION = UPPER(B.SHORT_DESCRIPTION)
                        FROM '||MASTER_PROCEDURE||' B
                        WHERE TRIM(UPPER(A.DRG_CODE)) = TRIM(UPPER(B.CODE)) AND UPPER(TRIM(B.PROCEDURE_TYPE)) LIKE ''%DRG%''';

  EXECUTE IMMEDIATE 'UPDATE ${stageTable} A
                        SET A.PLACE_OF_SERVICE_DESCRIPTION = UPPER(B.POS_NAME)
                        FROM '||MASTER_POS||' B
                        WHERE TRIM(UPPER(A.PLACE_OF_SERVICE_CODE)) = TRIM(UPPER(B.PLACE_OF_SERVICE_CODE))';

  FOR i IN 1 TO 2 DO
    counter1 := counter1 + 1;
    query_string := 'UPDATE ${stageTable} A
                        SET A.ICD_PROCEDURE_' || i || '_DESCRIPTION = UPPER(B.SHORT_DESCRIPTION)
                        FROM '||MASTER_PROCEDURE||' B
                        WHERE TRIM(UPPER(A.ICD_PROCEDURE_' || i || '_CODE)) = TRIM(UPPER(B.CODE))
                        AND UPPER(TRIM(B.PROCEDURE_TYPE))=''ICD10''';
    EXECUTE IMMEDIATE query_string;
  END FOR;

  FOR i IN 1 TO 3 DO
    counter2 := counter2 + 1;
    query_string := 'UPDATE ${stageTable} A
                    SET A.PROCEDURE_MODIFIER_' || i || '_DESCRIPTION = UPPER(B.DESCRIPTION)
                    FROM '||MASTER_PROCEDURE_MODIFIER||' B
                    WHERE TRIM(UPPER(A.PROCEDURE_MODIFIER_' || i || '_CODE)) = TRIM(UPPER(B.MODIFIER_CODE))';
    EXECUTE IMMEDIATE query_string;
  END FOR;

  FOR i IN 1 TO 25 DO
    counter3 := counter3 + 1;
    query_string := 'UPDATE ${stageTable} A
                    SET A.DIAGNOSIS_' || i || '_DESCRIPTION = UPPER(B.DIAGNOSIS_DESCRIPTION)
                    FROM '||MASTER_DIAGNOSIS||' B
                    WHERE TRIM(UPPER(A.DIAGNOSIS_' || i || '_CODE)) = TRIM(UPPER(B.CODE))';
    EXECUTE IMMEDIATE query_string;
  END FOR;

  EXECUTE IMMEDIATE 'UPDATE ${stageTable} A SET
                     PROCEDURE_CODE = COALESCE(CPT_CODE, HCPCS_CODE, REVENUE_CODE, DRG_CODE, ICD_PROCEDURE_1_CODE, ICD_PROCEDURE_2_CODE),
                     PROCEDURE_TYPE = COALESCE(
                       CASE WHEN CPT_CODE IS NOT NULL THEN ''CPT4'' ELSE NULL END,
                       CASE WHEN HCPCS_CODE IS NOT NULL THEN ''HCPCS'' ELSE NULL END,
                       CASE WHEN REVENUE_CODE IS NOT NULL THEN ''REV CODE'' ELSE NULL END,
                       CASE WHEN DRG_CODE IS NOT NULL THEN ''DRG 27'' ELSE NULL END,
                       CASE WHEN ICD_PROCEDURE_1_CODE IS NOT NULL THEN ''ICD10'' ELSE NULL END),
                     PROCEDURE_DESCRIPTION = COALESCE(
                       CASE WHEN CPT_CODE IS NOT NULL THEN CPT_DESCRIPTION ELSE NULL END,
                       CASE WHEN HCPCS_CODE IS NOT NULL THEN HCPCS_DESCRIPTION ELSE NULL END,
                       CASE WHEN REVENUE_CODE IS NOT NULL THEN REVENUE_DESCRIPTION ELSE NULL END,
                       CASE WHEN DRG_CODE IS NOT NULL THEN DRG_DESCRIPTION ELSE NULL END,
                       CASE WHEN ICD_PROCEDURE_1_CODE IS NOT NULL THEN ICD_PROCEDURE_1_DESCRIPTION ELSE NULL END)';

  -- REVERSAL REMOVAL
  EXECUTE IMMEDIATE 'CREATE OR REPLACE TEMPORARY TABLE ${stageTable}_REVERSAL_REMOVED AS
                     WITH REVERSED_CLAIMS AS (
                       SELECT CLAIM_ID||CLAIM_LINE_ID||SERVICE_FROM_DATE||SERVICE_TO_DATE||COALESCE(PROVIDER_NPI,''X'')||ABS(PAID_AMOUNT)
                       FROM ${stageTable} WHERE UDF8 = ''REVERSAL''
                     ),
                     REVERSED_AND_ORIGINAL_CLAIMS AS (
                       SELECT * FROM ${stageTable}
                       WHERE CLAIM_ID||CLAIM_LINE_ID||SERVICE_FROM_DATE||SERVICE_TO_DATE||COALESCE(PROVIDER_NPI,''X'')||ABS(PAID_AMOUNT) IN (SELECT * FROM REVERSED_CLAIMS)
                       AND UDF8 IN (''ORIGINAL'',''REVERSAL'')
                     )
                     SELECT * FROM ${stageTable} MINUS SELECT * FROM REVERSED_AND_ORIGINAL_CLAIMS';

  -- FINAL INSERT
  EXECUTE IMMEDIATE 'INSERT INTO '||MEDICAL_TABLE||'
                     SELECT * FROM ${stageTable}_REVERSAL_REMOVED A
                     WHERE NOT EXISTS (
                       SELECT 1 FROM '||MEDICAL_TABLE||' B WHERE
                       A.UDF1||A.CLAIM_ID||A.CLAIM_LINE_ID||IFNULL(A.DIAGNOSIS_1_CODE,''X'')||IFNULL(A.PROCEDURE_CODE,''X'')||IFNULL(A.PROVIDER_NPI,''0'')||IFNULL(A.SERVICE_FROM_DATE,''9999-12-31'')||IFNULL(A.PAID_AMOUNT,''0'')
                       =
                       B.UDF1||B.CLAIM_ID||B.CLAIM_LINE_ID||IFNULL(B.DIAGNOSIS_1_CODE,''X'')||IFNULL(B.PROCEDURE_CODE,''X'')||IFNULL(B.PROVIDER_NPI,''0'')||IFNULL(B.SERVICE_FROM_DATE,''9999-12-31'')||IFNULL(B.PAID_AMOUNT,''0'')
                     )';

  SOURCE_FILE_NAME := ARRAY_TO_STRING(SOURCE_FILE_ARRAY, CHR(10) || CHR(13));
  CALL REFDATA.DATA_DEV.SP_SEND_EMAIL(
    'Success - ${cfg.clientName} Medical Stream Data Processing.',
    '${cfg.clientName} Medical Stream Data has been processed successfully.'
    || CHR(10) || CHR(13) || 'SourceFileName: ' || :SOURCE_FILE_NAME
  );

  EXCEPTION
    WHEN OTHER THEN
      ERROR_MESSAGE := REGEXP_REPLACE(SQLERRM, '[^[:alnum:]]', ' ');
      CALL REFDATA.DATA_DEV.SP_SEND_EMAIL(
        'Error in ${cfg.clientName} Medical Stream Data Processing.',
        'Issue encountered in ${cfg.clientName} medical stream data processing.'
        || CHR(10) || CHR(13) || 'SourceFileName(s): ' || :SOURCE_FILE_NAME
        || CHR(10) || CHR(13) || 'Error Code: '    || :SQLCODE
        || CHR(10) || CHR(13) || 'Error Message: ' || :ERROR_MESSAGE
      );
  RETURN 'Medical Stream Data Scrub for ${cfg.clientName} Completed.';
END;
$$;`;
}
