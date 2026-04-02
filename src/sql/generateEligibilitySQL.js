function buildMapSelect(maps, esc, pad) {
  return maps
    .map((m, i) => {
      const expr =
        m.type === 'null'
          ? 'NULL'
          : m.type === 'direct'
            ? m.value
            : m.type === 'hardcoded'
              ? `''${esc(m.value)}''`
              : esc(m.value);
      return `${i === 0 ? pad(21, '') : pad(21, ', ')}${expr} AS ${m.target}`;
    })
    .join('\n');
}

/**
 * Universal eligibility scrub: stream → normalized staging (from mappings) →
 * employee/dependent reconciliation → latest row per member key → enrollment slice →
 * MAP_MEMBER + MAP_ELIGIBILITY loads → employee-id crosswalk → coverage type derivation →
 * employee name enrichment on dependents.
 */
export function generateEligibilitySQL(cfg, rawFields, maps) {
  const esc = s => String(s).replace(/'/g, "''");
  const pad = (n, s) => ' '.repeat(n) + s;

  const control = esc(cfg.controlFileFilter ?? '%CONTROLS%');
  const mk = cfg.memberKeyExpression ?? "UPPER(TRIM(FIRST_NAME))||UPPER(TRIM(LAST_NAME))||DOB||TRIM(UPPER(GENDER))";
  const mk2 = cfg.memberSecondaryKeyExpression ?? 'TRIM(MEMBER_ID)';
  const empRel = esc(cfg.employeeRelationshipValue ?? 'EMPLOYEE');
  const carrierId = esc(cfg.carrierId ?? 'CARRIER');
  const carrierName = esc(cfg.carrierName ?? 'CARRIER');
  const benefitType = esc(cfg.benefitType ?? 'MEDICAL');
  const policyId = esc(cfg.policyId ?? '');
  const defaultChEmp = esc(cfg.defaultChEmployerId ?? '802');
  const planType = cfg.planTypeExpression ?? 'NULL';
  const planId = cfg.planIdExpression ?? 'GROUP_ID';
  const planName = cfg.planNameExpression ?? 'GROUP_ID';

  const dedupCols = rawFields
    .map((f, i) => {
      const col = `UPPER(NULLIF(TRIM(${f}), '')) AS ${f}`;
      return i === 0 ? pad(20, col) : pad(20, ', ' + col);
    })
    .join('\n');

  const insertBody = buildMapSelect(maps, esc, pad);
  const listaggComma = "''" + "," + "''";

  return `CREATE OR REPLACE PROCEDURE ${cfg.schema}.SP_SCRUB_STREAM_${cfg.clientName}_ELIGIBILITY()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  MEMBER_TABLE            TEXT DEFAULT '${esc(cfg.memberTable)}';
  ELIGIBILITY_TABLE       TEXT DEFAULT '${esc(cfg.eligibilityTable)}';
  MASTER_CLIENT_GROUP     TEXT DEFAULT '${esc(cfg.masterClientGroup)}';
  MASTER_ZIP              TEXT DEFAULT '${esc(cfg.masterZip)}';
  CURRENTTIMESTAMP        TEXT;
  SOURCE_FILE_NAME        TEXT;
  SOURCE_FILE_ARRAY       ARRAY;
  ERROR_MESSAGE           TEXT;
BEGIN
  EXECUTE IMMEDIATE 'USE WAREHOUSE ${esc(cfg.warehouse)}';
  EXECUTE IMMEDIATE 'USE DATABASE ${esc(cfg.database)}';
  EXECUTE IMMEDIATE 'USE SCHEMA ${esc(cfg.schema)}';

  EXECUTE IMMEDIATE 'CREATE OR REPLACE TEMPORARY TABLE RAW_ELIGIBILITY_STREAM_CONSUMED AS SELECT * FROM ${esc(cfg.streamTable)}';
  SELECT ARRAY_AGG(DISTINCT SOURCEFILENAME) INTO SOURCE_FILE_ARRAY FROM RAW_ELIGIBILITY_STREAM_CONSUMED;
  SELECT TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDD_HH24MISS') INTO CURRENTTIMESTAMP FROM DUAL;

  EXECUTE IMMEDIATE 'CREATE OR REPLACE TEMPORARY TABLE RECEIVED_RAW_ELIGIBILITY_'||CURRENTTIMESTAMP||'
                     AS SELECT DISTINCT
${dedupCols}
                     FROM RAW_ELIGIBILITY_STREAM_CONSUMED
                     WHERE UPPER(SOURCEFILENAME) NOT LIKE ''${control}''';

  EXECUTE IMMEDIATE 'CREATE OR REPLACE TEMPORARY TABLE STAGING_ELIGIBILITY_'||CURRENTTIMESTAMP||' AS
                     SELECT
${insertBody}
                     FROM RECEIVED_RAW_ELIGIBILITY_'||CURRENTTIMESTAMP||' A';

  EXECUTE IMMEDIATE 'CREATE OR REPLACE TEMPORARY TABLE RECEIVED_ELIGIBILITY_LATEST_INFO_'||CURRENTTIMESTAMP||'
                     AS
                     WITH CTE_RAW AS
                     (
                     SELECT DISTINCT
                       (${mk}) AS MEMBER_KEY1
                     , (${mk2}) AS MEMBER_KEY2
                     , FIRST_VALUE(NULLIF(TRIM(RELATIONSHIP),'''')) IGNORE NULLS OVER (
                         PARTITION BY (${mk}) ORDER BY RECEIVED_DATE DESC, EFFECTIVE_DATE::DATE DESC,
                         COALESCE(NULLIF(TERMINATION_DATE::DATE, ''9999-12-31''), RECEIVED_DATE) DESC
                       ) AS RELATIONTYPE
                     , SOURCEFILENAME
                     , RECEIVED_DATE
                     , EFFECTIVE_DATE
                     , TERMINATION_DATE
                     , TRIM(MEMBER_ID) AS EMPLOYEEID
                     FROM STAGING_ELIGIBILITY_'||CURRENTTIMESTAMP||'
                     ),
                     CTE_EMPLOYEE AS
                     (
                     SELECT DISTINCT
                       MEMBER_KEY1
                     , FIRST_VALUE(TRIM(EMPLOYEEID)) IGNORE NULLS OVER (
                         PARTITION BY MEMBER_KEY1 ORDER BY RECEIVED_DATE DESC, EFFECTIVE_DATE::DATE DESC,
                         COALESCE(NULLIF(TERMINATION_DATE::DATE, ''9999-12-31''), RECEIVED_DATE) DESC
                       ) AS EMPLOYEE_ID
                     FROM CTE_RAW
                     WHERE TRIM(RELATIONTYPE) = ''${empRel}''
                     ),
                     CTE_DEPENDENTS AS
                     (
                     SELECT
                       MEMBER_KEY1
                     , LISTAGG(DISTINCT TRIM(EMPLOYEEID), ${listaggComma}) WITHIN GROUP (ORDER BY TRIM(EMPLOYEEID)) AS EMPLOYEE_ID
                     FROM CTE_RAW
                     WHERE TRIM(RELATIONTYPE) <> ''${empRel}''
                     GROUP BY MEMBER_KEY1
                     ),
                     CTE_ALL AS
                     (
                     SELECT * FROM CTE_EMPLOYEE
                     UNION ALL
                     SELECT * FROM CTE_DEPENDENTS
                     ),
                     CTE_MEMBERID AS
                     (
                     SELECT
                       MEMBER_KEY1
                     , LISTAGG(DISTINCT TRIM(MEMBER_KEY2), ${listaggComma}) WITHIN GROUP (ORDER BY TRIM(MEMBER_KEY2)) AS MEMBER_ID_2
                     FROM (
                       SELECT DISTINCT MEMBER_KEY1, MEMBER_KEY2
                       FROM (
                         SELECT (${mk}) AS MEMBER_KEY1, (${mk2}) AS MEMBER_KEY2
                         FROM STAGING_ELIGIBILITY_'||CURRENTTIMESTAMP||'
                       ) S
                     ) GROUP BY MEMBER_KEY1
                     ),
                     CTE_FIRST_VALUE AS
                     (
                     SELECT * FROM (
                       SELECT
                         S.*,
                         (${mk}) AS MEMBER_KEY1
                       FROM STAGING_ELIGIBILITY_'||CURRENTTIMESTAMP||' S
                     ) Q
                     QUALIFY ROW_NUMBER() OVER (
                       PARTITION BY MEMBER_KEY1
                       ORDER BY RECEIVED_DATE DESC, EFFECTIVE_DATE DESC NULLS LAST, TERMINATION_DATE DESC NULLS LAST
                     ) = 1
                     ),
                     CTE_FINAL AS
                     (
                     SELECT
                       A.* EXCLUDE (EMPLOYEE_ID),
                       TO_VARIANT(SPLIT(B.EMPLOYEE_ID, ${listaggComma})) AS EMPLOYEE_ID,
                       TO_VARIANT(SPLIT(C.MEMBER_ID_2, ${listaggComma})) AS MEMBER_ID_2
                     FROM CTE_FIRST_VALUE A
                     JOIN CTE_ALL B USING (MEMBER_KEY1)
                     JOIN CTE_MEMBERID C USING (MEMBER_KEY1)
                     )
                     SELECT * FROM CTE_FINAL';

  EXECUTE IMMEDIATE 'CREATE OR REPLACE TEMPORARY TABLE RECEIVED_ELIGIBILITY_ENROLLMENT_'||CURRENTTIMESTAMP||'
                     AS
                     SELECT DISTINCT
                       (${mk}) AS MEMBER_KEY1
                     , DATE_TRUNC(''MONTH'', EFFECTIVE_DATE)::DATE AS EFFECTIVE_DATE
                     , CASE
                         WHEN EFFECTIVE_DATE::DATE > LAST_DAY(RECEIVED_DATE::DATE) THEN LAST_DAY(EFFECTIVE_DATE::DATE)
                         WHEN TERMINATION_DATE::DATE > LAST_DAY(RECEIVED_DATE::DATE) THEN LAST_DAY(RECEIVED_DATE::DATE)
                         ELSE LAST_DAY(COALESCE(NULLIF(TERMINATION_DATE::DATE, ''9999-12-31''), RECEIVED_DATE::DATE))
                       END AS TERMINATION_DATE
                     FROM STAGING_ELIGIBILITY_'||CURRENTTIMESTAMP||'';

  EXECUTE IMMEDIATE 'TRUNCATE TABLE '||MEMBER_TABLE;
  EXECUTE IMMEDIATE 'INSERT INTO '||MEMBER_TABLE||'
                     WITH CTE_ZIP AS
                     (
                     SELECT TRIM(ZIP_CODE) AS ZIP_CODE
                          , UPPER(TRIM(CITY)) AS CITY
                          , UPPER(TRIM(STATE)) AS STATE
                          , UPPER(TRIM(COUNTY)) AS COUNTY
                     FROM '||MASTER_ZIP||'
                     QUALIFY ROW_NUMBER() OVER (PARTITION BY ZIP_CODE ORDER BY CITY DESC) = 1
                     )
                     SELECT
                       NULL AS CH_MEMBER_ID
                     , COALESCE(C.CH_EMPLOYER_ID, ''${defaultChEmp}'') AS CH_EMPLOYER_ID
                     , A.MEMBER_ID
                     , A.GROUP_ID
                     , A.EMPLOYEE_ID
                     , A.FIRST_NAME
                     , A.MIDDLE_NAME
                     , A.LAST_NAME
                     , COALESCE(A.SSN, LEFT(A.MEMBER_ID, 9)) AS SSN
                     , A.GENDER
                     , A.DOB
                     , UPPER(A.ADDRESS_1) AS ADDRESS_1
                     , UPPER(A.ADDRESS_2) AS ADDRESS_2
                     , UPPER(COALESCE(A.CITY, B.CITY)) AS CITY
                     , UPPER(B.COUNTY) AS COUNTY
                     , UPPER(COALESCE(A.STATE, B.STATE)) AS STATE
                     , A.ZIP_CODE
                     , A.MARITAL_STATUS
                     , A.RACE
                     , A.ETHNIC_GROUP
                     , A.EDUCATION_LEVEL
                     , A.RELATIONSHIP
                     , A.AGE_RANGE
                     , A.OFFICE_LOCATION
                     , A.ALTERNATE_MEMBER_ID
                     , A.PHONE_HOME
                     , A.PHONE_WORK
                     , A.PHONE_MOBILE
                     , A.EMAIL_ADDRESS_HOME
                     , A.EMAIL_ADDRESS_WORK
                     , A.MEMBER_STATUS
                     , A.PHS_REVIEWED
                     , A.ENGAGED
                     , A.MERGE_DT
                     , A.MERGED_INTO
                     , A.PHA_ENGAGEMENT_DATE
                     , A.CC_ENGAGEMENT_DATE
                     , A.PROGRAM_TYPE_ID
                     , A.FIRST_NAME_EDITED
                     , A.MIDDLE_NAME_EDITED
                     , A.LAST_NAME_EDITED
                     , A.GENDER_EDITED
                     , A.ADDRESS_1_EDITED
                     , A.ADDRESS_2_EDITED
                     , A.CITY_EDITED
                     , A.STATE_EDITED
                     , A.ZIP_CODE_EDITED
                     , A.EMAIL_ADDRESS_EDITED
                     , A.CELL_PHONE_EDITED
                     , A.WORK_PHONE_EDITED
                     , A.HOME_PHONE_EDITED
                     , A.ETHNIC_GROUP_EDITED
                     , A.USER_LAST_EDITED_BY_ID
                     , A.LAST_EDITED_TIMESTAMP
                     , A.UDF1
                     , A.UDF2
                     , A.SOURCEFILENAME
                     , A.RECEIVED_DATE
                     , A.IMPORT_DATE
                     , CURRENT_DATE() AS SCRUBBED_DATE
                     , A.DATA_SOURCE
                     , A.JOB_TITLE
                     , A.INCOME_LEVEL
                     , A.BUSINESS_UNIT
                     , A.EMPLOYEE_SSN
                     , A.EMPLOYEE_FIRST_NAME
                     , A.EMPLOYEE_MIDDLE_NAME
                     , A.EMPLOYEE_LAST_NAME
                     , A.PCP_NPI
                     , A.PCP_NAME
                     , A.PCP_DATE_LAST_SEEN
                     , A.PCP_DATE_FIRST_SEEN
                     , A.ENGAGED_WITH_PCP
                     , A.NO_OF_PCP_VISITS_SEEN_LAST12_MONTHS
                     , A.PCP_SOURCE
                     , A.ATTRIBUTED_PCP_NPI
                     , A.ATTRIBUTED_PCP_NAME
                     , A.ATTRIBUTED_PCP_DATE_LAST_SEEN
                     , A.ATTRIBUTED_PCP_DATE_FIRST_SEEN
                     , A.ENGAGED_WITH_ATTRIBUTED_PCP
                     , A.NO_OF_ATTRIBUTED_PCP_VISITS_SEEN_LAST12_MONTHS
                     , A.ATTRIBUTED_PCP_SOURCE
                     , TO_VARCHAR(A.MEMBER_ID_2) AS UDF3
                     , A.UDF4
                     , A.UDF5
                     FROM RECEIVED_ELIGIBILITY_LATEST_INFO_'||CURRENTTIMESTAMP||' A
                     LEFT JOIN CTE_ZIP B ON TRIM(A.ZIP_CODE) = B.ZIP_CODE
                     LEFT JOIN '||MASTER_CLIENT_GROUP||' C ON A.GROUP_ID = C.GROUP_ID';

  EXECUTE IMMEDIATE 'TRUNCATE TABLE '||ELIGIBILITY_TABLE;
  EXECUTE IMMEDIATE 'INSERT INTO '||ELIGIBILITY_TABLE||'
                     SELECT
                       NULL AS CH_ELIGIBILITY_ID
                     , NULL AS RECORD_ID
                     , NULL AS CH_MEMBER_ID
                     , COALESCE(C.CH_EMPLOYER_ID, ''${defaultChEmp}'') AS CH_EMPLOYER_ID
                     , A.MEMBER_ID
                     , A.GROUP_ID
                     , A.EMPLOYEE_ID
                     , ''${policyId}'' AS POLICY_ID
                     , ${planType} AS PLAN_TYPE
                     , ''${carrierId}'' AS CARRIER_ID
                     , ''${carrierName}'' AS CARRIER_NAME
                     , NULL AS COVERAGE_TYPE
                     , ${planId} AS PLAN_ID
                     , ${planName} AS PLAN_NAME
                     , ''${benefitType}'' AS BENEFIT_TYPE
                     , B.EFFECTIVE_DATE
                     , B.TERMINATION_DATE
                     , NULL AS MERGE_DT
                     , NULL AS MERGED_FROM_CH_MEMBER_ID
                     , NULL AS MERGED_FROM_MEMBER_ID
                     , A.SOURCEFILENAME
                     , A.RECEIVED_DATE
                     , A.IMPORT_DATE
                     , CURRENT_DATE() AS SCRUBBED_DATE
                     , A.DATA_SOURCE
                     , A.FIRST_NAME
                     , A.LAST_NAME
                     , A.DOB
                     , A.GENDER
                     , A.UDF1 AS UDF1
                     , A.UDF2 AS UDF2
                     , A.UDF3 AS UDF3
                     , A.UDF4 AS UDF4
                     , A.UDF5 AS UDF5
                     FROM RECEIVED_ELIGIBILITY_LATEST_INFO_'||CURRENTTIMESTAMP||' A
                     LEFT JOIN '||MASTER_CLIENT_GROUP||' C ON A.GROUP_ID = C.GROUP_ID
                     JOIN RECEIVED_ELIGIBILITY_ENROLLMENT_'||CURRENTTIMESTAMP||' B ON A.MEMBER_KEY1 = B.MEMBER_KEY1';

  EXECUTE IMMEDIATE 'CREATE OR REPLACE TEMPORARY TABLE ELIG_EMPID_CXWLK_'||CURRENTTIMESTAMP||'
                     AS
                     WITH EMPLOYEE_TABLE AS
                     (
                     SELECT MEMBER_ID, EMPLOYEE_ID, RELATIONSHIP, SOURCEFILENAME
                     FROM '||MEMBER_TABLE||'
                     WHERE RELATIONSHIP = ''${empRel}''
                     ),
                     DEPENDENT_TABLE AS
                     (
                     SELECT MEMBER_ID, EMPLOYEE_ID, RELATIONSHIP, SOURCEFILENAME
                     FROM '||MEMBER_TABLE||'
                     WHERE RELATIONSHIP IN (''DEPENDENT'', ''SPOUSE'')
                     AND ARRAY_SIZE(EMPLOYEE_ID::ARRAY) > 1
                     )
                     SELECT
                       D.EMPLOYEE_ID AS EMPLOYEE_ID_TO_BE_UPDATED
                     , E.EMPLOYEE_ID AS EMPLOYEE_ID_TO_UPDATE
                     FROM DEPENDENT_TABLE D
                     JOIN EMPLOYEE_TABLE E
                     ON ARRAYS_OVERLAP(D.EMPLOYEE_ID::ARRAY, E.EMPLOYEE_ID::ARRAY)';

  EXECUTE IMMEDIATE 'UPDATE '||MEMBER_TABLE||' A
                     SET A.EMPLOYEE_ID = B.EMPLOYEE_ID_TO_UPDATE
                     FROM ELIG_EMPID_CXWLK_'||CURRENTTIMESTAMP||' B
                     WHERE A.EMPLOYEE_ID = B.EMPLOYEE_ID_TO_BE_UPDATED';
  EXECUTE IMMEDIATE 'UPDATE '||ELIGIBILITY_TABLE||' A
                     SET A.EMPLOYEE_ID = B.EMPLOYEE_ID_TO_UPDATE
                     FROM ELIG_EMPID_CXWLK_'||CURRENTTIMESTAMP||' B
                     WHERE A.EMPLOYEE_ID = B.EMPLOYEE_ID_TO_BE_UPDATED';

  EXECUTE IMMEDIATE 'CREATE OR REPLACE TEMPORARY TABLE ELIG_COVERAGE_CXWLK_'||CURRENTTIMESTAMP||'
                     AS
                     SELECT
                       EMPLOYEE_ID
                     , LISTAGG(DISTINCT RELATIONSHIP, '' | '') WITHIN GROUP (ORDER BY RELATIONSHIP) AS RELATIONSHIPS
                     FROM '||MEMBER_TABLE||'
                     GROUP BY EMPLOYEE_ID';

  EXECUTE IMMEDIATE 'UPDATE '||ELIGIBILITY_TABLE||' A
                     SET A.COVERAGE_TYPE =
                     CASE WHEN B.RELATIONSHIPS = ''DEPENDENT | EMPLOYEE | SPOUSE'' THEN ''FAMILY''
                          WHEN B.RELATIONSHIPS = ''DEPENDENT | EMPLOYEE'' THEN ''EMPLOYEE + DEPENDENT''
                          WHEN B.RELATIONSHIPS = ''EMPLOYEE | SPOUSE'' THEN ''EMPLOYEE + SPOUSE''
                          WHEN B.RELATIONSHIPS = ''EMPLOYEE'' THEN ''EMPLOYEE ONLY''
                          ELSE ''UNKNOWN''
                     END
                     FROM ELIG_COVERAGE_CXWLK_'||CURRENTTIMESTAMP||' B
                     WHERE A.EMPLOYEE_ID = B.EMPLOYEE_ID';

  EXECUTE IMMEDIATE 'UPDATE '||ELIGIBILITY_TABLE||' A
                     SET A.EMPLOYEE_ID = B.EMPLOYEE_ID
                     FROM '||MEMBER_TABLE||' B
                     WHERE A.MEMBER_ID = B.MEMBER_ID';

  EXECUTE IMMEDIATE 'CREATE OR REPLACE TEMPORARY TABLE ELIG_COVERAGE_CXWLK2_'||CURRENTTIMESTAMP||'
                     AS
                     SELECT
                       EMPLOYEE_ID
                     , LISTAGG(DISTINCT RELATIONSHIP, '' | '') WITHIN GROUP (ORDER BY RELATIONSHIP) AS RELATIONSHIPS
                     FROM '||MEMBER_TABLE||'
                     GROUP BY EMPLOYEE_ID';

  EXECUTE IMMEDIATE 'UPDATE '||ELIGIBILITY_TABLE||' A
                     SET A.COVERAGE_TYPE =
                     CASE WHEN B.RELATIONSHIPS = ''DEPENDENT | EMPLOYEE | SPOUSE'' THEN ''FAMILY''
                          WHEN B.RELATIONSHIPS = ''DEPENDENT | EMPLOYEE'' THEN ''EMPLOYEE + DEPENDENT''
                          WHEN B.RELATIONSHIPS = ''EMPLOYEE | SPOUSE'' THEN ''EMPLOYEE + SPOUSE''
                          WHEN B.RELATIONSHIPS = ''EMPLOYEE'' THEN ''EMPLOYEE ONLY''
                          ELSE ''UNKNOWN''
                     END
                     FROM ELIG_COVERAGE_CXWLK2_'||CURRENTTIMESTAMP||' B
                     WHERE A.EMPLOYEE_ID = B.EMPLOYEE_ID';

  EXECUTE IMMEDIATE 'CREATE OR REPLACE TEMPORARY TABLE ELIG_EMPLOYEES_'||CURRENTTIMESTAMP||'
                     AS
                     SELECT DISTINCT
                       EMPLOYEE_ID
                     , FIRST_NAME AS EMPLOYEE_FIRST_NAME
                     , MIDDLE_NAME AS EMPLOYEE_MIDDLE_NAME
                     , LAST_NAME AS EMPLOYEE_LAST_NAME
                     , SSN AS EMPLOYEE_SSN
                     FROM '||MEMBER_TABLE||'
                     WHERE RELATIONSHIP = ''${empRel}''';

  EXECUTE IMMEDIATE 'UPDATE '||MEMBER_TABLE||' A
                     SET A.EMPLOYEE_SSN = B.EMPLOYEE_SSN
                       , A.EMPLOYEE_FIRST_NAME = B.EMPLOYEE_FIRST_NAME
                       , A.EMPLOYEE_MIDDLE_NAME = B.EMPLOYEE_MIDDLE_NAME
                       , A.EMPLOYEE_LAST_NAME = B.EMPLOYEE_LAST_NAME
                     FROM ELIG_EMPLOYEES_'||CURRENTTIMESTAMP||' B
                     WHERE A.EMPLOYEE_ID = B.EMPLOYEE_ID';

  SOURCE_FILE_NAME := ARRAY_TO_STRING(SOURCE_FILE_ARRAY, CHR(10) || CHR(13));
  CALL REFDATA.DATA_DEV.SP_SEND_EMAIL(
    'Success - ${esc(cfg.clientName)} Eligibility Stream Data Processing.',
    '${esc(cfg.clientName)} Eligibility Stream Data has been processed successfully.'
    || CHR(10) || CHR(13) || 'SourceFileName: ' || :SOURCE_FILE_NAME
  );

  EXCEPTION
    WHEN OTHER THEN
      ERROR_MESSAGE := REGEXP_REPLACE(SQLERRM, '[^[:alnum:]]', ' ');
      CALL REFDATA.DATA_DEV.SP_SEND_EMAIL(
        'Error in ${esc(cfg.clientName)} Eligibility Stream Data Processing.',
        'Issue encountered in ${esc(cfg.clientName)} eligibility stream data processing.'
        || CHR(10) || CHR(13) || 'SourceFileName(s): ' || :SOURCE_FILE_NAME
        || CHR(10) || CHR(13) || 'Error Code: '    || :SQLCODE
        || CHR(10) || CHR(13) || 'Error Message: ' || :ERROR_MESSAGE
      );
  RETURN 'Eligibility Stream Data Scrub for ${esc(cfg.clientName)} Completed.';
END;
$$;`;
}
