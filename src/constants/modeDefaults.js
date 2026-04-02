import { PX_RAW_FIELDS, PX_DEFAULTS } from './pharmacy.js';
import { MED_RAW_FIELDS, MED_DEFAULTS } from './medical.js';
import { ELIG_RAW_FIELDS, ELIG_DEFAULTS } from './eligibility.js';

/** Default raw fields, mapping rows, and config object for pharmacy vs medical mode. */
export function getModeDefaults(mode) {
  const isPx = mode === 'pharmacy';
  const isMedical = mode === 'medical';
  const storePfx = isPx ? 'px' : isMedical ? 'med' : 'elig';
  if (isPx) {
    return {
      storePfx,
      isPx,
      defaultRaw: PX_RAW_FIELDS,
      defaultMaps: PX_DEFAULTS,
      defaultCfg: {
        clientName: 'MJHLIFESCIENCES',
        warehouse: 'MJHLIFESCIENCES',
        database: 'DATA_WAREHOUSE_DEV',
        schema: 'MJHLIFESCIENCES_SCRUB',
        streamTable: 'RAW_PHARMACY_STREAM',
        masterDrug: 'REFDATA.DATA_DEV.MASTER_DRUG',
        masterProviders: 'REFDATA.GLOBAL_PROD.MASTER_PROVIDERS',
      },
    };
  }
  if (isMedical) {
    return {
      storePfx,
      isPx,
      defaultRaw: MED_RAW_FIELDS,
      defaultMaps: MED_DEFAULTS,
      defaultCfg: {
        clientName: 'MJHLIFESCIENCES',
        warehouse: 'MJHLIFESCIENCES',
        database: 'DATA_WAREHOUSE_DEV',
        schema: 'MJHLIFESCIENCES_SCRUB',
        streamTable: 'RAW_MEDICAL_STREAM',
        masterProviders: 'REFDATA.GLOBAL_PROD.MASTER_PROVIDERS',
        masterProcedure: 'REFDATA.DATA_DEV.MASTER_PROCEDURE',
        masterProcedureModifier: 'REFDATA.GLOBAL_PROD.MASTER_PROCEDURE_MODIFIER',
        masterDiagnosis: 'REFDATA.DATA_DEV.MASTER_DIAGNOSIS',
        masterPos: 'REFDATA.DATA_DEV.MASTER_POS',
      },
    };
  }
  return {
    storePfx,
    isPx,
    defaultRaw: ELIG_RAW_FIELDS,
    defaultMaps: ELIG_DEFAULTS,
    defaultCfg: {
      clientName: 'MJHLIFESCIENCES',
      warehouse: 'MJHLIFESCIENCES',
      database: 'DATA_WAREHOUSE_DEV',
      schema: 'MJHLIFESCIENCES_SCRUB',
      streamTable: 'RAW_ELIGIBILITY_STREAM',
      rawEligibilityTable: 'MJHLIFESCIENCES_DEV.RAW_ELIGIBILITY_PLATFORM',
      memberTable: 'MAP_MEMBER_PLATFORM',
      eligibilityTable: 'MAP_ELIGIBILITY_PLATFORM',
      masterClientGroup: 'REFDATA.GLOBAL_PROD.MASTER_CLIENT_GROUP',
      masterZip: 'REFDATA.GLOBAL_PROD.MASTER_ZIP_CODES',
      controlFileFilter: '%CONTROLS%',
      memberKeyExpression:
        "UPPER(TRIM(FIRST_NAME))||UPPER(TRIM(LAST_NAME))||DOB||TRIM(UPPER(GENDER))",
      memberSecondaryKeyExpression: 'TRIM(MEMBER_ID)',
      employeeRelationshipValue: 'EMPLOYEE',
      listaggDelimiter: ',',
      carrierId: 'CIGNA',
      carrierName: 'CIGNA',
      benefitType: 'Medical',
      policyId: 'BK',
      defaultChEmployerId: '802',
      planTypeExpression: 'NULL',
      planIdExpression: 'GROUP_ID',
      planNameExpression: 'GROUP_ID',
    },
  };
}
