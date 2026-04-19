import {
  createEmptyPharmacyMaps,
  createEmptyMedicalMaps,
  createEmptyEligibilityMemberMaps,
  createEmptyEligibilityEligMaps,
} from './vendorPresets.js';

/** Default raw fields, mapping rows, and config object for pharmacy vs medical mode.
 *  Initial layout is empty (no raw columns, all-null mappings) until a vendor preset is applied. */
export function getModeDefaults(mode) {
  const isPx = mode === 'pharmacy';
  const isMedical = mode === 'medical';
  const storePfx = isPx ? 'px' : isMedical ? 'med' : 'elig';
  if (isPx) {
    return {
      storePfx,
      isPx,
      defaultRaw: [],
      defaultMaps: createEmptyPharmacyMaps(),
      defaultMapsMember: null,
      defaultMapsEligibility: null,
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
      defaultRaw: [],
      defaultMaps: createEmptyMedicalMaps(),
      defaultMapsMember: null,
      defaultMapsEligibility: null,
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
    defaultRaw: [],
    defaultMaps: [],
    defaultMapsMember: createEmptyEligibilityMemberMaps(),
    defaultMapsEligibility: createEmptyEligibilityEligMaps(),
    defaultCfg: {
      clientName: 'MJHLIFESCIENCES',
      warehouse: 'MJHLIFESCIENCES',
      database: 'DATA_WAREHOUSE_DEV',
      schema: 'MJHLIFESCIENCES_SCRUB',
      streamTable: 'RAW_ELIGIBILITY_STREAM',
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
    },
  };
}
