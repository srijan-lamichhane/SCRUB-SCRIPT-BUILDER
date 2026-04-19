import { PX_RAW_FIELDS, PMO_RAW_FIELDS, PX_DEFAULTS, PMO_DEFAULTS } from './pharmacy.js';
import { MED_RAW_FIELDS, MED_DEFAULTS } from './medical.js';
import {
  ELIG_RAW_FIELDS,
  ELIG_MEMBER_PLATFORM_DEFAULTS,
  ELIG_ELIGIBILITY_PLATFORM_DEFAULTS,
} from './eligibility.js';

/** Fresh empty maps (all NULL) — same targets as full templates so the grid stays complete for new clients. */
export function createEmptyPharmacyMaps() {
  return PX_DEFAULTS.map(m => ({ target: m.target, type: 'null', value: '' }));
}

export function createEmptyMedicalMaps() {
  return MED_DEFAULTS.map(m => ({ target: m.target, type: 'null', value: '' }));
}

export function createEmptyEligibilityMemberMaps() {
  return ELIG_MEMBER_PLATFORM_DEFAULTS.map(m => ({
    target: m.target,
    type: 'null',
    value: '',
    insertExpr: '',
  }));
}

export function createEmptyEligibilityEligMaps() {
  return ELIG_ELIGIBILITY_PLATFORM_DEFAULTS.map(m => ({
    target: m.target,
    type: 'null',
    value: '',
  }));
}

/** Vendor bundles: raw field lists + mapping rows. Add new vendors here. */
export const VENDOR_PRESETS = {
  pharmacy: {
    slateRx: {
      label: 'SlateRx',
      rawFields: PX_RAW_FIELDS,
      maps: PX_DEFAULTS,
    },
    medone: {
      label: 'Medone',
      rawFields: PMO_RAW_FIELDS,
      maps: PMO_DEFAULTS,
    },
  },
  medical: {
    cigna: {
      label: 'Cigna',
      rawFields: MED_RAW_FIELDS,
      maps: MED_DEFAULTS,
    },
  },
  eligibility: {
    cigna: {
      label: 'Cigna',
      rawFields: ELIG_RAW_FIELDS,
      mapsMember: ELIG_MEMBER_PLATFORM_DEFAULTS,
      mapsEligibility: ELIG_ELIGIBILITY_PLATFORM_DEFAULTS,
    },
  },
};

export const PHARMACY_VENDOR_ORDER = ['slateRx', 'medone'];
export const MEDICAL_VENDOR_ORDER = ['cigna'];
export const ELIGIBILITY_VENDOR_ORDER = ['cigna'];
