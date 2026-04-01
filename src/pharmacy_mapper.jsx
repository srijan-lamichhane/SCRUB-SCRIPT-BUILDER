import { useState, useEffect, useCallback, useRef, useMemo } from "react";

const GLOBAL_STYLE = `
  @import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Sora:wght@400;500;600;700&display=swap');
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: #0b0f1a; }
  ::-webkit-scrollbar { width: 6px; height: 6px; }
  ::-webkit-scrollbar-track { background: #111827; }
  ::-webkit-scrollbar-thumb { background: #2d3748; border-radius: 3px; }
  ::-webkit-scrollbar-thumb:hover { background: #4a5568; }
`;

// ─────────────────────────────────────────────────────────────
// PHARMACY CONSTANTS
// ─────────────────────────────────────────────────────────────
const PX_RAW_FIELDS = [
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
  'DAWPENALTY','PERSONCODE','GROUPCODE','SUBSCRIBERSSN','SOURCEFILENAME','FILE_RECEIVED_DATE','IMPORT_DATE'
];
const PX_MAX_FIELDS = new Set(['SOURCEFILENAME','FILE_RECEIVED_DATE','IMPORT_DATE']);

const PX_STAGE_DDL = [
  'CH_PHARMACY_CLAIM_ID VARCHAR(16777216)','RECORD_ID VARCHAR(16777216)','MEMBER_ID VARCHAR(16777216)',
  'CLAIM_ID VARCHAR(16777216)','THERAPEUTIC_EQUIVALENCE_INDICATOR VARCHAR(16777216)',
  'THERAPEUTIC_EQUIVALENCE_INDICATOR_DESCRIPTION VARCHAR(16777216)','GROUP_ID VARCHAR(16777216)',
  'GCN_SEQUENCE_NUMBER VARCHAR(16777216)','CH_MEMBER_ID VARCHAR(16777216)','CH_EMPLOYER_ID VARCHAR(16777216)',
  'COMPOUND_FLAG VARCHAR(16777216)','OVER_THE_COUNTER_FLAG VARCHAR(16777216)','DRUG_TYPE VARCHAR(16777216)',
  'PRESCRIBER_ID VARCHAR(16777216)','PRESCRIBER_FIRST_NAME VARCHAR(16777216)','MAINTENANCE_DRUG_FLAG VARCHAR(16777216)',
  'CATEGORY_NUMBER VARCHAR(16777216)','PRESCRIBER_MIDDLE_NAME VARCHAR(16777216)','PRESCRIBER_LAST_NAME VARCHAR(16777216)',
  'CATEGORY_DESCRIPTION VARCHAR(16777216)','PROVIDER_DEA_NUMBER VARCHAR(16777216)','PROVIDER_NPI VARCHAR(16777216)',
  'PROVIDER_NABP_NUMBER VARCHAR(16777216)','PHARMACY_NAME VARCHAR(16777216)','WRITTEN_DATE DATE',
  'FILLED_DATE DATE','PAID_DATE DATE','DIAGNOSIS_CODE VARCHAR(16777216)','DIAGNOSIS_DESCRIPTION VARCHAR(16777216)',
  'NDC_CODE VARCHAR(16777216)','NDC_DESCRIPTION VARCHAR(16777216)','SPECIALTY_DRUG_FLAG VARCHAR(16777216)',
  'PRESCRIPTION_CLASS_CODE VARCHAR(16777216)','PRESCRIPTION_CLASS_DESCRIPTION VARCHAR(16777216)',
  'BRAND_CODE VARCHAR(16777216)','DRUG_NAME VARCHAR(16777216)','DRUG_DOSAGE VARCHAR(16777216)',
  'DRUG_STRENGTH VARCHAR(16777216)','UNIT_QUANTITY FLOAT','DAYS_OF_SUPPLY VARCHAR(16777216)',
  'PRESCRIPTION_LABEL_NAME VARCHAR(16777216)','FORMULARY_FLAG VARCHAR(16777216)','GENERIC_FLAG VARCHAR(16777216)',
  'MAIL_ORDER_FLAG VARCHAR(16777216)','UNIT_PRICE FLOAT','PACKAGE_PRICE FLOAT',
  'DUPLICATE_THERAPY_CLASS_ID VARCHAR(16777216)','DUPLICATE_THERAPY_DESCRIPTION VARCHAR(16777216)',
  'DUPLICATE_THERAPY_ALLOWANCE_NUMBER VARCHAR(16777216)','HIC3_CODE VARCHAR(16777216)',
  'HIC3_DESCRIPTION VARCHAR(16777216)','ROUTE_OF_ADMINISTRATION VARCHAR(16777216)','REFILL_QUANTITY FLOAT',
  'REFILLS_ALLOWED_NUMBER FLOAT','DISPENSED_AS_WRITTEN_CODE VARCHAR(16777216)',
  'DISPENSED_AS_WRITTEN_DESCRIPTION VARCHAR(16777216)','ALLOWED_AMOUNT FLOAT','BILLED_AMOUNT FLOAT',
  'COINSURANCE_AMOUNT FLOAT','COPAY_AMOUNT FLOAT','DEDUCTIBLE_AMOUNT FLOAT','DISPENSING_FEE_AMOUNT FLOAT',
  'INGREDIENT_COST_AMOUNT FLOAT','STATE_TAX_AMOUNT FLOAT','USUAL_CUSTOMARY_FEE_AMOUNT FLOAT',
  'PAID_AMOUNT FLOAT','PHARMACY_NPI VARCHAR(16777216)','PHARMACY_STATE VARCHAR(16777216)',
  'PHARMACY_ZIP_CODE VARCHAR(16777216)','PRESCRIPTION_GENERIC_BRAND_INDICATOR VARCHAR(16777216)',
  'ADJUDICATION_DESCRIPTION VARCHAR(16777216)','GPI10_CODE VARCHAR(16777216)','CARDHOLDER_ID VARCHAR(16777216)',
  'CLIENT_ACCOUNT_ID VARCHAR(16777216)','DAYS_SUPPLY_INTENDED_TO_BE_DISPENSED FLOAT',
  'DISPENSING_STATUS VARCHAR(16777216)','DRUG_CATEGORY_CODE VARCHAR(16777216)','OTHER_PAYER_AMOUNT FLOAT',
  'PROFESSIONAL_SERVICE_CODE_1 VARCHAR(16777216)','PROFESSIONAL_SERVICE_CODE_2 VARCHAR(16777216)',
  'PROFESSIONAL_SERVICE_CODE_3 VARCHAR(16777216)','QUANTITY_INTENDED_TO_BE_DISPENSED FLOAT',
  'REASON_FOR_SERVICE_CODE_1 VARCHAR(16777216)','REASON_FOR_SERVICE_CODE_2 VARCHAR(16777216)',
  'REASON_FOR_SERVICE_CODE_3 VARCHAR(16777216)','RESULT_OF_SERVICE_CODE_1 VARCHAR(16777216)',
  'RESULT_OF_SERVICE_CODE_2 VARCHAR(16777216)','RESULT_OF_SERVICE_CODE_3 VARCHAR(16777216)',
  'SERVICE_PROVIDER_ID VARCHAR(16777216)','UNIT_OF_MEASURE VARCHAR(16777216)','MERGE_DT DATE',
  'MERGED_FROM_CH_MEMBER_ID VARCHAR(16777216)','MERGED_FROM_MEMBER_ID VARCHAR(16777216)',
  'SOURCEFILENAME VARCHAR(16777216)','RECEIVED_DATE DATE','IMPORT_DATE DATE','SCRUBBED_DATE DATE',
  'AWP FLOAT','UDF1 VARCHAR(16777216)','UDF2 VARCHAR(16777216)','UDF3 VARCHAR(16777216)',
  'UDF4 VARCHAR(16777216)','UDF5 VARCHAR(16777216)','UDF6 VARCHAR(16777216)','UDF7 VARCHAR(16777216)',
  'UDF8 VARCHAR(16777216)','UDF9 VARCHAR(16777216)','UDF10 VARCHAR(16777216)'
];

const PX_DEFAULTS = [
  {target:'CH_PHARMACY_CLAIM_ID',type:'null',value:''},
  {target:'RECORD_ID',type:'null',value:''},
  {target:'MEMBER_ID',type:'expression',value:"MEMBERID||PERSONCODE"},
  {target:'CLAIM_ID',type:'direct',value:'TRANSACTIONID'},
  {target:'THERAPEUTIC_EQUIVALENCE_INDICATOR',type:'null',value:''},
  {target:'THERAPEUTIC_EQUIVALENCE_INDICATOR_DESCRIPTION',type:'null',value:''},
  {target:'GROUP_ID',type:'direct',value:'GROUPID'},
  {target:'GCN_SEQUENCE_NUMBER',type:'direct',value:'GCN'},
  {target:'CH_MEMBER_ID',type:'null',value:''},
  {target:'CH_EMPLOYER_ID',type:'hardcoded',value:'810'},
  {target:'COMPOUND_FLAG',type:'expression',value:"CASE WHEN COMPOUNDCODE IN ('2') THEN 'Y' WHEN COMPOUNDCODE = '1' THEN 'N' ELSE 'U' END"},
  {target:'OVER_THE_COUNTER_FLAG',type:'expression',value:"CASE WHEN RXOTCINDICATION = 'YES' THEN 'Y' WHEN RXOTCINDICATION = 'NO' THEN 'N' ELSE 'U' END"},
  {target:'DRUG_TYPE',type:'expression',value:"CASE WHEN DRUGTYPE IN ('GEN') THEN 'GENERIC' WHEN DRUGTYPE IN ('MSB','SSB','Branded Generic') THEN 'BRANDED' ELSE 'UNKNOWN' END"},
  {target:'PRESCRIBER_ID',type:'direct',value:'PRESCRIBERID'},
  {target:'PRESCRIBER_FIRST_NAME',type:'direct',value:'PRESCRIBINGPHYSICIAN'},
  {target:'MAINTENANCE_DRUG_FLAG',type:'null',value:''},
  {target:'CATEGORY_NUMBER',type:'null',value:''},
  {target:'PRESCRIBER_MIDDLE_NAME',type:'null',value:''},
  {target:'PRESCRIBER_LAST_NAME',type:'null',value:''},
  {target:'CATEGORY_DESCRIPTION',type:'null',value:''},
  {target:'PROVIDER_DEA_NUMBER',type:'direct',value:'DOCTORDEA'},
  {target:'PROVIDER_NPI',type:'direct',value:'PRESCRIBERID'},
  {target:'PROVIDER_NABP_NUMBER',type:'null',value:''},
  {target:'PHARMACY_NAME',type:'direct',value:'PHARMACYNAME'},
  {target:'WRITTEN_DATE',type:'expression',value:"TO_DATE(PRESCRIPTIONWRITTENDATE::VARCHAR,'MM/DD/YYYY')"},
  {target:'FILLED_DATE',type:'expression',value:"TO_DATE(SERVICEDATE::VARCHAR,'MM/DD/YYYY')"},
  {target:'PAID_DATE',type:'expression',value:"TO_DATE(TRANSACTIONDATE::VARCHAR,'MM/DD/YYYY')"},
  {target:'DIAGNOSIS_CODE',type:'null',value:''},
  {target:'DIAGNOSIS_DESCRIPTION',type:'null',value:''},
  {target:'NDC_CODE',type:'expression',value:"LPAD(NDC,11,'0')"},
  {target:'NDC_DESCRIPTION',type:'direct',value:'LABELNAME'},
  {target:'SPECIALTY_DRUG_FLAG',type:'expression',value:"CASE WHEN SPECIALTY = 'YES' THEN 'Y' WHEN SPECIALTY = 'NO' THEN 'N' ELSE 'U' END"},
  {target:'PRESCRIPTION_CLASS_CODE',type:'null',value:''},
  {target:'PRESCRIPTION_CLASS_DESCRIPTION',type:'null',value:''},
  {target:'BRAND_CODE',type:'null',value:''},
  {target:'DRUG_NAME',type:'direct',value:'DRUGNAME'},
  {target:'DRUG_DOSAGE',type:'direct',value:'DOSAGEFORM'},
  {target:'DRUG_STRENGTH',type:'direct',value:'DRUGSTRENGTH'},
  {target:'UNIT_QUANTITY',type:'direct',value:'QUANTITYDISPENSED'},
  {target:'DAYS_OF_SUPPLY',type:'expression',value:'CAST(DAYSSUPPLY AS INT)'},
  {target:'PRESCRIPTION_LABEL_NAME',type:'null',value:''},
  {target:'FORMULARY_FLAG',type:'expression',value:"CASE WHEN FORMULARYINDICATOR = 'YES' THEN 'Y' WHEN FORMULARYINDICATOR = 'NO' THEN 'N' ELSE 'N' END"},
  {target:'GENERIC_FLAG',type:'expression',value:"CASE WHEN DRUGTYPE = 'GEN' THEN 'Y' ELSE 'N' END"},
  {target:'MAIL_ORDER_FLAG',type:'null',value:''},
  {target:'UNIT_PRICE',type:'null',value:''},
  {target:'PACKAGE_PRICE',type:'null',value:''},
  {target:'DUPLICATE_THERAPY_CLASS_ID',type:'null',value:''},
  {target:'DUPLICATE_THERAPY_DESCRIPTION',type:'null',value:''},
  {target:'DUPLICATE_THERAPY_ALLOWANCE_NUMBER',type:'null',value:''},
  {target:'HIC3_CODE',type:'null',value:''},
  {target:'HIC3_DESCRIPTION',type:'null',value:''},
  {target:'ROUTE_OF_ADMINISTRATION',type:'null',value:''},
  {target:'REFILL_QUANTITY',type:'direct',value:'FILLNUMBER'},
  {target:'REFILLS_ALLOWED_NUMBER',type:'null',value:''},
  {target:'DISPENSED_AS_WRITTEN_CODE',type:'direct',value:'DAWCODE'},
  {target:'DISPENSED_AS_WRITTEN_DESCRIPTION',type:'expression',value:"CASE WHEN DAWCODE='0' THEN 'No Product Selection Indicated' WHEN DAWCODE='1' THEN 'Substitution Not Allowed by Prescriber' WHEN DAWCODE='2' THEN 'Substitution Allowed - Patient Requested Product Dispensed' WHEN DAWCODE='3' THEN 'Substitution Allowed - Pharmacist Selected Product Dispensed' WHEN DAWCODE='4' THEN 'Substitution Allowed - Generic Drug Not in Stock' WHEN DAWCODE='5' THEN 'Substitution Allowed - Brand Drug Dispensed as Generic' WHEN DAWCODE='6' THEN 'Override' WHEN DAWCODE='7' THEN 'Substitution Not Allowed - Brand Drug Mandated by Law' WHEN DAWCODE='8' THEN 'Substitution Allowed - Generic Drug Not Available in Marketplace' WHEN DAWCODE='9' THEN 'Substitution Allowed By Prescriber but Plan Requests Brand' ELSE DAWCODE END"},
  {target:'ALLOWED_AMOUNT',type:'direct',value:'GROSSAMTDUE'},
  {target:'BILLED_AMOUNT',type:'direct',value:'GROSSAMTDUE'},
  {target:'COINSURANCE_AMOUNT',type:'direct',value:'COINSURANCE'},
  {target:'COPAY_AMOUNT',type:'direct',value:'COPAY'},
  {target:'DEDUCTIBLE_AMOUNT',type:'direct',value:'DEDUCTIBLEAMT'},
  {target:'DISPENSING_FEE_AMOUNT',type:'direct',value:'DISPFEEPAID'},
  {target:'INGREDIENT_COST_AMOUNT',type:'direct',value:'INGREDIENTCOST'},
  {target:'STATE_TAX_AMOUNT',type:'direct',value:'SALESTAX'},
  {target:'USUAL_CUSTOMARY_FEE_AMOUNT',type:'direct',value:'USUALANDCUST'},
  {target:'PAID_AMOUNT',type:'expression',value:'PLANPAID+PATIENTPAID'},
  {target:'PHARMACY_NPI',type:'direct',value:'PHARMACYNPI'},
  {target:'PHARMACY_STATE',type:'direct',value:'PHARMACYSTATE'},
  {target:'PHARMACY_ZIP_CODE',type:'direct',value:'PHARMACYZIPCODE'},
  {target:'PRESCRIPTION_GENERIC_BRAND_INDICATOR',type:'null',value:''},
  {target:'ADJUDICATION_DESCRIPTION',type:'null',value:''},
  {target:'GPI10_CODE',type:'direct',value:'GPICODE'},
  {target:'CARDHOLDER_ID',type:'null',value:''},
  {target:'CLIENT_ACCOUNT_ID',type:'direct',value:'ACCOUNTID'},
  {target:'DAYS_SUPPLY_INTENDED_TO_BE_DISPENSED',type:'null',value:''},
  {target:'DISPENSING_STATUS',type:'null',value:''},
  {target:'DRUG_CATEGORY_CODE',type:'expression',value:"CASE WHEN DRUGTYPE = 'GEN' THEN 'GENERIC' WHEN DRUGTYPE = 'MSB' THEN 'MULTI SOURCE BRAND' WHEN DRUGTYPE = 'SSB' THEN 'SINGLE SOURCE BRAND' WHEN DRUGTYPE = 'BRANDED GENERIC' THEN 'BRANDED GENERIC' ELSE 'UNKNOWN' END"},
  {target:'OTHER_PAYER_AMOUNT',type:'null',value:''},
  {target:'PROFESSIONAL_SERVICE_CODE_1',type:'null',value:''},
  {target:'PROFESSIONAL_SERVICE_CODE_2',type:'null',value:''},
  {target:'PROFESSIONAL_SERVICE_CODE_3',type:'null',value:''},
  {target:'QUANTITY_INTENDED_TO_BE_DISPENSED',type:'null',value:''},
  {target:'REASON_FOR_SERVICE_CODE_1',type:'null',value:''},
  {target:'REASON_FOR_SERVICE_CODE_2',type:'null',value:''},
  {target:'REASON_FOR_SERVICE_CODE_3',type:'null',value:''},
  {target:'RESULT_OF_SERVICE_CODE_1',type:'null',value:''},
  {target:'RESULT_OF_SERVICE_CODE_2',type:'null',value:''},
  {target:'RESULT_OF_SERVICE_CODE_3',type:'null',value:''},
  {target:'SERVICE_PROVIDER_ID',type:'null',value:''},
  {target:'UNIT_OF_MEASURE',type:'direct',value:'DOSAGEFORM'},
  {target:'MERGE_DT',type:'null',value:''},
  {target:'MERGED_FROM_CH_MEMBER_ID',type:'null',value:''},
  {target:'MERGED_FROM_MEMBER_ID',type:'null',value:''},
  {target:'SOURCEFILENAME',type:'direct',value:'SOURCEFILENAME'},
  {target:'RECEIVED_DATE',type:'direct',value:'FILE_RECEIVED_DATE'},
  {target:'IMPORT_DATE',type:'direct',value:'IMPORT_DATE'},
  {target:'SCRUBBED_DATE',type:'expression',value:'CURRENT_DATE()'},
  {target:'AWP',type:'direct',value:'AWP'},
  {target:'UDF1',type:'expression',value:"IFNULL(MEMBERFIRSTNAME,'X')||'_'||IFNULL(MEMBERLASTNAME,'X')||'_'||COALESCE(TO_DATE(BIRTHDATE::VARCHAR,'MM/DD/YYYY'),'99991231')||'_'||IFNULL(MEMBERGENDER,'U')"},
  {target:'UDF2',type:'direct',value:'TRANSTATUS'},
  {target:'UDF3',type:'direct',value:'SUBSCRIBERSSN'},
  {target:'UDF4',type:'direct',value:'PLANPAID'},
  {target:'UDF5',type:'direct',value:'PATIENTPAID'},
  {target:'UDF6',type:'direct',value:'GNN'},
  {target:'UDF7',type:'direct',value:'OOP'},
  {target:'UDF8',type:'direct',value:'SPECIALTY'},
  {target:'UDF9',type:'direct',value:'PCN'},
  {target:'UDF10',type:'null',value:''},
];

// ─────────────────────────────────────────────────────────────
// MEDICAL CONSTANTS
// ─────────────────────────────────────────────────────────────
const MED_MAX_FIELDS = new Set(['SOURCEFILENAME','FILE_RECEIVED_DATE','IMPORT_DATE']);
// Fields aliased in dedup: SVC_MINR_CD_1 → AS SVC_MINR_CD
const MED_FIELD_ALIAS = { 'SVC_MINR_CD_1': 'SVC_MINR_CD' };
// Hardcoded '0.00' values injected into dedup (not from raw)
const MED_HARDCODED_DEDUP = ['DERV_COINS_AMT','DERV_COPAY_AMT','DERV_DDCTBL_AMT','DERV_COB_CIGNA_SAVE_AMT'];

const MED_RAW_FIELDS = [
  'CHNL_SRC_CD','CLIENT_ACCT_NUM','CLIENT_ID','HMO_CD','ELGBTY_BRNCH_NUM','BEN_PLAN_NUM',
  'ACCT_TY_CD','ACCT_SUBTY_CD','PRODT_TY_CD','RNDR_PROV_ADDR_LN_1','RNDR_PROV_ADDR_LN_2',
  'LGCY_MBR_NUM','PATNT_FRST_NM','PATNT_LAST_NM','PATNT_BRTH_DT','PATNT_GENDR_CD',
  'PATNT_ST_CD','PATNT_POSTL_CD','PATNT_AGE_NUM','PATNT_RELSHP_CD','SUBSCRBR_STAT_CD',
  'RNDR_PROV_CITY_NM','RNDR_PROV_ST_CD','RNDR_PROV_ID','BILL_PROV_TIN','BILL_PROV_NPI',
  'BILL_PROV_NM','BILL_PROV_ADDR_LN_1','BILL_PROV_ADDR_LN_2','BILL_PROV_CITY_NM',
  'BILL_PROV_ST_CD','BILL_PROV_POSTL_CD','RNDR_PROV_TY_CD','RNDR_PROV_SPECLTY_CD',
  'PROV_PARTCPNT_AGRMT_CD','RNDR_PROV_NM','RNDR_PROV_POSTL_CD','PD_DT','SVC_BEG_DT',
  'SVC_END_DT','CLM_SYS_RECV_DT','DERV_PAYBL_AMT',
  'PROC_CD_MOD_CD_1','PROC_CD_MOD_CD_2','PROC_CD_MOD_CD_3','PROC_CD_MOD_CD_4',
  ...Array.from({length:25},(_,i)=>`CLM_DIAG_CD_${i+1}`),
  ...Array.from({length:25},(_,i)=>`CLM_POA_CD_${i+1}`),
  'CLM_SYS_CLM_ID','SVC_LN_NUM','PROC_TY_CD','PROC_CD','REVNU_CD','BEN_NT_LVL_CD',
  'MDC_CD','DERV_SU_CNT','CLM_EVENT_KEY','SRC_SVC_RMK_CD','LN_PROCS_EVENT_CD',
  'NT_ID','RNDR_PROV_NT_ID','SVC_MINR_CD_1','DERV_POS_CD','COB_INVLVMT_CD',
  'SURCHRG_ST_CD','TY_OF_BILL_CD','ENCNTR_TY_CD','ACTL_LOS_DAYS_NUM','DRG_CD',
  'DISCHRG_STAT_CD','MSC_CD','SVC_MAJ_CD','SVC_MINR_CD_2','RNDR_PROV_NPI',
  'ICD_VRSN_CD','CLM_EVENT_DRG_CD','NDC','AMI','INDIV_ENTPR_ID','REFRL_PROV_NPI',
  'SU_CNT','NEW_CLM_SYS_CLM_ID',
  ...Array.from({length:5},(_,i)=>`ICD_PROC_CD_${i+1}`),
  ...Array.from({length:19},(_,i)=>`FILLER${i+1}`),
  'SOURCEFILENAME','FILE_RECEIVED_DATE','IMPORT_DATE'
];

const MED_STAGE_DDL = [
  'CH_MEDICAL_CLAIM_ID VARCHAR(16777216)','RECORD_ID VARCHAR(16777216)',
  'MEMBER_ID VARCHAR(16777216)','GROUP_ID VARCHAR(16777216)',
  'CH_MEMBER_ID VARCHAR(16777216)','CH_EMPLOYER_ID VARCHAR(16777216)',
  'CLAIM_ID VARCHAR(16777216)','CLAIM_LINE_ID VARCHAR(16777216)',
  'SERVICE_PROVIDER_ID VARCHAR(16777216)','PROVIDER_NPI VARCHAR(16777216)',
  'FACILITY_NPI VARCHAR(16777216)','BILLING_PROVIDER_NPI VARCHAR(16777216)',
  'PROVIDER_TIN VARCHAR(16777216)','PROVIDER_TYPE VARCHAR(16777216)',
  'PROVIDER_FIRST_NAME VARCHAR(16777216)','PROVIDER_MIDDLE_NAME VARCHAR(16777216)',
  'PROVIDER_LAST_NAME VARCHAR(16777216)','PROVIDER_NAME VARCHAR(16777216)',
  'PROVIDER_SPECIALTY_1_CODE VARCHAR(16777216)','PROVIDER_SPECIALTY_1_DESCRIPTION VARCHAR(16777216)',
  'PROVIDER_SPECIALTY_2_CODE VARCHAR(16777216)','PROVIDER_SPECIALTY_2_DESCRIPTION VARCHAR(16777216)',
  'PROVIDER_SPECIALTY_3_CODE VARCHAR(16777216)','PROVIDER_SPECIALTY_3_DESCRIPTION VARCHAR(16777216)',
  'PROVIDER_ADDRESS_1 VARCHAR(16777216)','PROVIDER_ADDRESS_2 VARCHAR(16777216)',
  'PROVIDER_CITY VARCHAR(16777216)','PROVIDER_COUNTY VARCHAR(16777216)',
  'PROVIDER_STATE VARCHAR(16777216)','PROVIDER_ZIP_CODE VARCHAR(16777216)',
  'PLACE_OF_SERVICE_CODE VARCHAR(16777216)','PLACE_OF_SERVICE_DESCRIPTION VARCHAR(16777216)',
  'DIAGNOSIS_TYPE VARCHAR(16777216)','ADMITTING_DIAGNOSIS_CODE VARCHAR(16777216)',
  'ADMITTING_DIAGNOSIS_DESCRIPTION VARCHAR(16777216)',
  ...Array.from({length:25},(_,i)=>`DIAGNOSIS_${i+1}_CODE VARCHAR(16777216),DIAGNOSIS_${i+1}_DESCRIPTION VARCHAR(16777216)`),
  'PROCEDURE_TYPE VARCHAR(16777216)','PROCEDURE_CODE VARCHAR(16777216)',
  'PROCEDURE_DESCRIPTION VARCHAR(16777216)','REVENUE_CODE VARCHAR(16777216)',
  'REVENUE_DESCRIPTION VARCHAR(16777216)','CPT_CODE VARCHAR(16777216)',
  'CPT_DESCRIPTION VARCHAR(16777216)','CPT_LONG_DESCRIPTION VARCHAR(16777216)',
  'ICD_PROCEDURE_1_CODE VARCHAR(16777216)','ICD_PROCEDURE_1_DESCRIPTION VARCHAR(16777216)',
  'ICD_PROCEDURE_2_CODE VARCHAR(16777216)','ICD_PROCEDURE_2_DESCRIPTION VARCHAR(16777216)',
  'DRG_TYPE_CODE VARCHAR(16777216)','DRG_TYPE_DESCRIPTION VARCHAR(16777216)',
  'DRG_CODE VARCHAR(16777216)','DRG_DESCRIPTION VARCHAR(16777216)',
  'HCPCS_CODE VARCHAR(16777216)','HCPCS_DESCRIPTION VARCHAR(16777216)',
  'HCPCS_LONG_DESCRIPTION VARCHAR(16777216)',
  'PROCEDURE_MODIFIER_1_CODE VARCHAR(16777216)','PROCEDURE_MODIFIER_1_DESCRIPTION VARCHAR(16777216)',
  'PROCEDURE_MODIFIER_2_CODE VARCHAR(16777216)','PROCEDURE_MODIFIER_2_DESCRIPTION VARCHAR(16777216)',
  'PROCEDURE_MODIFIER_3_CODE VARCHAR(16777216)','PROCEDURE_MODIFIER_3_DESCRIPTION VARCHAR(16777216)',
  'ADMISSION_TYPE VARCHAR(16777216)','SERVICE_FROM_DATE DATE','SERVICE_TO_DATE DATE',
  'ADJUDICATION_DATE DATE','PAID_DATE DATE','BENEFIT_CODE VARCHAR(16777216)',
  'BENEFIT_DESCRIPTION VARCHAR(16777216)','ALLOWED_AMOUNT FLOAT','BILLED_AMOUNT FLOAT',
  'COB_PAID_AMOUNT FLOAT','COINSURANCE_AMOUNT FLOAT','COPAY_AMOUNT FLOAT',
  'DEDUCTIBLE_AMOUNT FLOAT','NOT_COVERED_AMOUNT FLOAT','OTHER_SAVINGS FLOAT',
  'DISCOUNT_AMOUNT FLOAT','PAID_AMOUNT FLOAT','PAYMENT_TYPE VARCHAR(16777216)',
  'CHECK_NUMBER VARCHAR(16777216)','ORIGINAL_CLAIM_ID VARCHAR(16777216)',
  'MERGE_DT DATE','MERGED_FROM_CH_MEMBER_ID VARCHAR(16777216)',
  'MERGED_FROM_MEMBER_ID VARCHAR(16777216)','SOURCEFILENAME VARCHAR(16777216)',
  'RECEIVED_DATE DATE','IMPORT_DATE DATE','SCRUBBED_DATE DATE',
  'TYPE_OF_BILL VARCHAR(16777216)','CLAIM_NETWORK_TYPE VARCHAR(16777216)',
  'DISCHARGE_STATUS VARCHAR(16777216)','ADMIT_DATE DATE','DISCHARGE_DATE DATE',
  'UNITS_QUANTITY VARCHAR(16777216)','PLAN_PAID_AMOUNT FLOAT','MEMBER_PAID_AMOUNT FLOAT',
  'UDF1 VARCHAR(16777216)','UDF2 VARCHAR(16777216)','UDF3 VARCHAR(16777216)',
  'UDF4 VARCHAR(16777216)','UDF5 VARCHAR(16777216)','UDF6 VARCHAR(16777216)',
  'UDF7 VARCHAR(16777216)','UDF8 VARCHAR(16777216)','UDF9 VARCHAR(16777216)',
  'UDF10 VARCHAR(16777216)'
];

const MED_DEFAULTS = [
  {target:'CH_MEDICAL_CLAIM_ID',type:'null',value:''},
  {target:'RECORD_ID',type:'null',value:''},
  {target:'MEMBER_ID',type:'direct',value:'LGCY_MBR_NUM'},
  {target:'GROUP_ID',type:'direct',value:'CLIENT_ACCT_NUM'},
  {target:'CH_MEMBER_ID',type:'null',value:''},
  {target:'CH_EMPLOYER_ID',type:'hardcoded',value:'802'},
  {target:'CLAIM_ID',type:'direct',value:'CLM_SYS_CLM_ID'},
  {target:'CLAIM_LINE_ID',type:'direct',value:'SVC_LN_NUM'},
  {target:'SERVICE_PROVIDER_ID',type:'direct',value:'RNDR_PROV_ID'},
  {target:'PROVIDER_NPI',type:'expression',value:"COALESCE(LPAD(RNDR_PROV_NPI,10,'0'),LPAD(BILL_PROV_NPI,10,'0'),LPAD(REFRL_PROV_NPI,10,'0'))"},
  {target:'FACILITY_NPI',type:'null',value:''},
  {target:'BILLING_PROVIDER_NPI',type:'expression',value:"COALESCE(LPAD(BILL_PROV_NPI,10,'0'),LPAD(RNDR_PROV_NPI,10,'0'))"},
  {target:'PROVIDER_TIN',type:'direct',value:'BILL_PROV_TIN'},
  {target:'PROVIDER_TYPE',type:'null',value:''},
  {target:'PROVIDER_FIRST_NAME',type:'null',value:''},
  {target:'PROVIDER_MIDDLE_NAME',type:'null',value:''},
  {target:'PROVIDER_LAST_NAME',type:'null',value:''},
  {target:'PROVIDER_NAME',type:'null',value:''},
  {target:'PROVIDER_SPECIALTY_1_CODE',type:'null',value:''},
  {target:'PROVIDER_SPECIALTY_1_DESCRIPTION',type:'null',value:''},
  {target:'PROVIDER_SPECIALTY_2_CODE',type:'null',value:''},
  {target:'PROVIDER_SPECIALTY_2_DESCRIPTION',type:'null',value:''},
  {target:'PROVIDER_SPECIALTY_3_CODE',type:'null',value:''},
  {target:'PROVIDER_SPECIALTY_3_DESCRIPTION',type:'null',value:''},
  {target:'PROVIDER_ADDRESS_1',type:'null',value:''},
  {target:'PROVIDER_ADDRESS_2',type:'null',value:''},
  {target:'PROVIDER_CITY',type:'null',value:''},
  {target:'PROVIDER_COUNTY',type:'null',value:''},
  {target:'PROVIDER_STATE',type:'null',value:''},
  {target:'PROVIDER_ZIP_CODE',type:'null',value:''},
  {target:'PLACE_OF_SERVICE_CODE',type:'expression',value:"LPAD(TRIM(REGEXP_REPLACE(DERV_POS_CD,'[^0-9]', '')),2,'0')"},
  {target:'PLACE_OF_SERVICE_DESCRIPTION',type:'null',value:''},
  {target:'DIAGNOSIS_TYPE',type:'null',value:''},
  {target:'ADMITTING_DIAGNOSIS_CODE',type:'null',value:''},
  {target:'ADMITTING_DIAGNOSIS_DESCRIPTION',type:'null',value:''},
  ...Array.from({length:25},(_,i)=>([
    {target:`DIAGNOSIS_${i+1}_CODE`,type:'expression',value:`TRIM(REGEXP_REPLACE(CLM_DIAG_CD_${i+1}, '[^A-Za-z0-9]', ''))`},
    {target:`DIAGNOSIS_${i+1}_DESCRIPTION`,type:'null',value:''},
  ])).flat(),
  {target:'PROCEDURE_TYPE',type:'expression',value:"CASE WHEN PROC_TY_CD = 'CP' THEN 'CPT4' WHEN PROC_TY_CD = 'HC' THEN 'HCPCS' WHEN PROC_TY_CD = 'AD' THEN 'CDT' ELSE PROC_TY_CD END"},
  {target:'PROCEDURE_CODE',type:'direct',value:'PROC_CD'},
  {target:'PROCEDURE_DESCRIPTION',type:'null',value:''},
  {target:'REVENUE_CODE',type:'expression',value:"'R'||REGEXP_REPLACE(TRIM(REVNU_CD), '^0', '')"},
  {target:'REVENUE_DESCRIPTION',type:'null',value:''},
  {target:'CPT_CODE',type:'expression',value:"CASE WHEN PROC_TY_CD = 'CP' AND LENGTH(TRIM(REGEXP_REPLACE(PROC_CD, '[^A-Za-z0-9]', ''))) = '5' AND REGEXP_LIKE(TRIM(REGEXP_REPLACE(PROC_CD, '[^A-Za-z0-9]', '')), '^[0-9]{4}[A-Za-z]$|^[0-9]{5}$') THEN TRIM(REGEXP_REPLACE(PROC_CD, '[^A-Za-z0-9]', '')) ELSE NULL END"},
  {target:'CPT_DESCRIPTION',type:'null',value:''},
  {target:'CPT_LONG_DESCRIPTION',type:'null',value:''},
  {target:'ICD_PROCEDURE_1_CODE',type:'expression',value:'COALESCE(ICD_PROC_CD_1,ICD_PROC_CD_2,ICD_PROC_CD_3,ICD_PROC_CD_4,ICD_PROC_CD_5)'},
  {target:'ICD_PROCEDURE_1_DESCRIPTION',type:'null',value:''},
  {target:'ICD_PROCEDURE_2_CODE',type:'expression',value:'COALESCE(ICD_PROC_CD_2,ICD_PROC_CD_3,ICD_PROC_CD_4,ICD_PROC_CD_5)'},
  {target:'ICD_PROCEDURE_2_DESCRIPTION',type:'null',value:''},
  {target:'DRG_TYPE_CODE',type:'null',value:''},
  {target:'DRG_TYPE_DESCRIPTION',type:'null',value:''},
  {target:'DRG_CODE',type:'direct',value:'DRG_CD'},
  {target:'DRG_DESCRIPTION',type:'null',value:''},
  {target:'HCPCS_CODE',type:'expression',value:"CASE WHEN PROC_TY_CD = 'HC' AND LENGTH(TRIM(REGEXP_REPLACE(PROC_CD, '[^A-Za-z0-9]', ''))) = '5' AND REGEXP_LIKE(TRIM(REGEXP_REPLACE(PROC_CD, '[^A-Za-z0-9]', '')), '^[A-Za-z][0-9]{4}$') THEN TRIM(REGEXP_REPLACE(PROC_CD, '[^A-Za-z0-9]', '')) ELSE NULL END"},
  {target:'HCPCS_DESCRIPTION',type:'null',value:''},
  {target:'HCPCS_LONG_DESCRIPTION',type:'null',value:''},
  {target:'PROCEDURE_MODIFIER_1_CODE',type:'expression',value:'COALESCE(PROC_CD_MOD_CD_1,PROC_CD_MOD_CD_2,PROC_CD_MOD_CD_3,PROC_CD_MOD_CD_4)'},
  {target:'PROCEDURE_MODIFIER_1_DESCRIPTION',type:'null',value:''},
  {target:'PROCEDURE_MODIFIER_2_CODE',type:'expression',value:'COALESCE(PROC_CD_MOD_CD_2,PROC_CD_MOD_CD_3,PROC_CD_MOD_CD_4)'},
  {target:'PROCEDURE_MODIFIER_2_DESCRIPTION',type:'null',value:''},
  {target:'PROCEDURE_MODIFIER_3_CODE',type:'expression',value:'COALESCE(PROC_CD_MOD_CD_3,PROC_CD_MOD_CD_4)'},
  {target:'PROCEDURE_MODIFIER_3_DESCRIPTION',type:'null',value:''},
  {target:'ADMISSION_TYPE',type:'null',value:''},
  {target:'SERVICE_FROM_DATE',type:'expression',value:"TO_DATE(SVC_BEG_DT,'YYYYMMDD')"},
  {target:'SERVICE_TO_DATE',type:'expression',value:"TO_DATE(SVC_END_DT,'YYYYMMDD')"},
  {target:'ADJUDICATION_DATE',type:'null',value:''},
  {target:'PAID_DATE',type:'expression',value:"TO_DATE(PD_DT,'YYYYMMDD')"},
  {target:'BENEFIT_CODE',type:'null',value:''},
  {target:'BENEFIT_DESCRIPTION',type:'null',value:''},
  {target:'ALLOWED_AMOUNT',type:'expression',value:"IFNULL(DERV_COINS_AMT::FLOAT,'0.00')+IFNULL(DERV_COPAY_AMT::FLOAT,'0.00')+IFNULL(DERV_DDCTBL_AMT::FLOAT,'0.00')+IFNULL(DERV_PAYBL_AMT::FLOAT,'0.00')"},
  {target:'BILLED_AMOUNT',type:'expression',value:"IFNULL(DERV_COINS_AMT::FLOAT,'0.00')+IFNULL(DERV_COPAY_AMT::FLOAT,'0.00')+IFNULL(DERV_DDCTBL_AMT::FLOAT,'0.00')+IFNULL(DERV_PAYBL_AMT::FLOAT,'0.00')"},
  {target:'COB_PAID_AMOUNT',type:'null',value:''},
  {target:'COINSURANCE_AMOUNT',type:'expression',value:"IFNULL(DERV_COINS_AMT::FLOAT,'0.00')"},
  {target:'COPAY_AMOUNT',type:'expression',value:"IFNULL(DERV_COPAY_AMT::FLOAT,'0.00')"},
  {target:'DEDUCTIBLE_AMOUNT',type:'expression',value:"IFNULL(DERV_DDCTBL_AMT::FLOAT,'0.00')"},
  {target:'NOT_COVERED_AMOUNT',type:'null',value:''},
  {target:'OTHER_SAVINGS',type:'expression',value:"IFNULL(DERV_COB_CIGNA_SAVE_AMT::FLOAT,'0.00')"},
  {target:'DISCOUNT_AMOUNT',type:'null',value:''},
  {target:'PAID_AMOUNT',type:'expression',value:"IFNULL(DERV_COINS_AMT::FLOAT,'0.00')+IFNULL(DERV_COPAY_AMT::FLOAT,'0.00')+IFNULL(DERV_DDCTBL_AMT::FLOAT,'0.00')+IFNULL(DERV_PAYBL_AMT::FLOAT,'0.00')"},
  {target:'PAYMENT_TYPE',type:'null',value:''},
  {target:'CHECK_NUMBER',type:'null',value:''},
  {target:'ORIGINAL_CLAIM_ID',type:'null',value:''},
  {target:'MERGE_DT',type:'null',value:''},
  {target:'MERGED_FROM_CH_MEMBER_ID',type:'null',value:''},
  {target:'MERGED_FROM_MEMBER_ID',type:'null',value:''},
  {target:'SOURCEFILENAME',type:'direct',value:'SOURCEFILENAME'},
  {target:'RECEIVED_DATE',type:'direct',value:'FILE_RECEIVED_DATE'},
  {target:'IMPORT_DATE',type:'direct',value:'IMPORT_DATE'},
  {target:'SCRUBBED_DATE',type:'expression',value:'CURRENT_DATE()'},
  {target:'TYPE_OF_BILL',type:'direct',value:'TY_OF_BILL_CD'},
  {target:'CLAIM_NETWORK_TYPE',type:'expression',value:"CASE WHEN BEN_NT_LVL_CD = 'INNT' THEN 'IN-NETWORK' WHEN BEN_NT_LVL_CD = 'OUTNT' THEN 'OUT-NETWORK' WHEN BEN_NT_LVL_CD = 'INDEM' THEN 'INDEMITY' END"},
  {target:'DISCHARGE_STATUS',type:'expression',value:"A.DISCHRG_STAT_CD||'-'||F.DISCHRG_STAT_DESC"},
  {target:'ADMIT_DATE',type:'null',value:''},
  {target:'DISCHARGE_DATE',type:'null',value:''},
  {target:'UNITS_QUANTITY',type:'direct',value:'SU_CNT'},
  {target:'PLAN_PAID_AMOUNT',type:'expression',value:"IFNULL(DERV_PAYBL_AMT::FLOAT,'0.00')"},
  {target:'MEMBER_PAID_AMOUNT',type:'expression',value:"IFNULL(DERV_COINS_AMT::FLOAT,'0.00')+IFNULL(DERV_COPAY_AMT::FLOAT,'0.00')+IFNULL(DERV_DDCTBL_AMT::FLOAT,'0.00')"},
  {target:'UDF1',type:'expression',value:"IFNULL(PATNT_FRST_NM,'X')||'_'||IFNULL(PATNT_LAST_NM,'X')||'_'||COALESCE(TO_DATE(PATNT_BRTH_DT,'YYYYMMDD'),'99991231')||'_'||IFNULL(PATNT_GENDR_CD,'U')||'_'||IFNULL(AMI,'X')||'_'||IFNULL(PATNT_RELSHP_CD,'X')||'_'||IFNULL(INDIV_ENTPR_ID,'X')"},
  {target:'UDF2',type:'expression',value:'B.DESCRIPTION'},
  {target:'UDF3',type:'expression',value:"A.NT_ID||'-'||C.NETWORK_NAME"},
  {target:'UDF4',type:'expression',value:"A.SVC_MINR_CD||'-'||D.DESCRIPTION"},
  {target:'UDF5',type:'expression',value:"A.SVC_MAJ_CD||'-'||E.DESCRIPTION"},
  {target:'UDF6',type:'direct',value:'ACCT_TY_CD'},
  {target:'UDF7',type:'direct',value:'NEW_CLM_SYS_CLM_ID'},
  {target:'UDF8',type:'direct',value:'LN_PROCS_EVENT_CD'},
  {target:'UDF9',type:'direct',value:'CLM_EVENT_KEY'},
  {target:'UDF10',type:'direct',value:'BEN_NT_LVL_CD'},
];

// ─────────────────────────────────────────────────────────────
// SQL GENERATORS
// ─────────────────────────────────────────────────────────────
function generatePharmacySQL(cfg, rawFields, maps) {
  const esc = s => s.replace(/'/g, "''");
  const pad = (n, s) => ' '.repeat(n) + s;
  const dedupCols = rawFields.filter(f => !PX_MAX_FIELDS.has(f))
    .map(f => pad(20, `,UPPER(NULLIF(TRIM(${f}), '')) AS ${f}`)).join('\n');
  const maxCols = [...PX_MAX_FIELDS].filter(f => rawFields.includes(f))
    .map(f => pad(20, `,MAX(${f}) AS ${f}`)).join('\n');
  const ddlBody = PX_STAGE_DDL.map((c,i) => pad(22, (i===0?'':',')+c)).join('\n');
  const insertBody = maps.map((m,i) => {
    let expr = m.type==='null' ? 'NULL' : m.type==='direct' ? m.value
      : m.type==='hardcoded' ? `''${esc(m.value)}''` : esc(m.value);
    return `${i===0 ? pad(21,'') : pad(21,', ')}${expr} AS ${m.target}`;
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

function generateMedicalSQL(cfg, rawFields, maps) {
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
                     WHERE UPPER(SOURCEFILENAME) NOT LIKE ''${cfg.crosswalkFilter}''
                     GROUP BY ALL';

  EXECUTE IMMEDIATE 'CREATE OR REPLACE TEMPORARY TABLE ${stageTable}
                     (
${ddlBody}
                     )';

  EXECUTE IMMEDIATE 'INSERT INTO ${stageTable}
                     SELECT
${insertBody}
                     FROM ${cleanedTable}'||CURRENTTIMESTAMP||' A
                     LEFT JOIN ${cfg.mdcCrosswalk} B USING(MDC_CD)
                     LEFT JOIN ${cfg.networkCrosswalk} C USING(NT_ID)
                     LEFT JOIN ${cfg.svcMinrCrosswalk} D USING(SVC_MINR_CD)
                     LEFT JOIN ${cfg.svcMajCrosswalk} E USING(SVC_MAJ_CD)
                     LEFT JOIN ${cfg.dischrgCrosswalk} F USING(DISCHRG_STAT_CD)';

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

// ─────────────────────────────────────────────────────────────
// SHARED UI CONSTANTS
// ─────────────────────────────────────────────────────────────
const TYPE_META = {
  null:       {label:'NULL',       color:'#ef4444', bg:'rgba(239,68,68,0.12)',   border:'rgba(239,68,68,0.3)'},
  direct:     {label:'DIRECT',     color:'#22c55e', bg:'rgba(34,197,94,0.12)',   border:'rgba(34,197,94,0.3)'},
  hardcoded:  {label:'HARDCODED',  color:'#f59e0b', bg:'rgba(245,158,11,0.12)', border:'rgba(245,158,11,0.3)'},
  expression: {label:'EXPRESSION', color:'#a855f7', bg:'rgba(168,85,247,0.12)', border:'rgba(168,85,247,0.3)'},
};
const TYPE_ORDER = ['null','direct','hardcoded','expression'];

const MODE_META = {
  pharmacy: { label:'Pharmacy', icon:'💊', color:'#3b82f6', bg:'rgba(59,130,246,0.15)', border:'rgba(59,130,246,0.4)' },
  medical:  { label:'Medical',  icon:'🏥', color:'#10b981', bg:'rgba(16,185,129,0.15)', border:'rgba(16,185,129,0.4)' },
};

const S = {
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
    borderBottom: active ? `2px solid ${color}` : '2px solid transparent',
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

/** Default raw fields, mapping rows, and config object for pharmacy vs medical mode. */
function getModeDefaults(mode) {
  const isPx = mode === 'pharmacy';
  const storePfx = isPx ? 'px' : 'med';
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
      mdcCrosswalk: 'MDC_CODE_DESC_CXWLK',
      networkCrosswalk: 'NETWORK_ID_NAME_CXWLK',
      svcMinrCrosswalk: 'SVC_MINR_CD_CXWLK',
      svcMajCrosswalk: 'SVC_MAJ_CD_CXWLK',
      dischrgCrosswalk: 'DISCHRG_STAT_CD_CXWLK',
      crosswalkFilter: '%CONTROLS%',
    },
  };
}

// ─────────────────────────────────────────────────────────────
// ROOT APP — mode switcher lives here
// ─────────────────────────────────────────────────────────────
export default function App() {
  const [mode, setMode] = useState('pharmacy');
  const m = MODE_META[mode];

  return (
    <div style={S.app}>
      <style>{GLOBAL_STYLE}</style>

      {/* Sticky header with mode switcher */}
      <header style={S.header}>
        <div style={S.logo}>
          <div style={{...S.logoIcon, background:`${m.color}22`, border:`1px solid ${m.border}`}}>
            {m.icon}
          </div>
          <div>
            <div style={{fontSize:15,fontWeight:700,letterSpacing:'-0.3px'}}>
              {m.label} Scrub Builder
            </div>
            <div style={{fontSize:11,color:'#6b7280',letterSpacing:'0.5px'}}>
              SNOWFLAKE SP GENERATOR · {mode.toUpperCase()} CLAIMS
            </div>
          </div>
        </div>

        {/* Mode switcher */}
        <div style={S.modeSwitcher}>
          {Object.keys(MODE_META).map(k => (
            <button key={k} style={S.modeBtn(mode===k, k)} onClick={() => setMode(k)}>
              <span>{MODE_META[k].icon}</span>
              <span>{MODE_META[k].label}</span>
            </button>
          ))}
        </div>

        <div style={{display:'flex',gap:10,alignItems:'center'}}>
          <button style={S.btn('ghost')} onClick={() => {
            if (window.__resetMode) window.__resetMode();
          }}>↺ Reset to Default</button>
          <button style={{...S.btn('primary'), background: m.color}}
            onClick={() => { if (window.__generateSQL) window.__generateSQL(); }}>
            ⚙ Generate SQL
          </button>
        </div>
      </header>

      {/* Mode-keyed builder — remounts on mode switch, preserving per-mode storage */}
      <ScrubBuilderWithRef key={mode} mode={mode} accent={m.color} />
    </div>
  );
}

// Wrapper to expose reset/generate to the header buttons
function ScrubBuilderWithRef({ mode, accent }) {
  const [tab, setTab] = useState('config');
  const { storePfx, isPx, defaultRaw, defaultMaps, defaultCfg } = useMemo(
    () => getModeDefaults(mode),
    [mode]
  );

  const [cfg, setCfg]             = useState(defaultCfg);
  const [rawFields, setRawFields] = useState(defaultRaw);
  const [maps, setMaps]           = useState(defaultMaps.map(m=>({...m})));
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
        if (c) setCfg(JSON.parse(c.value));
        if (f) setRawFields(JSON.parse(f.value));
        if (m) setMaps(JSON.parse(m.value));
      } catch {}
    })();
  }, [storePfx]);

  const persist = useCallback(async (c, f, m) => {
    try {
      await window.storage.set(`${storePfx}_cfg`,    JSON.stringify(c));
      await window.storage.set(`${storePfx}_fields`, JSON.stringify(f));
      await window.storage.set(`${storePfx}_maps`,   JSON.stringify(m));
    } catch {}
  }, [storePfx]);

  useEffect(() => { persist(cfg, rawFields, maps); }, [cfg, rawFields, maps]);

  const doGenerate = useCallback(() => {
    const sql = isPx
      ? generatePharmacySQL(cfg, rawFields, maps)
      : generateMedicalSQL(cfg, rawFields, maps);
    setSqlOutput(sql);
    setTab('generate');
  }, [cfg, rawFields, maps, isPx]);

  const doReset = useCallback(() => {
    setRawFields(defaultRaw);
    setMaps(defaultMaps.map(m => ({ ...m })));
    setCfg(defaultCfg);
  }, [defaultRaw, defaultMaps, defaultCfg]);

  // Expose to header buttons via window (simple cross-component bridge)
  useEffect(() => {
    window.__generateSQL = doGenerate;
    window.__resetMode   = doReset;
    return () => { window.__generateSQL = null; window.__resetMode = null; };
  }, [doGenerate, doReset]);

  const updateMap = (idx, key, val) =>
    setMaps(prev => { const n=[...prev]; n[idx]={...n[idx],[key]:val}; return n; });

  const cycleType = (idx) =>
    setMaps(prev => { const n=[...prev]; const c=TYPE_ORDER.indexOf(n[idx].type);
      n[idx]={...n[idx],type:TYPE_ORDER[(c+1)%4],value:''}; return n; });

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
    a.download = `SP_SCRUB_STREAM_${cfg.clientName}_${isPx?'PHARMACY':'MEDICAL'}.sql`;
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

  const filteredMaps = useMemo(
    () =>
      maps
        .map((m, i) => ({ ...m, _i: i }))
        .filter(m => {
          const typeOk = typeFilter === 'all' || m.type === typeFilter;
          const q = search.toLowerCase();
          const searchOk =
            !search ||
            m.target.toLowerCase().includes(q) ||
            m.value.toLowerCase().includes(q);
          return typeOk && searchOk;
        }),
    [maps, typeFilter, search]
  );

  const counts = useMemo(() => {
    const c = { all: maps.length };
    TYPE_ORDER.forEach(t => {
      c[t] = maps.filter(m => m.type === t).length;
    });
    return c;
  }, [maps]);

  const cfgInput = (key, label, ph='') => (
    <div style={S.col()}>
      <label style={S.label}>{label}</label>
      <input style={S.input} value={cfg[key]||''} placeholder={ph}
        onChange={e=>setCfg(p=>({...p,[key]:e.target.value.toUpperCase()}))} />
    </div>
  );

  const spName = `${cfg.schema}.SP_SCRUB_STREAM_${cfg.clientName}_${isPx?'PHARMACY':'MEDICAL'}()`;

  return (
    <div>
      <nav style={S.nav}>
        {['config','fields','mappings','generate'].map(t=>(
          <button key={t} style={S.navTab(tab===t,accent)} onClick={()=>{
            if(t==='generate') doGenerate(); else setTab(t);
          }}>
            {t==='config'   && '01 · Client Config'}
            {t==='fields'   && `02 · Raw Fields (${rawFields.length})`}
            {t==='mappings' && `03 · Mappings (${maps.filter(m=>m.type!=='null').length} active)`}
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
            <div style={S.row}>
              {cfgInput('masterProviders','Master Providers')}
              {isPx ? cfgInput('masterDrug','Master Drug') : cfgInput('masterProcedure','Master Procedure')}
            </div>
            {!isPx && (<>
              <div style={S.row}>
                {cfgInput('masterProcedureModifier','Master Procedure Modifier')}
                {cfgInput('masterDiagnosis','Master Diagnosis')}
              </div>
              <div style={S.row}>{cfgInput('masterPos','Master POS')}<div style={S.col()}/></div>
              <div style={S.divider}/>
              <div style={S.sectionTitle}>Crosswalk Tables</div>
              <div style={S.row}>
                {cfgInput('mdcCrosswalk','MDC Crosswalk')}
                {cfgInput('networkCrosswalk','Network Crosswalk')}
              </div>
              <div style={S.row}>
                {cfgInput('svcMinrCrosswalk','Svc Minor Crosswalk')}
                {cfgInput('svcMajCrosswalk','Svc Major Crosswalk')}
              </div>
              <div style={S.row}>
                {cfgInput('dischrgCrosswalk','Discharge Crosswalk')}
                {cfgInput('crosswalkFilter','Source File Exclusion Filter')}
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
                <div style={{fontSize:12,color:'#6b7280'}}>Drag to reorder · Generates the UPPER/TRIM dedup block</div>
                {!isPx && (
                  <div style={{marginTop:6,padding:'8px 12px',background:'rgba(245,158,11,0.08)',
                    border:'1px solid rgba(245,158,11,0.25)',borderRadius:6,fontSize:11,color:'#f59e0b'}}>
                    ⚠ DERV_COINS/COPAY/DDCTBL/COB_CIGNA_SAVE_AMT are hardcoded '0.00' in dedup — managed by template.
                    SVC_MINR_CD_1 auto-aliased → SVC_MINR_CD.
                  </div>
                )}
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
          <div style={{display:'flex',gap:10,alignItems:'center',marginBottom:14,flexWrap:'wrap'}}>
            <input style={{...S.input,width:240}} placeholder="Search target field or value..."
              value={search} onChange={e=>setSearch(e.target.value)} />
            <div style={{display:'flex',gap:6,flexWrap:'wrap'}}>
              {['all',...TYPE_ORDER].map(t=>(
                <button key={t} style={S.pillFilter(typeFilter===t,accent)} onClick={()=>setTypeFilter(t)}>
                  {t.toUpperCase()} {counts[t]||''}
                </button>
              ))}
            </div>
            <div style={{marginLeft:'auto',fontSize:11,color:'#4b5563'}}>Click badge to cycle type</div>
          </div>
          <div style={S.tableContainer}>
            <table style={S.table}>
              <thead>
                <tr>
                  <th style={{...S.th,width:40}}>#</th>
                  <th style={S.th}>Target Field</th>
                  <th style={{...S.th,width:110}}>Type</th>
                  <th style={S.th}>Expression / Value</th>
                </tr>
              </thead>
              <tbody>
                {filteredMaps.map(m=>{
                  const i=m._i;
                  return (
                    <tr key={m.target} style={{background:i%2===0?'#0f1521':'transparent'}}>
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
                      <td style={{...S.tdBase,width:'55%'}}>
                        {m.type==='null' ? (
                          <span style={{fontFamily:"'DM Mono',monospace",fontSize:11,color:'#374151'}}>— NULL —</span>
                        ) : m.type==='direct' ? (
                          <select style={{...S.typeSelect,width:'100%'}} value={m.value}
                            onChange={e=>updateMap(i,'value',e.target.value)}>
                            <option value="">— select raw field —</option>
                            {rawFields.map(f=><option key={f} value={f}>{f}</option>)}
                          </select>
                        ) : m.type==='hardcoded' ? (
                          <input style={{...S.input,width:'100%',padding:'5px 10px'}}
                            value={m.value} onChange={e=>updateMap(i,'value',e.target.value)}
                            placeholder="Literal value (no quotes needed)" />
                        ) : (
                          <textarea ref={el=>textareaRefs.current[i]=el}
                            style={{...S.input,width:'100%',resize:'vertical',minHeight:36,padding:'5px 10px',lineHeight:1.5}}
                            value={m.value} rows={m.value.length>80?3:1}
                            onChange={e=>updateMap(i,'value',e.target.value)}
                            placeholder="SQL expression" />
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
                <span style={{fontSize:18,fontWeight:800,color:TYPE_META[t].color}}>{counts[t]}</span>
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
}