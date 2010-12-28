CREATE OR REPLACE PACKAGE TDM_USER.PKG_TESTDATA_SAVE AS
/******************************************************************************
   NAME:       PKG_TESTDATA_SAVE
   PURPOSE:

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        11/17/2009    Elango         1. Created this package.
******************************************************************************/
   TYPE chld is RECORD
   (
    constraint_name VARCHAR2(100),
    c_table_name VARCHAR2(100),
    c_column_name VARCHAR2(100),
    p_table_name VARCHAR2(200),
    p_column_name VARCHAR2(100)
   );

   TYPE vChld is varray(200) of chld;
   TYPE tblChld is TABLE of vChld index by VARCHAR2(200);
   my_tblChld tblChld;
   chldRelationshipsSaved VARCHAR2(1) := null;

      TYPE pKey is RECORD
   (
       pk_col_name VARCHAR2(100),
       table_name VARCHAR2(200)
   );
   TYPE pKeys is varray(1000) of pKey;
   TYPE tblpKeys IS TABLE of pKeys index by VARCHAR2(200);
   my_tbluiKeys tblpKeys;
   my_tblpKeys tblpKeys;
   delete_sequence_id NUMBER := 0;
   delete_sequence NUMBER;
   save_sequence NUMBER;

   TDM_EXCEPTION exception;
   PROCEDURE SaveTDMDataToTargetSchema(source_schema_name IN VARCHAR2,target_schema_name IN VARCHAR2,create_new_keys_flg IN VARCHAR2, tdm_data_id IN VARCHAR2 default null);
   PROCEDURE InsertDataFromXML(source_schema_name IN VARCHAR2,target_schema_name IN VARCHAR2,tdm_data_id IN VARCHAR2 ,seq_no IN VARCHAR2 default null);
   PROCEDURE enableDisableConstraints(status IN VARCHAR2,target_schema_name IN VARCHAR2,cons_type VARCHAR2,seq_no IN VARCHAR2 default null);
   FUNCTION getResultsFromKeyTables(tab_nam IN VARCHAR2,col_nam IN VARCHAR2 ,col_val VARCHAR2,source_schema_name IN VARCHAR2) RETURN VARCHAR2;
   PROCEDURE enableDisableTriggers(status IN VARCHAR2,schema_name IN VARCHAR2,seq_no IN VARCHAR2 default null);
   FUNCTION validateDate(xml_field_val VARCHAR2) RETURN VARCHAR2;
   FUNCTION getUpdatedXmlKeys(ins_data IN XMLTYPE,tab_name IN VARCHAR2,crt_dttm IN VARCHAR2,source_schema_name IN VARCHAR2) RETURN XMLTYPE ;
   FUNCTION validateseq(source_schema_name IN VARCHAR2,target_schema_name IN VARCHAR2,SEQ_NO IN VARCHAR2) RETURN VARCHAR2;
   PROCEDURE PopulateNewColValues(source_schema_name IN VARCHAR2,target_schema_name IN VARCHAR2,SEQ_NO IN VARCHAR2);
   PROCEDURE updateRunStatus(runstatus IN VARCHAR2,target_schema_name IN VARCHAR2);
   PROCEDURE updateJobStatus(jobstatus IN VARCHAR2,target_schema_name IN VARCHAR2);
   PROCEDURE savePrimaryColumns(sname in VARCHAR2);
   PROCEDURE saveChildRelationships(sname in VARCHAR2);
   PROCEDURE delChildrenRows(sname IN VARCHAR2, tab_name IN VARCHAR2,w_cls VARCHAR2,s_cls VARCHAR2,parent_flag VARCHAR2 default 'Y',parentRunId VARCHAR2 default '0',seq_no IN VARCHAR2 default null);
   --FUNCTION getPrimaryColumn(sname IN VARCHAR2, tab_name IN VARCHAR2) return pKeys;
   PROCEDURE PopulateAlternateKeys(source_schema_name IN VARCHAR2,target_schema_name IN VARCHAR2,flag IN VARCHAR2,seq_no IN VARCHAR2 default null);
   --FUNCTION validateSequence(schema_name in VARCHAR2,SEQ_NO IN VARCHAR2) RETURN VARCHAR2;
END PKG_TESTDATA_SAVE;
/