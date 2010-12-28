CREATE OR REPLACE PROCEDURE test_RunTDMTests(target_schema_name IN VARCHAR2,RESULT OUT VARCHAR2) IS
tmpVar NUMBER;
date_format VARCHAR2(50);
/******************************************************************************
   NAME:       test_RunTDMTests
   PURPOSE:    

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        12/17/2009          1. Created this procedure.

   NOTES:

   Automatically available Auto Replace Keywords:
      Object Name:     test_RunTDMTests
      Sysdate:         12/17/2009
      Date and Time:   12/17/2009, 4:26:01 PM, and 12/17/2009 4:26:01 PM
      Username:         (set in TOAD Options, Procedure Editor)
      Table Name:       (set in the "New PL/SQL Object" dialog)

******************************************************************************/
BEGIN
   tmpVar := 0;
  
   -- setup the data.
   test_setupActiveProductData();
   
   
   -- run the gen package here, then check the row count for results.
   -- 
   DECLARE 
      TDM_RUN_ID VARCHAR2(20);
      result_cnt NUMBER;
      RESULT VARCHAR2(10); 
      tmp_cons VARCHAR2(1000);
      tmp_cons_type varchar2(10);
      tmp_status varchar2(10);
      source_schema_name varchar2(20);
    BEGIN 
      RESULT :='FAIL';
      tmp_cons_type :='R';
      tmp_status :='DISABLED';
      source_schema_name :=NULL;
      select TDM_ERR_S.nextval into TDM_RUN_ID FROM DUAL;
      TDM_USER.TEST_SAVE_VALIDATEDATE(TDM_RUN_ID);
      TDM_USER.TEST_SAVE_GETUPDATEDXML(source_schema_name,TDM_RUN_ID);
      TDM_USER.TEST_SAVE_GETPRIMARYCOLUMN(target_schema_name,TDM_RUN_ID);
      TDM_USER.TEST_SAVE_ENABLEDISABLECONS(source_schema_name,TDM_RUN_ID);
      TDM_USER.TEST_SAVE_ENABDISTRIGGERS(source_schema_name,TDM_RUN_ID);
      
      /* Save */
      
      SELECT value 
             into date_format
             FROM v$nls_parameters WHERE parameter ='NLS_DATE_FORMAT';
             execute immediate 'alter session set nls_date_format=''DD-MON-YYYY HH24:MI:SS''';
             execute immediate 'delete from tmp_disabled_constraints where schema_name='''||target_schema_name||'''';
            tmp_cons := 'insert into tmp_disabled_constraints  (select constraint_name,table_name,owner from all_constraints where owner='''||target_schema_name||''''|| ' and constraint_type='''||tmp_cons_type||'''' ||' and status='''||tmp_status||''''||')';
            execute immediate tmp_cons;        
            TDM_USER.PKG_TESTDATA_SAVE.enableDisableConstraints('NO',target_schema_name,'R');
            TDM_USER.PKG_TESTDATA_SAVE.enableDisableTriggers('NO',target_schema_name);
             
      TDM_USER.TEST_SAVE_SAVETOTARGETSCHEMA(TDM_RUN_ID,target_schema_name);
      TDM_USER.PKG_TESTDATA_SAVE.enableDisableConstraints('YES',target_schema_name,'R');
      TDM_USER.PKG_TESTDATA_SAVE.enableDisableTriggers('YES',target_schema_name);
      execute immediate 'alter session set nls_date_format='''||date_format||'''';
       
     /* End Save */  
        
      TDM_USER.TEST_GEN_GETCOL(TDM_RUN_ID);
      TDM_USER.TEST_GEN_EXTRACTDATA(TDM_RUN_ID,'TDM_USER','TMP_PARENT','WHERE BUS_ID=''12''','Product');
      SELECT COUNT(1) into result_cnt FROM TDM_UNIT_TEST WHERE result='Failed' and RUN_ID=TDM_RUN_ID;
      IF result_cnt=0 THEN
        RESULT :='PASS';
      ELSE
        RESULT :='FAIL';
      END IF;
      
      --TDM_USER.TEST_GEN_EXTRACTDATA(RUN_ID,SNAME,TNAME,'');
       --VARCHAR2,sname IN varchar2, tab_name IN VARCHAR2, where_clause IN VARCHAR2, ename IN VARCHAR2

      
      --TDM_USER.PKG_TESTDATA_GEN.GETDATA ( SNAME, ENAME, TNAME, W_CLS );
      COMMIT; 
     EXCEPTION WHEN OTHERS THEN
      INSERT INTO tdm_user.TDM_ERR VALUES (TDM_ERR_S.nextval,'test_RunTDMTests','test_RunTDMTests failed.',sysdate,sysdate,'TDM','TDM');
    END; 
   
   test_RunGenPkgRowCount();
   
   test_teardown();
   
   EXCEPTION
     WHEN NO_DATA_FOUND THEN
       NULL;
     WHEN OTHERS THEN
       INSERT INTO tdm_user.TDM_ERR VALUES (TDM_ERR_S.nextval,'test_RunTDMTests','test_RunTDMTests failed.',sysdate,sysdate,'TDM','TDM');
       RAISE;
END test_RunTDMTests; 
/

