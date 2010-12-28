CREATE OR REPLACE PROCEDURE TEST_GEN_GETCOL(RUN_ID VARCHAR2) IS
tmpVar NUMBER;
retVal VARCHAR2(1000);
/******************************************************************************
   NAME:       TEST_GEN_GETCOL
   PURPOSE:    

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        2/18/2010          1. Created this procedure.

   NOTES:

   Automatically available Auto Replace Keywords:
      Object Name:     TEST_GEN_GETCOL
      Sysdate:         2/18/2010
      Date and Time:   2/18/2010, 10:27:04 AM, and 2/18/2010 10:27:04 AM
      Username:         (set in TOAD Options, Procedure Editor)
      Table Name:       (set in the "New PL/SQL Object" dialog)

******************************************************************************/
BEGIN
   tmpVar := 0;
   retVal := TDM_USER.PKG_TESTDATA_GEN.GETCOLS('TDM_USER','TDM_DATA');
   IF(retVal='DATA_ID,QRY,TNAME,INSRT_DATA,PKEY_DATA,PKEY_STRING,SCHEMA_NAME,CRT_DTTM,LST_UPDT_DTTM,CRT_USER_ID,LST_UPDT_USER_ID,ENTITY_NAME,DATA_FLAG') then
   
    INSERT INTO TDM_UNIT_TEST VALUES(RUN_ID,'GEN','getcols',null,'Pass',sysdate);
   ELSE
     INSERT INTO TDM_UNIT_TEST VALUES(RUN_ID,'GEN','getcols',null,'Fail',sysdate);
   END IF;
   dbms_output.put_line(retVal);
   EXCEPTION
     WHEN NO_DATA_FOUND THEN
       NULL;
     WHEN OTHERS THEN
       -- Consider logging the error and then re-raise
       RAISE;
END TEST_GEN_GETCOL; 
/

