CREATE OR REPLACE PROCEDURE TEST_SAVE_GETPRIMARYCOLUMN(TNAME IN VARCHAR2,RUN_ID IN VARCHAR2) IS
tmpVar NUMBER;
retVal varchar2(100);
/******************************************************************************
   NAME:       TDM_SAVE_GETPRIMARYCOLUMN
   PURPOSE:    

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        2/9/2010          1. Created this procedure.

   NOTES:

   Automatically available Auto Replace Keywords:
      Object Name:     TDM_SAVE_GETPRIMARYCOLUMN
      Sysdate:         2/9/2010
      Date and Time:   2/9/2010, 10:40:25 AM, and 2/9/2010 10:40:25 AM
      Username:         (set in TOAD Options, Procedure Editor)
      Table Name:       (set in the "New PL/SQL Object" dialog)

******************************************************************************/
BEGIN
   tmpVar := 0;
   /* Pass in the source name and the table name to GETPRIMARYCOLUMN function in SAVE Package*/
    retVal := TDM_USER.PKG_TESTDATA_SAVE.GETPRIMARYCOLUMN(TNAME,'CTLG_ITM_T');
    /* The expected return value is CTLG_ITM_ID*/
   IF retVal='CTLG_ITM_ID' THEN
    INSERT INTO TDM_UNIT_TEST VALUES(RUN_ID,'SAVE','GETPRIMARYCOLUMN','PostiveTest','Pass',sysdate);
   ELSE
    INSERT INTO TDM_UNIT_TEST VALUES(RUN_ID,'SAVE','GETPRIMARYCOLUMN','PostiveTest','Failed',sysdate);
   END IF;
      
   EXCEPTION
     WHEN NO_DATA_FOUND THEN
       NULL;
     WHEN OTHERS THEN
       -- Consider logging the error and then re-raise
       RAISE;
   
END TEST_SAVE_GETPRIMARYCOLUMN; 
/

