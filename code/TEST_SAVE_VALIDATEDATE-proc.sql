CREATE OR REPLACE PROCEDURE TEST_SAVE_VALIDATEDATE(RUN_ID VARCHAR2) IS
tmpVar NUMBER;
retVal VARCHAR2(10);
/******************************************************************************
   NAME:       TEST_VALIDATEDATE
   PURPOSE:    

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        2/9/2010          1. Created this procedure.

   NOTES:

   Automatically available Auto Replace Keywords:
      Object Name:     TEST_VALIDATEDATE
      Sysdate:         2/9/2010
      Date and Time:   2/9/2010, 10:07:47 AM, and 2/9/2010 10:07:47 AM
      Username:         (set in TOAD Options, Procedure Editor)
      Table Name:       (set in the "New PL/SQL Object" dialog)

******************************************************************************/
BEGIN
   tmpVar := 0;
   retVal := TDM_USER.PKG_TESTDATA_SAVE.VALIDATEDATE ( '11-OCT-2005 00:00:00' );
   IF retVal='YES' THEN
    INSERT INTO TDM_UNIT_TEST VALUES(RUN_ID,'SAVE','ValidateDate','Postive date Test','Pass',sysdate);
   ELSE
    INSERT INTO TDM_UNIT_TEST VALUES(RUN_ID,'SAVE','ValidateDate','Postive date Test','Failed',sysdate);
   END IF;
   
   retVal := TDM_USER.PKG_TESTDATA_SAVE.VALIDATEDATE ( 'HELLO' );
   
   IF retVal='NO' THEN
    INSERT INTO TDM_UNIT_TEST VALUES(RUN_ID,'SAVE','ValidateDate','Negative date Test with wrong value','Pass',sysdate);
   ELSE
    INSERT INTO TDM_UNIT_TEST VALUES(RUN_ID,'SAVE','ValidateDate','Negative date Test with wrong value','Failed',sysdate);
   END IF;
   
   
   EXCEPTION
     WHEN NO_DATA_FOUND THEN
       NULL;
     WHEN OTHERS THEN
     dbms_output.put_line('exception');
       -- Consider logging the error and then re-raise
       RAISE;
END TEST_SAVE_VALIDATEDATE; 
/

