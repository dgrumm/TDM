CREATE OR REPLACE PROCEDURE TEST_SAVE_ENABLEDISABLECONS(SNAME VARCHAR2,RUN_ID VARCHAR2) IS
tmpVar NUMBER;
Status VARCHAR2(10);
enabledCnt NUMBER;
disabledCnt NUMBER;
cons_type VARCHAR2(2);
/******************************************************************************
   NAME:       TEST_SAVE_ENABLEDISABLECONS
   PURPOSE:    

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        2/9/2010          1. Created this procedure.

   NOTES:

   Automatically available Auto Replace Keywords:
      Object Name:     TEST_SAVE_ENABLEDISABLECONS
      Sysdate:         2/9/2010
      Date and Time:   2/9/2010, 5:45:14 PM, and 2/9/2010 5:45:14 PM
      Username:         (set in TOAD Options, Procedure Editor)
      Table Name:       (set in the "New PL/SQL Object" dialog)

******************************************************************************/
BEGIN
   tmpVar := 0;
   Status :='ENABLED';
   cons_type :='R';
   EXECUTE IMMEDIATE 'SELECT COUNT(STATUS) FROM ALL_CONSTRAINTS WHERE STATUS= ''' ||Status ||''''||' AND OWNER= ''' ||SNAME||'''' || ' and constraint_type='''||cons_type||'''' into enabledCnt;
   TDM_USER.PKG_TESTDATA_SAVE.enableDisableConstraints('NO',SNAME,cons_type);
   Status :='DISABLED';
   EXECUTE IMMEDIATE 'SELECT COUNT(STATUS) FROM ALL_CONSTRAINTS WHERE STATUS= ''' ||Status ||''''||' AND OWNER= ''' ||SNAME||'''' || ' and constraint_type='''||cons_type||'''' into disabledCnt;
    dbms_output.put_line( 'enabledCnt= '|| enabledCnt  || ' disabledCnt= '||disabledCnt);  
    
   IF enabledCnt=disabledCnt THEN
           INSERT INTO TDM_UNIT_TEST VALUES(RUN_ID,'SAVE','enableDisableConstraints','disableCons','Pass',sysdate);
           ELSE
           INSERT INTO TDM_UNIT_TEST VALUES(RUN_ID,'SAVE','enableDisableConstraints','disableCons','Failed',sysdate);
   END IF; 
   
   
   Status :='ENABLED';
   enabledCnt :=0;
   TDM_USER.PKG_TESTDATA_SAVE.enableDisableConstraints('YES',SNAME,cons_type);
   EXECUTE IMMEDIATE 'SELECT COUNT(STATUS) FROM ALL_CONSTRAINTS WHERE STATUS= ''' ||Status ||''''||' AND OWNER= ''' ||SNAME||'''' || ' and constraint_type='''||cons_type||'''' into enabledCnt;
   
 dbms_output.put_line( 'enabledCnt= '|| enabledCnt  || ' disabledCnt= '||disabledCnt);    
 
 IF enabledCnt=disabledCnt THEN
       INSERT INTO TDM_UNIT_TEST VALUES(RUN_ID,'SAVE','enableDisableConstraints','enableCons','Pass',sysdate);
       ELSE
       INSERT INTO TDM_UNIT_TEST VALUES(RUN_ID,'SAVE','enableDisableConstraints','enableCons','Failed',sysdate);
   END IF;
   
   EXCEPTION
     WHEN NO_DATA_FOUND THEN
       NULL;
     WHEN OTHERS THEN
       -- Consider logging the error and then re-raise
       RAISE;
END TEST_SAVE_ENABLEDISABLECONS; 
/

