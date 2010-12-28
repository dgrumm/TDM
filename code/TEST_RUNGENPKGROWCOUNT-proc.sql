CREATE OR REPLACE PROCEDURE test_RunGenPkgRowCount IS
tmpVar NUMBER;
/******************************************************************************
   NAME:       test_RunGenPkgRowCount
   PURPOSE:    

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        12/17/2009          1. Created this procedure.

   NOTES:

   Automatically available Auto Replace Keywords:
      Object Name:     test_RunGenPkgRowCount
      Sysdate:         12/17/2009
      Date and Time:   12/17/2009, 4:33:48 PM, and 12/17/2009 4:33:48 PM
      Username:         (set in TOAD Options, Procedure Editor)
      Table Name:       (set in the "New PL/SQL Object" dialog)

******************************************************************************/
BEGIN
   tmpVar := 0;
   
   select count(*) into tmpVar from tdm_user.tdm_data;
   
   EXCEPTION
     WHEN NO_DATA_FOUND THEN
       NULL;
     WHEN OTHERS THEN
       -- Consider logging the error and then re-raise
       INSERT INTO tdm_user.TDM_ERR VALUES (TDM_ERR_S.nextval,'test_RunGenPkgRowCount','Get row count failed.',sysdate,sysdate,'TDM','TDM');
       RAISE;
END test_RunGenPkgRowCount; 
/

