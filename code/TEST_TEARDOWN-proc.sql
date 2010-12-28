CREATE OR REPLACE PROCEDURE test_teardown IS
tmpVar NUMBER;
/******************************************************************************
   NAME:       test_teardown
   PURPOSE:    

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        12/17/2009          1. Created this procedure.

   NOTES:

   Automatically available Auto Replace Keywords:
      Object Name:     test_teardown
      Sysdate:         12/17/2009
      Date and Time:   12/17/2009, 4:23:00 PM, and 12/17/2009 4:23:00 PM
      Username:         (set in TOAD Options, Procedure Editor)
      Table Name:       (set in the "New PL/SQL Object" dialog)

******************************************************************************/
BEGIN
   tmpVar := 0;
   EXCEPTION
     WHEN NO_DATA_FOUND THEN
       NULL;
     WHEN OTHERS THEN
       -- Consider logging the error and then re-raise
       RAISE;
END test_teardown; 
/

