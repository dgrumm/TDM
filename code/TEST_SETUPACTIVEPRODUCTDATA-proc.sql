CREATE OR REPLACE PROCEDURE TEST_SETUPACTIVEPRODUCTDATA IS
tmpVar NUMBER;
/******************************************************************************
   NAME:       test_setupActiveProductData
   PURPOSE:    

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        12/17/2009          1. Created this procedure.

   NOTES:

   Automatically available Auto Replace Keywords:
      Object Name:     test_setupActiveProductData
      Sysdate:         12/17/2009
      Date and Time:   12/17/2009, 4:20:59 PM, and 12/17/2009 4:20:59 PM
      Username:         (set in TOAD Options, Procedure Editor)
      Table Name:      TDM_DATA (set in the "New PL/SQL Object" dialog)

******************************************************************************/
BEGIN
   tmpVar := 0;
   EXCEPTION
     WHEN NO_DATA_FOUND THEN
       NULL;
     WHEN OTHERS THEN
       -- Consider logging the error and then re-raise
       RAISE;
END test_setupActiveProductData;
/


