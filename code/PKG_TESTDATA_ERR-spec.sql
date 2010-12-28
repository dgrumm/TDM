CREATE OR REPLACE PACKAGE PKG_TESTDATA_ERR AS
/******************************************************************************
   NAME:       PKG_TESTDATA_ERR
   PURPOSE:

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        3/22/2010             1. Created this package.
******************************************************************************/

  FUNCTION getErrSeqNo RETURN NUMBER;
  PROCEDURE insertError(errSeqNo IN NUMBER, errMsg IN VARCHAR2, proc_name IN VARCHAR2);

END PKG_TESTDATA_ERR; 
/

