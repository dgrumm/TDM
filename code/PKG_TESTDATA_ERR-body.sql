CREATE OR REPLACE PACKAGE BODY PKG_TESTDATA_ERR AS
/******************************************************************************
   NAME:       PKG_TESTDATA_ERR
   PURPOSE:

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        3/22/2010             1. Created this package.
******************************************************************************/

  FUNCTION getErrSeqNo RETURN NUMBER IS
  seq_no number;
  BEGIN
    select TDM_ERR_S.nextval into seq_no from DUAL;
    return seq_no; 
  END;
  
  PROCEDURE insertError(errSeqNo IN NUMBER, errMsg IN VARCHAR2, proc_name IN VARCHAR2) IS
  BEGIN
    INSERT INTO TDM_ERR VALUES (errSeqNo,proc_name,errMsg,sysdate,sysdate,'TDM','TDM');
  END;  

END PKG_TESTDATA_ERR; 
/

