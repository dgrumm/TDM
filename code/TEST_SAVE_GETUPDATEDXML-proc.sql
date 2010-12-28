CREATE OR REPLACE PROCEDURE TEST_SAVE_GETUPDATEDXML(SNAME VARCHAR2,RUN_ID VARCHAR2) IS
tmpVar NUMBER;
xmlString varchar2(4000);
update_XML XMLTYPE;
returned_XML XMLTYPE;
date_format VARCHAR2(50);
test_value VARCHAR2(100);
/******************************************************************************
   NAME:       TEST_SAVE_GETUPDATEDXML
   PURPOSE:    

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        2/10/2010          1. Created this procedure.

   NOTES:

   Automatically available Auto Replace Keywords:
      Object Name:     TEST_SAVE_GETUPDATEDXML
      Sysdate:         2/10/2010
      Date and Time:   2/10/2010, 10:53:30 AM, and 2/10/2010 10:53:30 AM
      Username:         (set in TOAD Options, Procedure Editor)
      Table Name:       (set in the "New PL/SQL Object" dialog)

******************************************************************************/
BEGIN
   tmpVar := 0;
   xmlString :='<ROW><CTLG_ITM_ID>2202861</CTLG_ITM_ID><CTLG_ITM_NM>Chaturanga&amp;#153 Yoga Short</CTLG_ITM_NM><CTLG_ID>900</CTLG_ID><CTLG_ITM_STRT_DT>20-MAR-2009 00:00:00</CTLG_ITM_STRT_DT><CTLG_ITM_END_DT>01-JAN-2400 00:00:00</CTLG_ITM_END_DT><CRT_DTTM>19-FEB-2009 21:41:00</CRT_DTTM><LST_UPDT_DTTM>29-JAN-2010 22:33:13</LST_UPDT_DTTM><CRT_USER_ID>MCM Import</CRT_USER_ID><LST_UPDT_USER_ID>MCM Import</LST_UPDT_USER_ID><CTLG_ITM_STYP_ID>13</CTLG_ITM_STYP_ID><CTLG_ITM_TYP_ID>3</CTLG_ITM_TYP_ID><BUS_ID>683757</BUS_ID><ORIG_CTLG_ITM_ID>2202861</ORIG_CTLG_ITM_ID><MCM_STAT_CD>601</MCM_STAT_CD><MDSE_HIER_SCLS_ID>1101020900</MDSE_HIER_SCLS_ID><APV_DT>20-MAR-2009 00:00:00</APV_DT></ROW>';
   select xmltype(xmlString) into update_XML from dual;
   returned_XML:=TDM_USER.PKG_TESTDATA_SAVE.GETUPDATEDXMLKEYS(update_XML,'CTLG_ITM_T','01-FEB-10',SNAME );
   select extractValue(returned_XML,'/ROW/CRT_USER_ID') into test_value FROM DUAL;
   IF test_value IS NOT NULL AND test_value='TDM' THEN
       INSERT INTO TDM_UNIT_TEST VALUES(RUN_ID,'SAVE','getupdatedxml','CRT_USER_ID value','Pass',sysdate);
   ELSE
       INSERT INTO TDM_UNIT_TEST VALUES(RUN_ID,'SAVE','getupdatedxml','CRT_USER_ID value','Failed',sysdate);
   END IF;
   test_value :='';
   
   select extractValue(returned_XML,'/ROW/LST_UPDT_USER_ID') into test_value FROM DUAL;
   IF test_value IS NOT NULL AND test_value='TDM' THEN
       INSERT INTO TDM_UNIT_TEST VALUES(RUN_ID,'SAVE','getupdatedxml','LST_UPDT_USER_ID Value','Pass',sysdate);
   ELSE
       INSERT INTO TDM_UNIT_TEST VALUES(RUN_ID,'SAVE','getupdatedxml','LST_UPDT_USER_ID Value','Failed',sysdate);
   END IF;
   
   
   
  EXCEPTION
     WHEN NO_DATA_FOUND THEN
       NULL;
     WHEN OTHERS THEN
       -- Consider logging the error and then re-raise
       RAISE;
END TEST_SAVE_GETUPDATEDXML; 
/

