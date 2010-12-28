CREATE OR REPLACE PROCEDURE TEST_SAVE_SAVETOTARGETSCHEMA(RUN_ID VARCHAR2,TSCHEMA VARCHAR2) IS
tmpVar NUMBER;
tdm_data_id VARCHAR2(10);
xml_string VARCHAR2(4000);
xml_pkey VARCHAR2(1000);
ins_data XMLTYPE;
pkey_data XMLTYPE;
ins_data_update XMLTYPE;
ins_string VARCHAR2(100);
pkey_string VARCHAR2(100);
ctlg_itm_id VARCHAR2(10);
ctlg_id_t VARCHAR2(10);
cnt NUMBER;
err_msg VARCHAR2(1000);

/******************************************************************************
   NAME:       TEST_SAVE_SAVETOTARGETSCHEMA
   PURPOSE:    

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        2/10/2010          1. Created this procedure.

   NOTES:

   Automatically available Auto Replace Keywords:
      Object Name:     TEST_SAVE_SAVETOTARGETSCHEMA
      Sysdate:         2/10/2010
      Date and Time:   2/10/2010, 12:39:38 PM, and 2/10/2010 12:39:38 PM
      Username:         (set in TOAD Options, Procedure Editor)
      Table Name:       (set in the "New PL/SQL Object" dialog)

******************************************************************************/
BEGIN

   xml_string :='<ROW><CTLG_ITM_ID>1000001000</CTLG_ITM_ID><CTLG_ID>900</CTLG_ID><CTLG_ITM_STRT_DT>25-MAR-2009 00:00:00</CTLG_ITM_STRT_DT><CRT_DTTM>20-FEB-2009 21:27:33</CRT_DTTM><LST_UPDT_DTTM>29-JAN-2010 22:33:14</LST_UPDT_DTTM><CRT_USER_ID>MCM Import</CRT_USER_ID><LST_UPDT_USER_ID>MCM Import</LST_UPDT_USER_ID><CTLG_ITM_STYP_ID>15</CTLG_ITM_STYP_ID><CTLG_ITM_TYP_ID>3</CTLG_ITM_TYP_ID><BUS_ID>6839120120001</BUS_ID><MCM_STAT_CD>601</MCM_STAT_CD></ROW>';
   xml_pkey :='<ROWSET><ROW><COLUMN_NAME>CTLG_ITM_ID</COLUMN_NAME><POSITION>1</POSITION></ROW></ROWSET>';
   select xmltype(xml_string) into ins_data from dual;
   select xmltype(xml_pkey) into pkey_data from dual;
  
   ctlg_itm_id:='1000001000';
   ins_string :=' where CTLG_ITM_ID = '''||ctlg_itm_id||''''|| ' and to_number(ctlg_itm_styp_id) > 12 '; 
   pkey_string :=' and CTLG_ITM_ID = '''||ctlg_itm_id||'''';
   --dbms_output.put_line('INSERT INTO TDM_DATA values (1,' || ins_string ||','|| 'CTLG_ITM_T'||',' || xml_string ||','|| xml_pkey ||',' || pkey_string||','||'TEST'||','|| sysdate||','||sysdate||','||'TDM','TDM'||','||'NULL)');
  
    SELECT COUNT(1) into cnt FROM tdm_data WHERE data_id=1;
    IF cnt=1 then
         DELETE FROM TDM_DATA WHERE data_id=1;
    END IF;

   INSERT INTO TDM_DATA values (1,ins_string,'CTLG_ITM_T',ins_data,pkey_data,pkey_string,'TEST',sysdate,sysdate,'TDM','TDM','N','PRODUCT'); 
   TDM_USER.PKG_TESTDATA_SAVE.InsertDataFromXML('TEST',TSCHEMA,1);
   --(source_schema_name IN VARCHAR2,target_schema_name IN VARCHAR2,tdm_data_id IN VARCHAR2,seq_no IN VARCHAR2 default null)
   --TDM_USER.PKG_TESTDATA_SAVE.SAVETDMDATATOTARGETSCHEMA ('TEST',TSCHEMA,1); 
   cnt :=0;
   EXECUTE IMMEDIATE ' SELECT COUNT(1)  FROM '|| TSCHEMA ||'.CTLG_ITM_T WHERE CTLG_ITM_ID= '''|| ctlg_itm_id||'''' into cnt;
 
   IF cnt=1 THEN
       INSERT INTO TDM_UNIT_TEST VALUES(RUN_ID,'SAVE','SAVEDATATOTARGETSCHEMA','Insert XML ','Pass',sysdate);
   ELSE
       INSERT INTO TDM_UNIT_TEST VALUES(RUN_ID,'SAVE','SAVEDATATOTARGETSCHEMA','Insert XML','Failed',sysdate);
   END IF;
   
 
   IF cnt=1 THEN
       xml_string :='<ROW><CTLG_ITM_ID>1000001000</CTLG_ITM_ID><CTLG_ID>901</CTLG_ID><CTLG_ITM_STRT_DT>25-MAR-2009 00:00:00</CTLG_ITM_STRT_DT><CRT_DTTM>20-FEB-2009 21:27:33</CRT_DTTM><LST_UPDT_DTTM>29-JAN-2010 22:33:14</LST_UPDT_DTTM><CRT_USER_ID>MCM Import</CRT_USER_ID><LST_UPDT_USER_ID>MCM Import</LST_UPDT_USER_ID><CTLG_ITM_STYP_ID>15</CTLG_ITM_STYP_ID><CTLG_ITM_TYP_ID>3</CTLG_ITM_TYP_ID><BUS_ID>6839120120001</BUS_ID><MCM_STAT_CD>601</MCM_STAT_CD></ROW>';
       DELETE FROM TDM_DATA WHERE data_id='1';
       select xmltype(xml_string) into ins_data_update from dual;
       --UPDATE TDM_DATA SET INSRT_DATA=ins_data_update WHERE DATA_ID='1';
       INSERT INTO TDM_DATA values (1,ins_string,'CTLG_ITM_T',ins_data_update,pkey_data,pkey_string,'TEST',sysdate,sysdate,'TDM','TDM','N','PRODUCT');
       commit;
       TDM_USER.PKG_TESTDATA_SAVE.InsertDataFromXML('TEST',TSCHEMA,'1');

       --TDM_USER.PKG_TESTDATA_SAVE.SAVETDMDATATOTARGETSCHEMA ('TEST',TSCHEMA,1);
      EXECUTE IMMEDIATE ' SELECT CTLG_ID  FROM '||TSCHEMA||'.CTLG_ITM_T WHERE CTLG_ITM_ID= '''|| ctlg_itm_id||'''' into ctlg_id_t;
      INSERT INTO TDM_UNIT_TEST VALUES(RUN_ID,'SAVE','SAVEDATATOTARGETSCHEMA',ctlg_id_t,'Pass',sysdate);
     -- SELECT CTLG_ID into ctlg_id_t FROM CTLG_ITM_T WHERE CTLG_ITM_ID=ctlg_itm_id;
       dbms_output.put_line('ctlg_id_t=' || ctlg_id_t);
       IF ctlg_id_t='901' THEN
        INSERT INTO TDM_UNIT_TEST VALUES(RUN_ID,'SAVE','SAVEDATATOTARGETSCHEMA','Update XML ','Pass',sysdate);
       ELSE
        INSERT INTO TDM_UNIT_TEST VALUES(RUN_ID,'SAVE','SAVEDATATOTARGETSCHEMA','Update XML','Failed',sysdate);
       END IF;
   END IF;
       
   /* clean up process */
      cnt :=0;
       SELECT COUNT(1) into cnt FROM tdm_data WHERE data_id='1';
       IF cnt=1 then
         dbms_output.put_line('1');
         DELETE FROM TDM_DATA WHERE data_id='1';
       END IF;
       cnt :=0;
       
       EXECUTE IMMEDIATE ' SELECT COUNT(1)  FROM ' || TSCHEMA ||'.CTLG_ITM_T WHERE CTLG_ITM_ID= '''|| ctlg_itm_id||'''' into cnt;
      
       IF cnt=1 then
         EXECUTE IMMEDIATE ' DELETE FROM '|| TSCHEMA ||'.CTLG_ITM_T WHERE CTLG_ITM_ID= '''|| ctlg_itm_id||'''';
       END IF;
       
   /* End clean up process */
   
   exception when others then
     err_msg :='EXCEPTION =' ||SQLERRM; 
     INSERT INTO TDM_UNIT_TEST VALUES(RUN_ID,'SAVE','SAVEDATATOTARGETSCHEMA',err_msg,'Failed',sysdate);
   
  /* clean up process */
       cnt :=0;
       SELECT COUNT(1) into cnt FROM tdm_data WHERE data_id='1';
       IF cnt=1 then
       dbms_output.put_line('1');
         DELETE FROM TDM_DATA WHERE data_id='1';
       END IF;
       cnt :=0;
       dbms_output.put_line('1111' || ' SELECT COUNT(1)  FROM ' || TSCHEMA ||'.CTLG_ITM_T WHERE CTLG_ITM_ID= '''|| ctlg_itm_id||'''');
       INSERT INTO TDM_UNIT_TEST VALUES(RUN_ID,'SAVE','SAVEDATATOTARGETSCHEMA',' SELECT COUNT(1)  FROM ' || TSCHEMA ||'.CTLG_ITM_T WHERE CTLG_ITM_ID= '''|| ctlg_itm_id||'''','Failed',sysdate);
       EXECUTE IMMEDIATE ' SELECT COUNT(1)  FROM ' || TSCHEMA ||'.CTLG_ITM_T WHERE CTLG_ITM_ID= '''|| ctlg_itm_id||'''' into cnt;
       dbms_output.put_line('2222');
       IF cnt=1 then
        EXECUTE IMMEDIATE ' DELETE FROM '|| TSCHEMA ||'.CTLG_ITM_T WHERE data_id== '''|| ctlg_itm_id||'''';
       END IF;
    /* End clean up process */
     
END TEST_SAVE_SAVETOTARGETSCHEMA; 
/

