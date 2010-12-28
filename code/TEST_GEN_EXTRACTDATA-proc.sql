CREATE OR REPLACE PROCEDURE TEST_GEN_EXTRACTDATA(RUN_ID VARCHAR2,sname IN varchar2, tab_name IN VARCHAR2, where_clause IN VARCHAR2, ename IN VARCHAR2) IS
tmpVar NUMBER;
tdm_cnt NUMBER;
tdm_key_cnt NUMBER;
tdm_rfrntl_cnt NUMBER;
/******************************************************************************
   NAME:       TEST_GEN_EXTRACTDATA
   PURPOSE:    

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        2/18/2010          1. Created this procedure.

   NOTES:

   Automatically available Auto Replace Keywords:
      Object Name:     TEST_GEN_EXTRACTDATA
      Sysdate:         2/18/2010
      Date and Time:   2/18/2010, 3:05:17 PM, and 2/18/2010 3:05:17 PM
      Username:         (set in TOAD Options, Procedure Editor)
      Table Name:       (set in the "New PL/SQL Object" dialog)

******************************************************************************/
BEGIN
   tmpVar := 0;
   BEGIN
    /* Tmp Table Creation */
     
     /* No Privelages 
     execute immediate 'CREATE TABLE TMP_PARENT(PARENT_ID NUMBER, BUS_ID NUMBER, COMMENTS VARCHAR2(100),CONSTRAINT parent_pk PRIMARY KEY(PARENT_ID))';
     execute immediate 'CREATE TABLE TMP_GEN(GEN_ID NUMBER ,PARENT_ID NUMBER,CTLG_NUM NUMBER,COMMENTS VARCHAR2(100),CONSTRAINT gen_pk PRIMARY KEY(GEN_ID),CONSTRAINT parent_fk FOREIGN KEY(PARENT_ID) REFERENCES TMP_PARENT(PARENT_ID))';
     execute immediate 'CREATE TABLE TMP_CHILD(CHILD_ID NUMBER,GEN_ID NUMBER,COMMENTS VARCHAR2(100),CONSTRAINT child_pk PRIMARY KEY(CHILD_ID),cONSTRAINT gen_fk  FOREIGN KEY(GEN_ID) REFERENCES TMP_GEN(GEN_ID))';
     commit;    
     
     */
    /*End of tmp table creation */
      
     BEGIN
         DELETE FROM TDM_DATA WHERE SCHEMA_NAME=sname;
         DELETE FROM TDM_KEYS WHERE SCHEMA_NAME=sname;
         DELETE FROM TDM_RFRNTL_KEYS WHERE SCHEMA_NAME=sname;
         DELETE FROM TMP_CHILD;
         DELETE FROM TMP_GEN;
         DELETE FROM TMP_PARENT;
     EXCEPTION WHEN NO_DATA_FOUND THEN
        NULL;
     END;
       
    insert into TMP_PARENT VALUES(1,12,'Parent Record');
    insert into TMP_GEN VALUES(11,1,4,'Gen Record');
    insert into TMP_CHILD VALUES(21,11,'Child Record');
    TDM_USER.PKG_TESTDATA_GEN.extractData(sname,tab_name, where_clause,ename);
    
    SELECT COUNT(1) into tdm_cnt FROM TDM_DATA  WHERE SCHEMA_NAME=sname;
    SELECT COUNT(1) into tdm_key_cnt  FROM TDM_KEYS WHERE SCHEMA_NAME=sname;
    SELECT COUNT(1) into tdm_rfrntl_cnt FROM TDM_RFRNTL_KEYS  WHERE SCHEMA_NAME=sname;
   
    IF tdm_cnt=3 and tdm_key_cnt=3 AND tdm_rfrntl_cnt=2 THEN
        INSERT INTO TDM_UNIT_TEST VALUES(RUN_ID,'GEN','extractData',null,'Pass',sysdate);
    ELSE
        INSERT INTO TDM_UNIT_TEST VALUES(RUN_ID,'GEN','extractData',null,'Fail',sysdate);
    END IF;
    
   EXCEPTION WHEN OTHERS THEN
     dbms_output.put_line('Exception occured' ||sqlerrm );
     INSERT INTO TDM_UNIT_TEST VALUES(RUN_ID,'GEN','extractData','Exception-Block','Fail',sysdate);
     --INSERT INTO TDM_UNIT_TEST VALUES(RUN_ID,'GEN','extractData',null,'Fail',sysdate);  
   END;
  
   EXCEPTION
     WHEN NO_DATA_FOUND THEN
       NULL;
     WHEN OTHERS THEN
       -- Consider logging the error and then re-raise
       RAISE;
END TEST_GEN_EXTRACTDATA;
/
