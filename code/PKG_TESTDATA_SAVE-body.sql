CREATE OR REPLACE PACKAGE BODY TDM_USER.pkg_testdata_save
AS
/******************************************************************************
   NAME:       PKG_TESTDATA_SAVE
   PURPOSE:

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        11/17/2009     Elango       1. Created this package body.
******************************************************************************/
   PROCEDURE SaveTDMDataToTargetSchema(source_schema_name IN VARCHAR2,target_schema_name IN VARCHAR2,create_new_keys_flg IN VARCHAR2, tdm_data_id IN VARCHAR2 default null)
   IS
   run_status VARCHAR2(2);
   err_msg VARCHAR2(600);
   seq_no VARCHAR2(100);
   col_name VARCHAR2(20);
   col_val VARCHAR2(20);
   date_format varchar2(50);
   pkey_w_cls VARCHAR2(4000);
   pkey_sel_cls VARCHAR2(4000);
   qry VARCHAR2(4000);
   pkey_value VARCHAR2(100);
   cnt NUMBER;
   row_cnt NUMBER;
   new_col_value VARCHAR2(60);
   tmp_cons VARCHAR2(1000);
   tmp_cons_type VARCHAR2(10);
   tmp_status VARCHAR2(20);
   err_cnt NUMBER;
   rec_count NUMBER;
   cursor C1 is select err_msg from tdm_err where run_id = save_sequence;

   BEGIN
    rec_count := 0;
     /* Get the Sequence number From TDM_ERR_S */

     select TDM_ERR_S.nextval into seq_no from DUAL;
     save_sequence := seq_no;

     /* a) Look @ the TDM_STATUS table to check if the Target Schema Status already exists .
        b) If TargetSchema does not exist insert a entry with status as 'Y'.
     */

     SELECT COUNT(*) into cnt FROM TDM_STATUS WHERE SCHEMA_NAME=target_schema_name;
     IF cnt=0 THEN
        INSERT INTO TDM_STATUS VALUES (target_schema_name,'Y',null);
     END IF;

     SELECT STATUS INTO  run_status FROM TDM_STATUS WHERE SCHEMA_NAME=target_schema_name;

     /* a) IF the status 'Y' the save process is good to get started. Else an error message will be entered in TDM_ERR table*/
     BEGIN
     IF run_status='Y' THEN

         updateRunStatus('N',target_schema_name);
        /*Populate AlternateKeys in TDM_DATA*/
         update tdm_status set status = 'Y' where schema_name = source_schema_name;
          IF source_schema_name IS NOT NULL THEN
             --dbms_output.put_line('seq_no' || seq_no);
             --dbms_output.put_line('PopulateAlternateKeys Start Time - '|| systimestamp);
             PopulateAlternateKeys(source_schema_name,target_schema_name,'Y',seq_no);
             --dbms_output.put_line('PopulateAlternateKeys End Time - '|| systimestamp);
          END IF;
         --commit;
         SELECT COUNT(1) into err_cnt FROM TDM_ERR WHERE RUN_ID=seq_no;
         --dbms_output.put_line('PAK Error Count' ||err_cnt);
         
         if err_cnt != 0 then
           --dbms_output.put_line('raise tdm_exception');
           raise TDM_EXCEPTION;
         else
           commit;
         end if;

        /*Create New Keys in TDM_DATA*/
        if create_new_keys_flg = 'Y' then

          IF source_schema_name IS NOT NULL THEN
             --dbms_output.put_line('seq_no' || seq_no);
             --dbms_output.put_line('PopulateNewColValues Start Time - '|| systimestamp);
             PopulateNewColValues(source_schema_name,target_schema_name,seq_no);
             --dbms_output.put_line('PopulateNewColValues End Time - '|| systimestamp);
          END IF;

          SELECT COUNT(1) into err_cnt FROM TDM_ERR WHERE RUN_ID=seq_no;
          --dbms_output.put_line('PNC Error Count' ||err_cnt);
          if err_cnt != 0 then
           --dbms_output.put_line('raise tdm_exception');
           raise TDM_EXCEPTION;
          else
           commit;
          end if;
        end if;
        /* delete the Children Rows when DATA_FLAG is 'Y' in TDM_DATA */


       --dbms_output.put_line('DelChildrenRows Start Time - '||rec_count ||'-' ||systimestamp);

        /* FOR REC IN (SELECT DATA_ID,'WHERE' || SUBSTR(PKEY_STRING,5) pkey_string,TNAME,PKEY_DATA,INSRT_DATA
                     FROM TDM_DATA
                     WHERE schema_name=source_schema_name
                     and ((DATA_FLAG='Y')
                     OR ((TNAME,entity_name) IN (SELECT TNAME,entity_name FROM TDM_DELETE_TABLES)))
                     and TNAME not in (select mview_name from all_mviews where owner = target_schema_name))
          LOOP
             pkey_w_cls :=NULL;
             pkey_sel_cls := NULL;
             FOR myrec in (SELECT extractvalue(column_value,'/COLUMN_NAME') col
                            from XMLTable ('//ROWSET/ROW/COLUMN_NAME' PASSING REC.pkey_data))
              loop
                 --begin
                     new_col_value :=NULL;
                     select extractValue(REC.INSRT_DATA,'/ROW/'||myrec.col) into pkey_value FROM DUAL;
                     IF pkey_sel_cls IS NULL THEN
                        pkey_sel_cls := myrec.col;
                     ELSE
                        pkey_sel_cls := pkey_sel_cls || ',' || myrec.col;
                     END IF;
                     --qry := 'SELECT COUNT(COL_NEW_VALUE) FROM TDM_KEYS WHERE COL_NAME='''|| myrec.col||''''|| ' AND  COL_VALUE='''|| pkey_value||'''' || ' AND SCHEMA_NAME='''||source_schema_name||'''' || ' AND TNAME='''||REC.TNAME||'''';
                     new_col_value := getResultsFromKeyTables(REC.TNAME,myrec.col,pkey_value,source_schema_name);
                     --EXECUTE IMMEDIATE 'SELECT COL_NEW_VALUE FROM TDM_KEYS WHERE COL_NAME='''|| myrec.col||''''|| ' AND  COL_VALUE='''|| pkey_value||'''' || ' AND SCHEMA_NAME='''||source_schema_name||'''' || ' AND TNAME='''||REC.TNAME||'''' INTO new_col_value;
                     --dbms_output.put_line('QRY' ||qry);
                    --EXECUTE IMMEDIATE qry into  row_cnt;
                         --IF row_cnt >0 THEN
                         --EXECUTE IMMEDIATE 'SELECT COL_NEW_VALUE FROM TDM_KEYS WHERE COL_NAME='''|| myrec.col||''''|| ' AND  COL_VALUE='''|| pkey_value||'''' || ' AND SCHEMA_NAME='''||source_schema_name||'''' || ' AND TNAME='''||REC.TNAME||'''' INTO new_col_value;
                        IF new_col_value IS NOT NULL THEN
                            pkey_value :=new_col_value;
                        END IF;

                        IF pkey_w_cls IS NULL THEN
                            pkey_w_cls :=' WHERE ' || myrec.col||'='''||pkey_value||'''';
                        ELSE
                            IF pkey_value IS NOT NULL THEN
                                pkey_w_cls :=  pkey_w_cls ||  ' AND '  || myrec.col||'='''||pkey_value||'''';
                            ELSE
                                pkey_w_cls :=  pkey_w_cls ||  ' AND '  || myrec.col|| ' IS NULL ';
                            END IF;
                        END IF;
                          --ELSE
                  --EXCEPTION WHEN NO_DATA_FOUND THEN
                    --       pkey_w_cls := REC.pkey_string;
                        --END IF;
                  --end;
               end loop;
               --dbms_output.put_line('Data id ' ||rec.data_id);
               --dbms_output.put_line('pkey_w_cls ' ||pkey_w_cls);
               --dbms_output.put_line('pkey_sel_cls' || pkey_sel_cls);
               rec_count := rec_count + 1;
              --dbms_output.put_line('DelChildrenRows Start Time - '||rec_count ||'-' ||systimestamp);
              delChildrenRows(target_schema_name,REC.tname,pkey_w_cls,pkey_sel_cls,'Y','0',seq_no);
              --exit;
              --dbms_output.put_line('DelChildrenRows End Time - '|| rec_count || '-' ||systimestamp);
          END LOOP;
        dbms_output.put_line('DelChildrenRows End Time - '|| rec_count || '-' ||systimestamp);

         SELECT COUNT(1) into err_cnt FROM TDM_ERR WHERE RUN_ID=seq_no;
         /*if err_cnt != 0 then
           raise TDM_EXCEPTION;
         end if;*/
       -- dbms_output.put_line('DCR Error Count' ||err_cnt);

      /* Change the DateFormat*/
           begin
             SELECT value
             into date_format
             FROM v$nls_parameters WHERE parameter ='NLS_DATE_FORMAT';
             execute immediate 'alter session set nls_date_format=''DD-MON-YYYY HH24:MI:SS''';
             execute IMMEDIATE 'ALTER SESSION SET CONSTRAINTS = IMMEDIATE';
             --execute IMMEDIATE 'SET CONSTRAINTS ALL IMMEDIATE';
             --SET CONSTRAINTS IMMEDIATE BRCATA.PK_PDVT_PDVI;
            /*     Make the TDM_STATUS to 'N' b) disable the constraints c) disable the triggers   */
              --dbms_output.put_line('before');

           tmp_cons_type :='R';
            tmp_status :='DISABLED';
            execute immediate 'delete from tmp_disabled_constraints where schema_name='''||target_schema_name||'''';
            tmp_cons := 'insert into tmp_disabled_constraints  (select constraint_name,table_name,owner from all_constraints where owner='''||target_schema_name||''''|| ' and constraint_type='''||tmp_cons_type||'''' ||' and status='''||tmp_status||''''||')';
            execute immediate tmp_cons;
             err_cnt :=0;

             --enableDisableConstraints('NO',target_schema_name,'R',seq_no);
             --enableDisableTriggers('NO',target_schema_name,seq_no);
             --dbms_output.put_line('after');
            /*  Save Method */
             --dbms_output.put_line('save method called');


            BEGIN
               --execute immediate ('alter session set constraints = immediate');
               --dbms_output.put_line('InsertDataFromXML Start Time - '|| systimestamp);
               InsertDataFromXML(source_schema_name,target_schema_name,tdm_data_id,seq_no);
               --dbms_output.put_line('InsertDataFromXML End Time - '|| systimestamp);
            EXCEPTION WHEN OTHERS THEN
                  err_msg :=' Exception= Insert-O'||SQLERRM;
                  PKG_TESTDATA_ERR.insertError(seq_no,err_msg,'SaveTDMDataToTargetSchema');
                 --INSERT INTO TDM_ERR VALUES (seq_no,'SaveTDMDataToTargetSchema',err_msg,sysdate,sysdate,'TDM','TDM');
                 --DBMS_OUTPUT.put_line ('SaveTDMDataToTargetSchema1 ' ||SQLCODE ||' And Error Message'||SQLERRM);
            END;
            --dbms_output.put_line('InsertDataFromXML End Time - '|| systimestamp);
           exception
           when OTHERS then
           err_msg := ' Exception= Insert-OO'||SQLERRM;
           PKG_TESTDATA_ERR.insertError(seq_no,err_msg,'SaveTDMDataToTargetSchema');
           --DBMS_OUTPUT.put_line ('Caught here in TDMEXCEPTION1 ' ||SQLCODE ||' And Error Message'||SQLERRM);
           end;
        /*     a) Make the TDM_STATUS to 'Y' b) enable the constraints c) enable the triggers   */

            --updateRunStatus('Y',target_schema_name);
             --enableDisableConstraints('YES',target_schema_name,'R',seq_no);
             --enableDisableTriggers('YES',target_schema_name,seq_no);

         /*Revert the DateFormat to the Orginal*/

        execute immediate 'alter session set nls_date_format='''||date_format||'''';
        execute IMMEDIATE 'ALTER SESSION SET CONSTRAINTS = DEFAULT';
        SELECT COUNT(1) into err_cnt FROM TDM_ERR WHERE RUN_ID=seq_no;
        --DBMS_OUTPUT.put_line ('Insert Error ' ||err_cnt);
        if err_cnt != 0 then
           raise TDM_EXCEPTION;
        /*else
            execute immediate 'Delete from' || target_schema_name || '.cmpl_prc_mv where ctlg_itm_id in
            (select distinct(ctlg_itm_id) from '||target_schema_name|| '.cmpl_prc_mv where lst_updt_user_id = ''TDM'')
            and lst_updt_user_id <> ''TDM''';

            execute immediate 'Delete from' || target_schema_name || '.inv_stat_rollup_t where ctlg_itm_id in
            (select distinct(ctlg_itm_id) from '||target_schema_name|| '.inv_stat_rollup_t where lst_updt_user_id = ''TDM'')
            and lst_updt_user_id <> ''TDM''';*/
        end if;
        
     ELSE
        err_msg :='TDM Instance already Running.';
        --INSERT INTO TDM_ERR VALUES (seq_no,'SaveTDMDataToTargetSchema',substr(err_msg,1,500),sysdate,sysdate,'TDM','TDM');
        PKG_TESTDATA_ERR.insertError(seq_no,err_msg,'SaveTDMDataToTargetSchema');
        --DBMS_OUTPUT.put_line ('SaveTDMDataToTargetSchema ' ||err_msg ||' And Error Message'||SQLERRM);
     END IF;
     exception
           when TDM_EXCEPTION then
           --DBMS_OUTPUT.put_line ('Caught here in TDMEXCEPTION2 ' ||SQLCODE ||' And Error Message'||SQLERRM);
           updateRunStatus('Y',target_schema_name);
             --null;
     END;
    --commit;


      SELECT COUNT(1) into err_cnt FROM TDM_ERR WHERE RUN_ID=seq_no;
      DBMS_OUTPUT.put_line (target_schema_name || ' - Total Error Count ' ||err_cnt);
      
     if err_cnt != 0 then
        rollback;
        err_msg := 'Unsuccessful Save for Source Schema-' ||source_schema_name || ' and Target Schema' || target_schema_name;
        updateJobStatus('Failed',target_schema_name);
        commit;
        PKG_TESTDATA_ERR.insertError(seq_no,err_msg,'SaveTDMDataToTargetSchema');
        --raise_application_error(-20001,'Problem Encountered During Save. Please see tdm_error table for run_id '||to_char(seq_no)||'.');
        /*for rec in C1
        loop
            dbms_output.put_line(rec.err_msg);
        end loop;*/
        raise_application_error(-20001,target_schema_name || ': Problem Encountered During Save. Please see tdm_error table for run_id '||to_char(seq_no)||'.');
     else
        updateJobStatus('Success',target_schema_name);
        DBMS_OUTPUT.put_line (target_schema_name || ' - Save Completed Successfully ');
        Commit;
     end if;

   END;

   PROCEDURE InsertDataFromXML(source_schema_name IN VARCHAR2,target_schema_name IN VARCHAR2,tdm_data_id IN VARCHAR2,seq_no IN VARCHAR2 default null)
   IS
      insctx     dbms_xmlstore.ctxtype;
      updttx    dbms_xmlstore.ctxtype;
      ROWS       NUMBER;
      err_num    NUMBER;
      prim_key   VARCHAR2(200);
      --updated_xml VARCHAR2(2000);
      counter NUMBER;
      incr NUMBER;
      pk_val VARCHAR2(100);
       pk_val_valid VARCHAR2(100);
      date_format varchar2(50);
      updated_xml XMLTYPE;
      tmp_cons VARCHAR2(1000);
      del_qry_where VARCHAR2(1000);
      --seq_no VARCHAR2(100);
      err_msg VARCHAR2(600);
      OTHER_EXCEPTIONS EXCEPTION;
      TABLE_NOT_FOUND EXCEPTION;
      rec_count_insert NUMBER;
      rec_count_update NUMBER;


      /* Cursor to get the TDM_DATA information */

      CURSOR insrt_data
      IS
         SELECT tdm.data_id, tdm.tname,
                tdm.insrt_data
                insdata, pkey_data,crt_dttm
          FROM tdm_data tdm,
            (select c_owner, c_tname, max(level) lvl
             from (select tbl.table_name tname, chld.owner c_owner, chld.table_name c_tname, prent.owner p_owner, prent.table_name p_tname
                   from all_constraints prent, all_constraints chld, all_tables tbl
                   where tbl.owner = target_schema_name
                   and tbl.table_name = chld.table_name
                   and tbl.owner = chld.owner
                   and chld.constraint_type  = 'R'
                   and chld.r_owner = prent.owner
                   and chld.r_constraint_name = prent.constraint_name)
              start with (p_owner, p_tname) not in (select owner, table_name from all_constraints t where t.constraint_type = 'R')
              connect by nocycle prior c_tname = p_tname
              and        prior c_owner = p_owner
              group by c_owner, c_tname) ordered_tnames
          WHERE SCHEMA_NAME=source_schema_name
          and tname not in (select mview_name from all_mviews where owner = target_schema_name)
          and data_id=nvl(tdm_data_id,data_id)
          and c_owner (+) = target_schema_name
          and TDM.TNAME = ordered_tnames.c_tname (+)
          ORDER BY lvl asc nulls first , data_id asc ; --where data_id='654073'; --where crt_dttm >  sysdate - 1;-- WHERE data_id>='644251';-- WHERE CRT_DTTM LIKE SYSDATE-3; --where data_id='647395';-- row num<1000;--DATA_ID='616251';--rownum<10000;-- where DATA_ID='616290';-- rownum<1000;--DATA_ID='616289';-- rownum<100;

   BEGIN
      /* Get the Sequence number From TDM_ERR_S */

       --select TDM_ERR_S.nextval into seq_no from DUAL;
   BEGIN
         /* Iterate the TDM_DATA info */
        rec_count_insert := 0;
        rec_count_update := 0;
      FOR rec IN insrt_data
      LOOP
         BEGIN
             del_qry_where := NULL;
            --DBMS_OUTPUT.put_line ('Data id=' || rec.data_id);
            insctx := dbms_xmlstore.newcontext (target_schema_name||'.' || rec.tname);
            dbms_xmlstore.clearupdatecolumnlist (insctx);
            --DBMS_OUTPUT.put_line('Before getUpdatedXmlKeys' || rec.insdata.getstringval());
            updated_xml := getUpdatedXmlKeys(rec.insdata,rec.tname,rec.crt_dttm,source_schema_name);
            --DBMS_OUTPUT.put_line ('InsertDataFromXML() : updated_xml=' || updated_xml.getstringval());
            /* Inserts the XML to the database . If it return  NULL then there is no table in Target_Schema*/
            ROWS := dbms_xmlstore.insertxml (insctx, updated_xml);
            IF ROWS IS NULL THEN
                err_msg :='Table Not Found in Target_Schema' || rec.tname;
                INSERT INTO TDM_ERR VALUES (seq_no,'Insert','data_id='||rec.data_id ||substr(err_msg,1,500),sysdate,sysdate,'TDM','TDM');
            END IF;
           rec_count_insert := rec_count_insert + 1;
           --DBMS_OUTPUT.put_line ('InsertDataFromXML() : inserted xml=' || updated_xml.getstringval());
            /* If the Insert Fails Exception Block will be executed */

          EXCEPTION
            WHEN OTHERS
            THEN
               err_num := SQLCODE;
             /* a) If the error Code is -31011 then insert failed because the row already exists with the same Pk
                b) Update.
             */

              IF (err_num = -31011)
               THEN
                 declare
                   cursor c1 is SELECT extractvalue(column_value,'/COLUMN_NAME') col
                                from XMLTable ('//ROWSET/ROW/COLUMN_NAME' PASSING rec.pkey_data);
                   cursor c2 is select t2.COLUMN_VALUE.getrootelement() col_name from XMLTable('/ROW/*' passing rec.insdata) t2;
                 begin
                 dbms_xmlstore.clearupdatecolumnlist (insctx);
                 for myrec1 in c1
                 loop
                    --DBMS_OUTPUT.put_line ('Primary Key Columns=' || myrec1.col);
                     select extractValue(rec.insdata,'/ROW/'||myrec1.col) into pk_val_valid FROM DUAL;
                     --DBMS_OUTPUT.put_line ('Primary Key Value=' || pk_val_valid);
                     IF pk_val_valid IS NOT NULL THEN
                            --DBMS_OUTPUT.put_line ('Primary Key Columns=' || myrec1.col);
                            dbms_xmlstore.setkeycolumn(insctx, myrec1.col);
                     END IF;
                 end loop;
                 for myrec2 in c2
                  loop
                             --DBMS_OUTPUT.put_line ('Other Columns=' || myrec2.col_name);
                   dbms_xmlstore.setupdatecolumn(insctx, myrec2.col_name);
                  end loop;
                 ROWS := dbms_xmlstore.updatexml(insctx, updated_xml);
                  rec_count_update := rec_count_update +1;
                 EXCEPTION
                    WHEN OTHERS THEN
                    
                    --DBMS_OUTPUT.put_line ('Cannot Insert. Try Update. InsertDataFromXML() : err_num1=' || SQLCODE ||SQLERRM  || rec.data_id);
                    IF (err_num = -31011)
                    THEN
                         --DBMS_OUTPUT.put_line ('inside -31011');
                          begin
                          updttx := dbms_xmlstore.newcontext (target_schema_name||'.' || rec.tname);
                          dbms_xmlstore.clearupdatecolumnlist (updttx);
                        for myrec1 in c1
                        loop
                            --DBMS_OUTPUT.put_line ('Primary Key Columns=' || myrec1.col);
                            select extractValue(rec.insdata,'/ROW/'||myrec1.col) into pk_val_valid FROM DUAL;
                            --DBMS_OUTPUT.put_line ('Primary Key Value=' || pk_val_valid);
                            IF pk_val_valid IS NOT NULL THEN
                                --DBMS_OUTPUT.put_line ('Primary Key Columns=' || myrec1.col);
                                dbms_xmlstore.setkeycolumn(updttx, myrec1.col);
                                --DBMS_OUTPUT.put_line ('Hello' || myrec1.col);
                            END IF;
                        end loop;
                          ROWS := dbms_xmlstore.updatexml(updttx, updated_xml);
                          rec_count_update := rec_count_update +1;
                          EXCEPTION
                            WHEN OTHERS THEN
                            --DBMS_OUTPUT.put_line ('Exception in Second Update ' ||SQLCODE ||'for dataId= '||rec.data_id  ||' And Error Message'||SQLERRM);
                            err_msg :='Data_id='||rec.data_id ||' Exception= '||SQLERRM;
                            --INSERT INTO TDM_ERR VALUES (seq_no,'updateDataWithNewKeyValues-U1',substr(err_msg,1,500),sysdate,sysdate,'TDM','TDM');
                          IF (INSTRB(err_msg,'FK_CICGT_CII',1,1) != 0 or INSTRB(err_msg,'FK_RCIT_CI1I',1,1) != 0 OR INSTRB(err_msg,'FK_RCIT_CI2I',1,1) != 0)
                          THEN
                            PKG_TESTDATA_ERR.insertError(NULL,err_msg,'INSERTDATAFROMXML');
                          ELSE
                            PKG_TESTDATA_ERR.insertError(seq_no,err_msg,'INSERTDATAFROMXML');
                          END IF;
                          end;
                    else
                        --DBMS_OUTPUT.put_line ('Exception in Update ' ||SQLCODE ||'for dataId= '||rec.data_id  ||' And Error Message'||SQLERRM);
                        err_msg :='Data_id='||rec.data_id ||' Exception= '||SQLERRM;
                        --INSERT INTO TDM_ERR VALUES (seq_no,'updateDataWithNewKeyValues-U',substr(err_msg,1,500),sysdate,sysdate,'TDM','TDM');
                        IF (INSTRB(err_msg,'FK_CICGT_CII',1,1) != 0 or INSTRB(err_msg,'FK_RCIT_CI1I',1,1) != 0 OR INSTRB(err_msg,'FK_RCIT_CI2I',1,1) != 0)
                            THEN
                            PKG_TESTDATA_ERR.insertError(NULL,err_msg,'INSERTDATAFROMXML');
                        ELSE
                            PKG_TESTDATA_ERR.insertError(seq_no,err_msg,'INSERTDATAFROMXML');
                        END IF;
                    end if;

                 end;

              ELSE
                  --DBMS_OUTPUT.put_line ('Exception in Insert ' ||SQLCODE ||'for dataId= '||rec.data_id  ||' And Error Message'||SQLERRM);
                  err_msg :='Data_id='||rec.data_id ||' Exception= '||SQLERRM;
                  --INSERT INTO TDM_ERR VALUES (seq_no,'insertDataWithNewKeyValues-O','data_id='||rec.data_id ||substr(err_msg,1,500),sysdate,sysdate,'TDM','TDM');
                  IF (INSTRB(err_msg,'FK_CICGT_CII',1,1) != 0 or INSTRB(err_msg,'FK_RCIT_CI1I',1,1) != 0 OR INSTRB(err_msg,'FK_RCIT_CI2I',1,1) != 0)
                  THEN
                      PKG_TESTDATA_ERR.insertError(NULL,err_msg,'INSERTDATAFROMXML');
                  ELSE
                      PKG_TESTDATA_ERR.insertError(seq_no,err_msg,'INSERTDATAFROMXML');
                  END IF;
                 -- updateRunStatus('Y',target_schema_name);
                  RAISE OTHER_EXCEPTIONS;
               END IF;

         END;
         -- Close the context

         dbms_xmlstore.closecontext (insctx);
      END LOOP;
      --commit;
      DBMS_OUTPUT.put_line(target_schema_name || '- rec_count_insert' || rec_count_insert);
      DBMS_OUTPUT.put_line(target_schema_name || '- rec_count_update' || rec_count_update);

   EXCEPTION
           WHEN OTHER_EXCEPTIONS THEN
           err_msg :='Exception= '||SQLERRM;
           PKG_TESTDATA_ERR.insertError(seq_no,err_msg,'INSERTDATAFROMXML');
          --DBMS_OUTPUT.put_line ('The Program Exited With Error Code ' ||SQLCODE ||'for dataId= '||' And Error Message'||SQLERRM);

   END;

   END;

   /*  Enable/Disable the constraints . If the Status is passed as 'YES' constraints will be ENABLED .Else DISABLED */

    PROCEDURE enableDisableConstraints(status IN VARCHAR2,target_schema_name IN VARCHAR2,cons_type VARCHAR2,seq_no IN VARCHAR2 default null) IS
    err_msg VARCHAR2(600);
    BEGIN
    --DBMS_OUTPUT.put_line ('schema_name'||schema_name);
       FOR cur_rec IN (SELECT table_name, constraint_name
                         FROM all_constraints
                        WHERE owner = target_schema_name
                            AND constraint_type  =cons_type
                            and constraint_name not in (select constraint_name from tmp_disabled_constraints where schema_name=target_schema_name)
                           )
                            -- AND TABLE_NAME IN (SELECT DISTINCT(TNAME) table_name FROM TDM_DATA a)
                            --and constraint_name not in (select constraint_name from tmp_disabled_constraints))
         LOOP
         EXECUTE IMMEDIATE 'ALTER TABLE '||target_schema_name ||'.' || cur_rec.table_name
                                           || CASE status WHEN 'YES' THEN ' ENABLE NOVALIDATE ' ELSE ' DISABLE ' END
                                           || 'CONSTRAINT '
                                           || cur_rec.constraint_name;
         END LOOP;

       EXCEPTION WHEN OTHERS THEN
       err_msg :=' Exception= '||SQLERRM;
       PKG_TESTDATA_ERR.insertError(seq_no,err_msg,'enableDisableConstraints');
       --INSERT INTO TDM_ERR VALUES (seq_no,'enableDisableConstraints',err_msg,sysdate,sysdate,'TDM','TDM');
       --updateRunStatus('Y',schema_name);
       raise TDM_EXCEPTION;

    END;

    /*  Enable/Disable the triggers . If the Status is passed as 'YES' triggers will be ENABLED .Else DISABLED */

     PROCEDURE enableDisableTriggers(status IN VARCHAR2,schema_name IN VARCHAR2,seq_no IN VARCHAR2 default null) IS
     err_msg VARCHAR2(600);
        BEGIN
            FOR cur_rec IN (SELECT table_name FROM ALL_TRIGGERS WHERE OWNER=schema_name)
             LOOP

             --DBMS_OUTPUT.put_line ('inside loop...' || cur_rec.table_name);
             EXECUTE IMMEDIATE 'ALTER TABLE '||schema_name ||'.' || cur_rec.table_name
                                               || CASE status WHEN 'YES' THEN ' ENABLE ALL TRIGGERS' ELSE ' DISABLE ALL TRIGGERS' END;

             END LOOP;
            EXCEPTION WHEN OTHERS THEN
            err_msg :=' Exception= '||SQLERRM;
            --INSERT INTO TDM_ERR VALUES (seq_no,'enableDisableTriggers',err_msg,sysdate,sysdate,'TDM','TDM');
            PKG_TESTDATA_ERR.insertError(seq_no,err_msg,'enableDisableTriggers');
            --updateRunStatus('Y',schema_name);
            raise TDM_EXCEPTION;

        END;

    /*
           a) if new_col_val is populated for a combination of table_name , column_name and source_schema in TDM_KEYS return new_col_val.
           b) if step (a) is not true then  check for the same combination in  TDM_RFRNTL_KEYS and get the Key_Id whose value can then be
              retervied from TDM_KEYS.
           c) If step (a) and (b) are false then return NULL.
    */

    FUNCTION getResultsFromKeyTables(tab_nam IN VARCHAR2,col_nam IN VARCHAR2 ,col_val VARCHAR2,source_schema_name IN VARCHAR2) RETURN VARCHAR2 IS
    key_val VARCHAR2(200);
    new_col_val VARCHAR2(4000);
    new_col_val1 NUMBER;
    key_val_count VARCHAR2(200);
    tab1_name VARCHAR2(100);
    col_name VARCHAR2(100);
    col_value VARCHAR(1000);
    BEGIN

         BEGIN
         --DBMS_OUTPUT.put_line ('getResultsFromKeyTables()' || tab_nam || col_nam|| col_val);
          select tdm.COL_NEW_VALUE INTO new_col_val FROM TDM_KEYS tdm WHERE TNAME=tab_nam AND COL_NAME=col_nam AND COL_VALUE=col_val AND SCHEMA_NAME=source_schema_name;

         EXCEPTION when no_data_found
         then
             new_col_val := NULL;
         END;
        --DBMS_OUTPUT.put_line ('getResultsFromKeyTables()1' || new_col_val);
        BEGIN

          IF new_col_val IS  NULL
          THEN
            select tdm_keys.KEY_ID INTO key_val FROM TDM_RFRNTL_KEYS tdm_keys WHERE TNAME=tab_nam AND COL_NAME=col_nam AND COL_VALUE=col_val AND SCHEMA_NAME=source_schema_name;
             key_val_count := -1;
          END IF;
          EXCEPTION when no_data_found
          then
            key_val_count := -2;

          END;
        --DBMS_OUTPUT.put_line ('getResultsFromKeyTables()2' ||key_val);
         BEGIN

         IF(key_val_count= -1)
         THEN
            select COL_NEW_VALUE INTO new_col_val FROM TDM_KEYS WHERE KEY_ID=key_val ;
         END IF;
         EXCEPTION when no_data_found
         then
            new_col_val := NULL;
         END;
         --DBMS_OUTPUT.put_line ('getResultsFromKeyTables()2' || new_col_val);
     return new_col_val;

    END;

    /*
            Validate and return 'YES' if the passed String is a valid date. Else Return 'N"
    */

     FUNCTION validateDate(xml_field_val VARCHAR2) RETURN VARCHAR2 IS
     STATUS VARCHAR2(10);
     valid_date DATE;
     BEGIN

              IF xml_field_val IS NULL OR xml_field_val ='' OR length(xml_field_val)=0 THEN
                  --DBMS_OUTPUT.put_line ('validateDate() :length(xml_field_val) ' || length(xml_field_val));
                     STATUS :='NO';
                 RETURN STATUS;
                 ELSE
                   --SELECT TO_DATE (xml_field_val, 'DD-MON-YYYY HH24:MI:SS') INTO valid_date FROM DUAL;
                   valid_date := TO_DATE (xml_field_val, 'DD-MON-YYYY HH24:MI:SS');
                    --DBMS_OUTPUT.put_line ('validateDate():valid date=' ||  valid_date);
                    STATUS :='YES';
                    RETURN STATUS;
              end if;
             EXCEPTION WHEN OTHERS THEN
             STATUS := 'NO';
     return STATUS;
     END;


    /*
            Updates the following fields in the XML a) all the date fields in XML b) CRT_DTMM c) LST_UPDT_DTTM d) CRT_USER_ID e) LST_UPDT_USER_ID

    */
    FUNCTION getUpdatedXmlKeys(ins_data IN XMLTYPE,tab_name IN VARCHAR2,crt_dttm IN VARCHAR2,source_schema_name IN VARCHAR2) RETURN XMLTYPE IS
          child_col_val VARCHAR2(200);
          col_val_keytable VARCHAR2(200);
          replaced_xml_val VARCHAR2(32767);
          xml_builder XMLTYPE;
          date_validate VARCHAR2(10);
          modified_xml_date VARCHAR2(20);
          created_date_time VARCHAR2(20);
          xml_update_string VARCHAR2(32767);
          xml_create_date_upd VARCHAR2(2000);
          nodevalue VARCHAR2(2000);

    /* Cursor gives the node name and node values from the XML*/
    CURSOR c1
     IS
     select extractvalue(t2.COLUMN_VALUE,'/*') nodevalues,t2.COLUMN_VALUE.getrootelement() nodes from xmltable('/ROW/*' passing ins_data) t2;
     BEGIN
         --xml_builder :=ins_data;
         xml_update_string :='<ROW>';
         replaced_xml_val :=null;
         /* Iterate the Cursor */

         FOR rec IN c1
             LOOP

                /* Validate to see if each node value in the XML is a valid Date*/

                date_validate := validateDate(rec.nodevalues);
               --DBMS_OUTPUT.put_line ('date_validate for ' || rec.nodes ||'= '||date_validate);
               /*
                    a) If the node value is a valid date field enter the IF block.
                    b) If the node name is CRT_DTTM and LST_UPDT_DTTM the date can be made as sysdate.
                    c) For the node name with Valid date Format Values perform the followinglogic
                            (subtract the crt_dttm with nodevalues) and subtract the return value with SYSDATE).
                      If the date format logic throws Error default it to '31-DEC-9999 00:00:00';

               */

                IF date_validate = 'YES' --AND rec.nodes !='' AND rec.nodes is NOT NULL
                THEN
                        IF(rec.nodes ='CRT_DTTM' OR rec.nodes='LST_UPDT_DTTM') then

                         modified_xml_date :=sysdate;
                        ELSE
                             BEGIN
                              modified_xml_date  := to_char(SYSDATE - (TO_DATE(crt_dttm,'DD-MM-YYYY HH24:MI:SS')-TO_DATE(rec.nodevalues,'DD-MON-YYYY HH24:MI:SS')),'DD-MON-YYYY HH24:MI:SS') ;
                             EXCEPTION WHEN OTHERS THEN
                              modified_xml_date  :='31-DEC-9999 00:00:00';
                             END;
                             if (substr(rec.nodevalues,13) = '00:00:00')
                             then
                               --DBMS_OUTPUT.put_line ('GG modified_xml_date' || modified_xml_date);
                               modified_xml_date := substr(modified_xml_date,1,12) || '00:00:00';
                               --DBMS_OUTPUT.put_line ('GG modified_xml_date' || modified_xml_date);
                             end if;
                         END IF;

                    --DBMS_OUTPUT.put_line ('modified_xml_date' || modified_xml_date);

                 /* Construct the string with the updated values */


                       IF modified_xml_date IS NOT NULL THEN
                            --xml_update_string := xml_update_string || ',''' || '/ROW/'||rec.nodes||'/text()'||''','''||modified_xml_date||''' ' ;
                            xml_update_string := xml_update_string || '<' || rec.nodes||'>'|| modified_xml_date||'</' || rec.nodes || '>';
                       END IF;


                END IF;

                 /* If the date_validate is NO then enter this IF block
                        a) If the node name is 'CRT_USER_ID' or 'LST_UPDT_USER_ID' then substitute the VALUE 'TDM'.
                        b) For other nodes call the function getResultsFromKeyTables() to get the updated value.
                 */
                 IF date_validate ='NO'
                THEN
                   IF(rec.nodes ='CRT_USER_ID' OR rec.nodes='LST_UPDT_USER_ID') then
                     col_val_keytable :='TDM';
                   ELSE
                     col_val_keytable := getResultsFromKeyTables(tab_name,rec.nodes,rec.nodevalues,source_schema_name);
                     --DBMS_OUTPUT.put_line('col_val_keytable' ||rec.nodes ||'-'||col_val_keytable||'-' ||rec.nodevalues);
                   END IF;

                   /* Construct the string with the updated values */

                   IF col_val_keytable IS NOT NULL THEN
                            nodevalue := dbms_xmlgen.convert(col_val_keytable);
                            --select dbms_xmlgen.convert(col_val_keytable) into  nodevalue from dual;
                            xml_update_string := xml_update_string || '<' || rec.nodes||'>'|| nodevalue||'</' || rec.nodes || '>';
                   ELSE
                     IF rec.nodevalues IS NOT NULL THEN
                           nodevalue := dbms_xmlgen.convert(rec.nodevalues);
                           --select dbms_xmlgen.convert(rec.nodevalues) into   nodevalue from dual;
                           xml_update_string := xml_update_string || '<' || rec.nodes||'>'|| nodevalue||'</' || rec.nodes || '>';
                     ELSE
                           xml_update_string := xml_update_string || '<' || rec.nodes||'/>';
                     END IF;
                   END IF;


                END IF;


              --DBMS_OUTPUT.put_line ('rec.node' || rec.nodes);

              END LOOP;

       /* a) If the updated String is NOT NULL then update the unmodified XML stored in the TMP_XMLTABLE .
          Note : The reason for maintainting TEMP table is to avoid the memory issue .
       */

            xml_update_string := xml_update_string || '</ROW>';

            --DBMS_OUTPUT.put_line ('InsertDataFromXML() : updated_xml=' || xml_update_string);
            --SELECT XMLTYPE(xml_update_string) into xml_builder FROM dual;
            xml_builder := XmlType(xml_update_string);

        --DBMS_OUTPUT.put_line ('InsertDataFromXML() :H updated_xml=' || xml_builder.getstringval());
         return xml_builder;

     END;

      /* This function will populate the new_col_val of TDM_KEYS table */

      PROCEDURE PopulateNewColValues(source_schema_name IN VARCHAR2,target_schema_name IN VARCHAR2,SEQ_NO IN VARCHAR2) IS
      seq_name VARCHAR2(100);
      schem_name VARCHAR2(100);
      result_msg VARCHAR2(10);
      tmp_seq_name VARCHAR2(100);
      err_mess VARCHAR2(300);
      seq_num VARCHAR2(20);
      keyid varchar2(20);
      COUNT_EXCEPTIONS EXCEPTION;
      BEGIN
         if seq_no is null then
           select TDM_ERR_S.nextval into seq_num from DUAL;
         else
           seq_num := seq_no;
         end if;
         save_sequence := seq_no;
         /* Validate in Target Shcema to see to the Sequence Exist for all the col_name in TDM_DATA */

         result_msg:=validateseq(source_schema_name,target_schema_name,seq_num);

         /* IF YES this block will get Executed and TDM_KEYS table will be updated.*/

         IF result_msg ='YES' THEN
             FOR REC IN (SELECT KEY_ID,TNAME,COL_NAME FROM TDM_KEYS WHERE TNAME NOT IN(SELECT TNAME FROM TDM_MASTER_T union select mview_name from all_mviews where owner = target_schema_name) and SCHEMA_NAME=source_schema_name and COL_NEW_VALUE IS NULL)
             LOOP
                BEGIN
                    schem_name := target_schema_name;
                    keyid :=REC.KEY_ID;
                    SELECT SEQUENCE_NAME,SCHEMA_NAME into seq_name,schem_name FROM TDM_KEY_SEQ WHERE TNAME=REC.TNAME and COL_NAME=REC.COL_NAME;
                    IF schem_name IS NULL THEN
                        schem_name :=target_schema_name;
                    END IF;

                 execute immediate 'UPDATE TDM_KEYS SET COL_NEW_VALUE='||schem_name ||'.' ||seq_name||'.nextval WHERE KEY_ID=' || REC.key_id;
                EXCEPTION WHEN no_data_found THEN                     IF  SUBSTR(REC.COL_NAME,length(REC.COL_NAME)-1,length(REC.COL_NAME))='ID' THEN
                        tmp_seq_name := CONCAT(SUBSTR(REC.COL_NAME,1,length(REC.COL_NAME)-2),'S');
                     ELSE
                        tmp_seq_name :=CONCAT(REC.COL_NAME,'_S');
                     END IF;
                     -- tmp_seq_name := CONCAT(SUBSTR(REC.COL_NAME,1,length(REC.COL_NAME)-2),'S');
                     execute immediate 'UPDATE TDM_KEYS SET COL_NEW_VALUE='||target_schema_name ||'.' ||tmp_seq_name||'.nextval WHERE KEY_ID=' || REC.key_id;
                END;
             END LOOP;
             /*
             FOR CURREC IN (SELECT KEY_ID,TNAME,COL_VALUE FROM TDM_KEYS WHERE TNAME IN(SELECT TNAME FROM TDM_MASTER_T))
              LOOP
                execute immediate 'UPDATE TDM_KEYS SET COL_NEW_VALUE='''|| CURREC.COL_VALUE ||''' WHERE KEY_ID=' || CURREC.key_id;
              END LOOP;
             */


         END IF;

          EXCEPTION WHEN OTHERS THEN
            IF seq_name IS NULL THEN
              seq_name :=tmp_seq_name;
            END IF;
            err_mess :='SEQ_NAME= '|| seq_name ||' KEY_ID='||keyid ||' Exception= '||SQLERRM;
            PKG_TESTDATA_ERR.insertError(seq_no,err_mess,'populatencolnewval');
           --INSERT INTO TDM_ERR VALUES (SEQ_NUM,'populatencolnewval',err_mess,sysdate,sysdate,'TDM','TDM');

      END;

     /*
        Validate the Sequence in TargetSchema and return 'YES' if exists and 'NO' if not.
     */
     FUNCTION validateseq(source_schema_name IN VARCHAR2,target_schema_name IN VARCHAR2,SEQ_NO IN VARCHAR2) RETURN VARCHAR2 IS
       seq_name VARCHAR2(100);
       tmp_seq_name VARCHAR2(100);
       err_msg VARCHAR2(600);
       result_msg VARCHAR2(10);
       seq_qry varchar2(1000);
       syn_qry varchar2(1000);
       keyid varchar2(100);
       row_count number;
       tmp_schema_name VARCHAR2(100);
       sch_name VARCHAR2(100);
       type cur_type is ref cursor;
       c cur_type;

       BEGIN
        result_msg :='YES';

         /* Get the TNAME and COL_NAME from TDM_KEYS if they doesn't exist in TDM_MASTER_T*/

         FOR REC IN (SELECT KEY_ID,TNAME,COL_NAME
                     FROM TDM_KEYS
                     WHERE TNAME NOT IN(SELECT TNAME FROM TDM_MASTER_T union select mview_name from all_mviews where owner = target_schema_name) AND SCHEMA_NAME=source_schema_name and col_new_value is null)
            LOOP
                BEGIN
                   keyid:=REC.KEY_ID;
                   sch_name := target_schema_name;
                   /* Get the Sequence_name and Schema_name From TDM_KEY_SEQ based on the Tname and Col_name*/
                   begin
                     SELECT SEQUENCE_NAME,SCHEMA_NAME
                     into seq_name,sch_name
                     FROM TDM_KEY_SEQ
                     WHERE TNAME=REC.TNAME
                     and COL_NAME=REC.COL_NAME;
                   exception when no_data_found then
                     IF  SUBSTR(REC.COL_NAME,length(REC.COL_NAME)-1,length(REC.COL_NAME))='ID' THEN
                        seq_name := CONCAT(SUBSTR(REC.COL_NAME,1,length(REC.COL_NAME)-2),'S');
                     ELSE
                        seq_name :=CONCAT(REC.COL_NAME,'_S');
                     END IF;
                   end;
                   --dbms_output.put_line('Tmp_Schema_Name:'||tmp_schema_name);
                   --dbms_output.put_line('Tmp_Seq:'||seq_name);
                   tmp_schema_name :=sch_name;
                   tmp_seq_name :=seq_name;

                   /* There is a possibility that the schema name could be NULL . In that case default the schema_name to Target_schema_name passed .*/

                   IF seq_name IS NOT NULL AND sch_name is NULL THEN
                    tmp_schema_name :=target_schema_name;
                    tmp_seq_name :=seq_name;
                   END IF;
                   --dbms_output.put_line('Tmp_Schema_Name2:'||tmp_schema_name);
                   --dbms_output.put_line('Tmp_Seq2:'||seq_name);

                   seq_qry:='SELECT count(1) cnt from all_sequences where sequence_owner=''' ||tmp_schema_name || ''' and sequence_name='''||tmp_seq_name ||'''';
                   --seq_qry := 'select count(1) from all_sequences where sequence_owner ';
                   --dbms_output.put_line(seq_qry);
                   syn_qry:=' SELECT count(1) cnt FROM ALL_SYNONYMS WHERE TABLE_OWNER=''' ||tmp_schema_name || ''' and synonym_name='''||tmp_seq_name||'''';

                   /* Execute to see if the Sequence Exist in the schema*/

                   execute immediate seq_qry into row_count;

                   --dbms_output.put_line ('RowCnt:'||to_char(row_count));

                   /* If the row_count is 0 check to see if the synonym exist in the schema*/
                   IF row_count=0 then
                    execute immediate syn_qry into row_count;
                   END IF;

                   --dbms_output.put_line('validateseq() : row_count'||row_count);
                        --execute immediate 'BEGIN '||seq_qry||'; END;';

                   /* If the Sequence and Synonym does not exist error it out to TDM_ERR table.*/

                    IF row_count = 0 THEN
                        result_msg :='NO';
                         err_msg :=tmp_seq_name ||' is missing. '||' KEY_ID='||keyid;
                         PKG_TESTDATA_ERR.insertError(seq_no,err_msg,'validateseq');
                        --INSERT INTO TDM_ERR VALUES (SEQ_NO,'validateseq',substr(err_msg,1,500),sysdate,sysdate,'TDM','TDM');
                    END IF;

                   /* Exception block will get executed If the Schema and Sequence Name is not present in TDM_KEY_SEQ
                       a) If the last two characters for the COL_NAME is 'ID 'replace it with 'S' to get the Sequence NAme
                       b) If the last two characters for the COL_NAME is not 'ID' append _S to it to get the Sequence Name
                   */

                END;
            END LOOP;

       --DBMS_OUTPUT.put_line ('result_msg' || result_msg);

       return result_msg;
       END;

     /* Update trhe TDM_STATUS table based on value passed . Value will be 'YES"/'NO'*/

     PROCEDURE updateRunStatus(runstatus IN VARCHAR2,target_schema_name IN VARCHAR2) IS
      BEGIN
         execute immediate 'UPDATE TDM_STATUS SET STATUS='''||runstatus|| ''' WHERE SCHEMA_NAME='''||target_schema_name||'''';
      END;

     PROCEDURE updateJobStatus(jobstatus IN VARCHAR2,target_schema_name IN VARCHAR2) IS
      BEGIN
         execute immediate 'UPDATE TDM_STATUS SET JOB_STATUS='''||jobstatus|| ''' WHERE SCHEMA_NAME='''||target_schema_name||'''';
      END;
  /*
    This function will return the primary Key Value based on the tab_name and schema_name.
  */
  /*FUNCTION getPrimaryColumn(sname IN VARCHAR2, tab_name IN VARCHAR2) return pKeys IS
     cons_type VARCHAR2(1);
     index_type VARCHAR2(6);
     str varchar2(4000);
     my_pkname pKeys;
     cnt NUMBER;
     cnt_qry VARCHAR2(4000);
     return_val VARCHAR2(100);
     BEGIN

        --dbms_output.put_line('tab_name' ||tab_name);
        return_val :=NULL;
         cons_type :='P';
         my_pkname := pKeys();
         --my_pkname.extend(1);
         --cnt_qry:= 'SELECT COUNT(COLUMN_NAME)  FROM ALL_CONS_COLUMNS WHERE CONSTRAINT_NAME =(select CONSTRAINT_NAME from all_constraints where OWNER=''' ||sname ||'''' ||' AND TABLE_NAME=''' || tab_name || ''' AND CONSTRAINT_TYPE=''' || cons_type ||'''' ||') AND OWNER='''||sname||'''';
         str :='SELECT COLUMN_NAME  FROM ALL_CONS_COLUMNS WHERE CONSTRAINT_NAME =(select CONSTRAINT_NAME from all_constraints where OWNER=''' ||sname ||'''' ||' AND TABLE_NAME=''' || tab_name || ''' AND CONSTRAINT_TYPE=''' || cons_type ||'''' ||') AND OWNER='''||sname||'''';
        --dbms_output.put_line(str);
         --EXECUTE IMMEDIATE cnt_qry INTO cnt;
         --IF cnt>0 THEN
         BEGIN
         EXECUTE IMMEDIATE str BULK COLLECT INTO my_pkname;
         if my_pkname.count < 1 THEN
            index_type := 'UNIQUE';
            --dbms_output.put_line('Again' || tab_name);
            str :='SELECT COLUMN_NAME  FROM ALL_IND_COLUMNS WHERE INDEX_NAME =(select INDEX_NAME from all_indexes where OWNER=''' ||sname ||'''' ||' AND TABLE_NAME=''' || tab_name || ''' AND uniqueness=''' || index_type ||'''' ||') AND INDEX_OWNER='''||sname||'''';
            EXECUTE IMMEDIATE str BULK COLLECT INTO my_pkname;
         end if;
         EXCEPTION WHEN OTHERS THEN
             NULL;
         END;

         return my_pkname;
     END;*/
     
     PROCEDURE savePrimaryColumns(sname IN VARCHAR2) IS
     cons_type VARCHAR2(1);
     index_type VARCHAR2(6);
     str varchar2(4000);
     my_pkname pKeys;
     my_pknameSplit pKeys;
     my_uiname pKeys;
     my_uinameSplit pKeys;
     cnt NUMBER;
     cnt_qry VARCHAR2(4000);
     return_val VARCHAR2(100);
     tab_name VARCHAR2(100);
     BEGIN

        --dbms_output.put_line('tab_name' ||tab_name);
        return_val :=NULL;
         cons_type :='P';
         my_pkname := pKeys();
         --my_pkname.extend(1);
         --cnt_qry:= 'SELECT COUNT(COLUMN_NAME)  FROM ALL_CONS_COLUMNS WHERE CONSTRAINT_NAME =(select CONSTRAINT_NAME from all_constraints where OWNER=''' ||sname ||'''' ||' AND TABLE_NAME=''' || tab_name || ''' AND CONSTRAINT_TYPE=''' || cons_type ||'''' ||') AND OWNER='''||sname||'''';
         str :='SELECT COLUMN_NAME, TABLE_NAME FROM ALL_CONS_COLUMNS WHERE CONSTRAINT_NAME in (select CONSTRAINT_NAME from all_constraints where OWNER=''' ||sname || ''' AND CONSTRAINT_TYPE=''' || cons_type ||'''' ||') AND OWNER='''||sname||''' order by table_name';
         --dbms_output.put_line('str' || str);
         --BEGIN
         EXECUTE IMMEDIATE str BULK COLLECT INTO my_pkname;
         tab_name := my_pkname(1).table_name;
         my_pknameSplit := pKeys();
         cnt := 0;
         FOR i IN my_pkname.first .. my_pkname.last
         LOOP
         --dbms_output.put_line(my_vChld(i).constraint_name || '-' || my_vChld(i).p_table_name || '-'|| my_vChld(i).c_table_name);
         if tab_name = my_pkname(i).table_name then
            my_pknameSplit.extend(1);
            cnt := cnt + 1;
            my_pknameSplit(cnt) := my_pkname(i);
           else
            my_tblpKeys(tab_name) := my_pknameSplit;
            tab_name := my_pkname(i).table_name;
            my_pknameSplit := pKeys();
            my_pknameSplit.extend(1);
            cnt := 1;
            my_pknameSplit(1) := my_pkname(i);
         end if;
         END LOOP;
         my_tblpKeys(tab_name) := my_pknameSplit;
         index_type := 'UNIQUE';
        --dbms_output.put_line('Again' || tab_name);
         str :='SELECT COLUMN_NAME, TABLE_NAME  FROM ALL_IND_COLUMNS WHERE INDEX_NAME IN(select INDEX_NAME from all_indexes where OWNER=''' ||sname ||''' AND uniqueness=''' || index_type ||'''' ||') AND INDEX_OWNER='''||sname||''' order by table_name';
         EXECUTE IMMEDIATE str BULK COLLECT INTO my_uiname;
         tab_name := my_uiname(1).table_name;
         my_uinameSplit := pKeys();
         cnt := 0;
         FOR i IN my_uiname.first .. my_uiname.last
         LOOP
         --dbms_output.put_line(my_vChld(i).constraint_name || '-' || my_vChld(i).p_table_name || '-'|| my_vChld(i).c_table_name);
         if tab_name = my_uiname(i).table_name then
            my_uinameSplit.extend(1);
            cnt := cnt + 1;
            my_uinameSplit(cnt) := my_uiname(i);
           else
            my_tbluiKeys(tab_name) := my_uinameSplit;
            tab_name := my_uiname(i).table_name;
            my_uinameSplit := pKeys();
            my_uinameSplit.extend(1);
            cnt := 1;
            my_uinameSplit(1) := my_uiname(i);
         end if;
         END LOOP;
         my_tbluiKeys(tab_name) := my_uinameSplit;
         --EXCEPTION WHEN OTHERS THEN
         --    dbms_output.put_line('Exception');
         --END;

     END;


   /*  Delete Cascade Functionality implemented .
        a) Look for the children and leaf row based on parent table name and where clause .
        b) Delete using botton-up approach from leaf row all the way to parent row.
   */
    PROCEDURE saveChildRelationships(sname in VARCHAR2) IS
       my_vChld vChld;
       vChldSplit vChld;
       str VARCHAR2(4000);
       tab_name VARCHAR2(200);
       cnt NUMBER;

    BEGIN
        str := 'select rc.constraint_name, c.table_name c_table_name, c.column_name c_column_name,
            p.table_name p_table_name, p.column_name p_column_name
     from all_constraints rc, all_cons_columns c, all_cons_columns p
     where rc.constraint_type = ''R''
     and   rc.owner =''' || sname ||
     ''' and   c.owner = '''|| sname ||
     ''' and   p.owner = '''|| sname ||
     ''' and   p.constraint_name = rc.r_constraint_name
     and   c.constraint_name = rc.constraint_name
     and   c.position = p.position
     order by p.table_name, rc.constraint_name,p.position';
        
        EXECUTE IMMEDIATE str BULK COLLECT INTO my_vChld;
        tab_name := my_vChld(1).p_table_name;
        vChldSplit := vChld();
        cnt := 0;
        --dbms_output.put_line('Hello');
        FOR i IN my_vChld.first .. my_vChld.last
        LOOP
         --dbms_output.put_line(my_vChld(i).constraint_name || '-' || my_vChld(i).p_table_name || '-'|| my_vChld(i).c_table_name);
         if tab_name = my_vChld(i).p_table_name then
            vChldSplit.extend(1);
            cnt := cnt + 1;
            vChldSplit(cnt) := my_vChld(i);
           else
            my_tblChld(tab_name) := vChldSplit;
            tab_name := my_vChld(i).p_table_name;
            vChldSplit := vChld();
            vChldSplit.extend(1);
            cnt := 1;
            vChldSplit(1) := my_vChld(i);
         end if;
        END LOOP;
        my_tblChld(tab_name) := vChldSplit;
    END;

    PROCEDURE delChildrenRows(sname IN VARCHAR2, tab_name IN VARCHAR2,w_cls VARCHAR2,s_cls VARCHAR2,parent_flag VARCHAR2 default 'Y',parentRunId VARCHAR2 default '0',seq_no IN VARCHAR2 default null) IS
    /* This Cursor return the parent table , children table */
    /*cursor cur_children(sname varchar2, tname varchar2) is
        select rc.constraint_name, c.table_name c_table_name, c.column_name c_column_name,
            p.table_name p_table_name, p.column_name p_column_name
     from all_constraints rc, all_cons_columns c, all_cons_columns p
     where rc.constraint_type = 'R'
     and   rc.owner = sname
     and   c.owner = sname
     and   p.owner = sname
     and   p.constraint_name = rc.r_constraint_name
     and   p.table_name = tname
     and   c.constraint_name = rc.constraint_name
     and   c.position = p.position
     order by rc.constraint_name,p.position;*/
     --cursor c1(sname VARCHAR2) is SELECT DEL_QRY FROM TMP_CHILD_RECORDS WHERE SCHEMA_NAME= sname order by RUN_ID DESC;
     TYPE rec_tab_type IS TABLE OF VARCHAR2(4000);
      rec_tab REC_TAB_TYPE;

     cur_children vChld;
     TYPE childs_rec is RECORD
     (
       tname varchar2(30),
       w_cls   varchar2(4000),
       tmp_par_qry varchar2(4000)
     );
     cons_nm user_constraints.constraint_name%TYPE;
     sel_qry_where VARCHAR2(4000);
     t_nm user_constraints.table_name%type;
     and_string varchar2(6);
     qry_value varchar2(100);
     type children is varray(4000) of childs_rec;
     my_children children;
     cnt number;
     my_pkey_id number;
     my_rkey_id number;
     doc1 xmltype;
     val_cnt NUMBER;
     qry varchar2(4000);
     rec_cnt NUMBER;
     err_msg VARCHAR2(600);
     tmp_parent_qry VARCHAR2(2000);
     tmp_master_qry VARCHAR2(2000);
     del_qry VARCHAR2(4000);
     cons_type VARCHAR2(10);
     pk_val VARCHAR2(20);
     pk_actual_val VARCHAR2(20);
     cons_qry VARCHAR2(4000);
     where_clause VARCHAR2(4000);
     str_qry VARCHAR2(4000);
     err_num VARCHAR2(50);
     pkey_w_cls VARCHAR2(4000);
     c_pkey_w_cls VARCHAR2(4000);
     my_pkname pKeys;
     c_my_pkname pKeys;
     pkey VARCHAR2(4000);
     rec_count NUMBER;
     i NUMBER;
     pRunId VARCHAR2(30);


  BEGIN
     my_children := children();
     cons_nm := null;
     sel_qry_where := 'where ';
     t_nm := null;
     and_string := null;
     cnt := 1;
     rec_cnt :=0;
     qry_value :='';
     pkey := null;
    if chldRelationshipsSaved is null then
        --dbms_output.put_line('saveChildRelationships Start Time - '|| systimestamp);
        saveChildRelationships(sname);
        --dbms_output.put_line('saveChildRelationships End Time - '|| systimestamp);
        --dbms_output.put_line('savePrimaryColumns End Time - '|| systimestamp);
        savePrimaryColumns(sname);
        --dbms_output.put_line('savePrimaryColumns End Time - '|| systimestamp);
        chldRelationshipsSaved := 'Y';
        select tdm_delete_s.nextval into delete_sequence from dual;
    end if;

     IF parent_flag='Y' THEN
          DELETE FROM TMP_CHILD_RECORDS where schema_name=sname;
          --select TDM_DELETE_S.nextval into pRunId from dual;
          delete_sequence_id := delete_sequence_id + 1;
          --pRunId := delete_sequence_id;
          pRunId := to_char(delete_sequence) ||'_' ||to_char(delete_sequence_id);
          INSERT INTO TMP_CHILD_RECORDS VALUES(pRunId,' DELETE FROM '||sname ||'.'||tab_name||' '||w_cls  ,tab_name,'N',' ',' SELECT ' || s_cls ||' FROM '||sname ||'.'||tab_name||'  ' ||w_cls,sysdate,sname);
     ELSE
        pRunId := parentRunId;
        --dbms_output.put_line('pRunId' || pRunId);
     END IF;
     --for rec in cur_children(sname, tab_name)
     --cur_children := vChld();
     BEGIN
     cur_children := my_tblChld(tab_name);
     --dbms_output.put_line('Table' || tab_name);
     EXCEPTION WHEN OTHERS
     THEN
        cur_children := vChld();
     END;
     --dbms_output.put_line('Table ' || tab_name || ' has ' || cur_children.count || ' child relationships');
     if cur_children.count <> 0 then
     FOR i IN cur_children.first .. cur_children.last
     loop
       --dbms_output.put_line('1.1');
     --dbms_output.put_line(cur_children(i).constraint_name || '-' || cur_children(i).c_table_name || '-'|| cur_children(i).c_column_name
     --|| '-' ||cur_children(i).p_table_name||'-' ||cur_children(i).p_column_name);
       if (cons_nm is not null  and cons_nm != cur_children(i).constraint_name)
       then
       --dbms_output.put_line('1.2');
         if (sel_qry_where != 'where ')
         then
           --dbms_output.put_line('Adding to varray:'||t_nm||','||sel_qry_where);
           my_children.extend(1);
           select t_nm, sel_qry_where,tmp_parent_qry
           into my_children(cnt)
           from dual;
           cnt := cnt + 1;
         end if;
         and_string := null;
         sel_qry_where := 'where ';
       end if;
       --dbms_output.put_line('1.3');

       if( cons_nm = cur_children(i).constraint_name and t_nm = cur_children(i).c_table_name) then
           pkey := pkey || ',' || cur_children(i).c_column_name;
       else
           pkey := cur_children(i).c_column_name;
       end if;
       cons_nm := cur_children(i).constraint_name;
       t_nm := cur_children(i).c_table_name;

       begin
            BEGIN
                --EXECUTE IMMEDIATE 'SELECT PARENT_QRY  FROM TMP_CHILD_RECORDS WHERE TABLE_NAME='''||cur_children(i).p_table_name||'''' ||'and schema_name='''||sname||'''' into tmp_master_qry;
                 --dbms_output.put_line('pRunId '|| pRunId);
                SELECT PARENT_QRY  into tmp_master_qry FROM TMP_CHILD_RECORDS WHERE RUN_ID=pRunId and SCHEMA_NAME = sname;
                EXCEPTION WHEN NO_DATA_FOUND then
                and_string:='';
            END;

            sel_qry_where :=  ' WHERE (' || pkey ||') IN ( ' || tmp_master_qry || ' )';
            --dbms_output.put_line('sel_qry_where :' || sel_qry_where);
             and_string := ' and ';
           --tmp_parent_qry := ' SELECT ' || rec.p_column_name || ' FROM ' ||sname ||'.'||  rec.p_table_name || ' WHERE ' || rec.p_column_name || ' = ''' || qry_value || '''';
                 /*pk_val :=getPrimaryColumn(sname,rec.c_table_name);*/
           c_pkey_w_cls := NULL;
           c_my_pkname := pKeys();
           --c_my_pkname :=getPrimaryColumn(sname,cur_children(i).c_table_name);
           BEGIN
           --dbms_output.put_line('Table Name is '|| cur_children(i).c_table_name);
           c_my_pkname := my_tblpKeys(cur_children(i).c_table_name);
           EXCEPTION WHEN OTHERS THEN
                BEGIN
                    c_my_pkname := my_tbluiKeys(cur_children(i).c_table_name);
                EXCEPTION WHEN OTHERS THEN
                    c_my_pkname := pKeys();
                END;
           END;
           --dbms_output.put_line('Count-' ||c_my_pkname.count);
           if c_my_pkname.count > 0 then
           FOR i IN c_my_pkname.first .. c_my_pkname.last
           LOOP
            --dbms_output.put_line(c_my_pkname(i).pk_col_name);
            IF c_pkey_w_cls IS NULL THEN
                  c_pkey_w_cls :=c_my_pkname(i).pk_col_name;
            ELSE
                  c_pkey_w_cls :=c_pkey_w_cls || ','||c_my_pkname(i).pk_col_name;
            END IF;
           END LOOP;
           end if;
             /*dbms_output.put_line('pk_val='||pk_val);*/

           IF c_pkey_w_cls IS NOT NULL THEN
            tmp_parent_qry := ' SELECT ' || c_pkey_w_cls || ' FROM ' ||sname ||'.'||  cur_children(i).c_table_name||sel_qry_where;
           ELSE
            tmp_parent_qry := NULL;
           END IF;
           --END IF;
       exception when value_error
       then
         --dbms_output.put_line('Foreign key is null');
         null;
       end;
       --dbms_output.put_line('tmp_parent_qry' || tmp_parent_qry);
     end loop;
     end if;

     if cons_nm is not null
     then
       if (sel_qry_where != 'where ')
       then
         --dbms_output.put_line('Adding to varray:'||t_nm||','||sel_qry_where);
         my_children.extend(1);
         select t_nm, sel_qry_where,tmp_parent_qry
         into my_children(cnt)
         from dual;
         cnt := cnt + 1;
       end if;
       and_string := null;
       sel_qry_where := 'where ';
       end if;

     if (my_children.COUNT <> 0) then
     --dbms_output.put_line('Table ' || tab_name || ' has ' || my_children.count || ' children');
     for i in my_children.first .. my_children.last
     loop
      qry:= ' SELECT COUNT(*) FROM '||sname ||'.'||my_children(i).tname||' ' ||my_children(i).w_cls;
      --dbms_output.put_line('Query '||i|| ' - ' ||qry );
      --begin
      execute immediate qry into val_cnt;
      --dbms_output.put_line('val_cnt' || val_cnt);
      --end;
     -- dbms_output.put_line('2');
      IF val_cnt<>0 THEN
           --select TDM_DELETE_S.nextval into pRunId from dual;
           delete_sequence_id := delete_sequence_id + 1;
           pRunId := delete_sequence_id;
           pRunId := to_char(delete_sequence) ||'_' ||to_char(delete_sequence_id);
           --dbms_output.put_line('Record' || my_children(i).tname || '-Delete from' || my_children(i).w_cls ||' -Select ' ||my_children(i).tmp_par_qry);
           INSERT INTO TMP_CHILD_RECORDS VALUES(pRunId,' DELETE FROM '||sname ||'.'||my_children(i).tname||' ' ||my_children(i).w_cls ,my_children(i).tname,'Y',qry_value,my_children(i).tmp_par_qry,sysdate,sname);
            delChildrenRows(sname,my_children(i).tname,my_children(i).w_cls,s_cls,'N',pRunId);
      END IF;
     end loop;
     end if;
    
    IF parent_flag='Y' THEN
     
    FOR delchildren in (SELECT DEL_QRY, run_id FROM TMP_CHILD_RECORDS WHERE SCHEMA_NAME=sname order by RUN_ID DESC)
    LOOP
      dbms_output.put_line(delchildren.run_id);
      dbms_output.put_line(delchildren.DEL_QRY);
      EXECUTE IMMEDIATE delchildren.DEL_QRY;
    END LOOP;
    end if;
    /*OPEN C1(sname);
    LOOP
    FETCH C1 BULK COLLECT INTO rec_tab LIMIT 1000;
    EXIT WHEN rec_tab.COUNT() = 0;
    FOR i IN rec_tab.FIRST .. rec_tab.LAST
    loop
    --EXEC IMMEDIATE rec_tab(i);
    EXECute immediate 'select ''' || rec_tab(i) || ''' from dual';
    --dbms_output.put_line(rec_tab(i));
    END LOOP;
    CLOSE C1;*/
    exception /*when value_error then
    --dbms_output.put_line('Value Error in Process Children');
    err_msg:=SQLERRM ||'-'||tab_name;
     /*IF parent_flag='N' THEN
      EXECUTE IMMEDIATE 'UPDATE TMP_CHILD_RECORDS SET FLAG=''N''' ||' WHERE TABLE_NAME='''||TAB_NAME||'''' || ' and  SCHEMA_NAME= ''' ||sname||'''' ;
     --COMMIT;
     END IF;*/
      --INSERT INTO TMP_CHILD_RECORDS VALUES(1,err_msg,'TEST','T',qry_value,'test',sysdate,sname);
    /*when no_data_found then
        null;
      --dbms_output.put_line('no data found');*/
    when others then
    err_msg:=SQLERRM ||'-'||pRunId;
    err_num := SQLCODE;
    --dbms_output.put_line('Error Message' || SQLERRM || 'Error Code'|| SQLCODE);
    PKG_TESTDATA_ERR.insertError(seq_no,err_msg,'delchildrenrows');
    /*IF(err_num !=-01422) THEN
     INSERT INTO TDM_ERR VALUES(seq_no,'delchildrenrows()',substr(err_msg,1,500),sysdate,sysdate,'TDM','TDM');
     END IF;*/
  END;


  PROCEDURE PopulateAlternateKeys(source_schema_name IN VARCHAR2,target_schema_name IN VARCHAR2,flag IN VARCHAR2,seq_no IN VARCHAR2 default null) IS
    w_cls VARCHAR2(4000);
    pkey_w_cls VARCHAR2(4000);
    rec_value VARCHAR2(1000);
    row_cnt VARCHAR2(10);
    cnt_qry VARCHAR2(4000);
    TYPE tmp_pk is RECORD
     (
       pk_col_name VARCHAR2(100)
     );
     type pk_name is varray(100) of tmp_pk;
     my_pkname pk_name;
     cons_type VARCHAR2(1);
     cons_qry VARCHAR2(4000);
     table_value VARCHAR2(100);
     xml_value  VARCHAR2(100);
     tdm_key_id VARCHAR2(10);
     pkey_xml XMLTYPE;
     myCtx dbms_xmlgen.ctxHandle;
     cnt NUMBER(20);
     err_msg VARCHAR2(4000);
     data_id varchar2(100);
     date_validate VARCHAR2(10);
     date_format varchar2(50);
     rec_val_date DATE;
     err_num NUMBER;
     new_val VARCHAR2(100);
     rec_count_keys NUMBER;
     rec_count_rfrntl_keys NUMBER;
    -- tdm_key_id VARCHAR2(20);
    BEGIN
      SELECT value
      into date_format
      FROM v$nls_parameters WHERE parameter ='NLS_DATE_FORMAT';

      execute immediate 'alter session set nls_date_format=''DD-MON-YYYY HH24:MI:SS''';
        rec_count_keys := 0;
        rec_count_rfrntl_keys := 0;
        --dbms_output.put_line('Start Time - '|| systimestamp);

         /* outer loop that gets the DATA_ID,TNAME AND CRT_DTTM FROM TDM_DATA */

         FOR REC IN (SELECT DATA_ID,TNAME,crt_dttm, ENTITY_NAME
                     FROM TDM_DATA
                     WHERE SCHEMA_NAME=source_schema_name
                     and TNAME IN (SELECT DISTINCT(TNAME)
                                   FROM TDM_ALTERNATE_KEYS
                                   minus
                                   select mview_name from all_mviews where owner = target_schema_name)
                     ORDER BY DATA_ID ASC)
         LOOP
            BEGIN

            w_cls :=NULL;
            pkey_w_cls :=NULL;
            cnt :=0;
            cnt_qry :='SELECT COUNT(1) FROM '||target_schema_name||'.' ||REC.TNAME;
            data_id :=REC.DATA_ID;
            --dbms_output.put_line('cnt'|| cnt_qry);

            /* START 1
                a) For each table name get the corresponding col_name from Alternate Keys
               b) This block will get the column value from the TDM_DATA insrt_data xml field
               c) Consrtuct a where_clause based on the col_name derived from Alternate keys and column value from XML .
               d) Date Validation function will be called for each value derived from the insrt_data xml
            */
            --
            --dbms_output.put_line('Table Name ' || REC.TNAME);
            IF flag='Y' THEN

                FOR C1 IN (SELECT COL_NAME
                           FROM TDM_ALTERNATE_KEYS
                           WHERE NOT EXISTS (SELECT 1
                                             FROM TDM_RFRNTL_KEYS
                                             WHERE TNAME=REC.TNAME
                                             AND SCHEMA_NAME=source_schema_name)
                           AND TNAME=REC.TNAME)
                LOOP

                      select extractValue(INSRT_DATA,'/ROW/'||C1.COL_NAME) into rec_value FROM TDM_DATA where data_id=REC.DATA_ID;
--                       date_validate:= validateDate(rec_value);
                       rec_val_date :=NULL;
                       --dbms_output.put_line('date_validate='|| date_validate);
/*                    IF date_validate = 'YES' --AND rec.nodes !='' AND rec.nodes is NOT NULL
                    THEN
                            begin
                            rec_val_date  := to_char(SYSDATE - (TO_DATE(REC.crt_dttm,'DD-MM-YYYY HH24:MI:SS')-TO_DATE(rec_value,'DD-MON-YYYY HH24:MI:SS')),'DD-MON-YYYY HH24:MI:SS') ;
                            dbms_output.put_line('rec_val_date='|| rec_val_date);
                              EXCEPTION WHEN OTHERS THEN
                                rec_val_date  :=to_char('31-DEC-9999 00:00:00');
                             END;
                               rec_value :='to_date(''' || rec_val_date ||'''' ||','''||'DD-MON-YYYY HH24:MI:SS'||''''|| ')';
                    END IF;
  */
                    rec_value := replace(rec_value,'''','''''');
                     --dbms_output.put_line('rec_value'|| rec_value);
                     IF w_cls IS NULL THEN
                           IF rec_val_date IS NULL THEN
                             if rec_value is not null  then
                               w_cls :=' WHERE '||C1.COL_NAME ||' = '''||rec_value||'''' ;
                             else
                               w_cls :=' WHERE '||C1.COL_NAME ||' is null ' ;
                             end if;
                           ELSE
                             if rec_value is not null then
                               w_cls :=' WHERE '||C1.COL_NAME ||' = ' || rec_value ;
                             else
                               w_cls :=' WHERE '||C1.COL_NAME ||' is null ';
                             end if;
                           END IF;
                     ELSE
                           IF rec_val_date IS NULL THEN
                             if rec_value is not null then
                               w_cls := w_cls || ' AND '||C1.COL_NAME ||' = '''||rec_value||'''' ;
                             else
                               w_cls := w_cls || ' AND '||C1.COL_NAME ||' is null ' ;
                             end if;
                            ELSE
                              if rec_value is not null then
                                w_cls :=w_cls || ' AND '||C1.COL_NAME ||' = ' || rec_value ;
                              else
                                w_cls :=w_cls || ' AND '||C1.COL_NAME ||' is null ';
                              end if;
                           END IF;
                    END IF;

                END LOOP;

            END IF;

            IF FLAG='N' THEN
                FOR C1 IN (SELECT COL_NAME
                           FROM TDM_ALTERNATE_KEYS
                           WHERE  EXISTS (SELECT 1
                                          FROM TDM_RFRNTL_KEYS
                                          WHERE TNAME=REC.TNAME
                                          AND SCHEMA_NAME=source_schema_name)
                           AND TNAME=REC.TNAME)
                LOOP
                   --dbms_output.put_line('test');
                      select extractValue(INSRT_DATA,'/ROW/'||C1.COL_NAME) into rec_value FROM TDM_DATA where data_id=REC.DATA_ID;
  --                     date_validate:= validateDate(rec_value);
                       rec_val_date :=NULL;
                       --dbms_output.put_line('date_validate1='|| date_validate);
/*                    IF date_validate = 'YES' --AND rec.nodes !='' AND rec.nodes is NOT NULL
                    THEN
                            begin
                            rec_val_date  := to_char(SYSDATE - (TO_DATE(REC.crt_dttm,'DD-MM-YYYY HH24:MI:SS')-TO_DATE(rec_value,'DD-MON-YYYY HH24:MI:SS')),'DD-MON-YYYY HH24:MI:SS') ;
                            dbms_output.put_line('rec_val_date='|| rec_val_date);
                              EXCEPTION WHEN OTHERS THEN
                                rec_val_date  :=to_char('31-DEC-9999 00:00:00');
                             END;
                               rec_value :='to_date(''' || rec_val_date ||'''' ||','''||'DD-MON-YYYY HH24:MI:SS'||''''|| ')';
                    END IF;
  */
                     new_val :=NULL;
                     tdm_key_id :=NULL;
                     begin
                       SELECT KEY_ID into tdm_key_id FROM TDM_RFRNTL_KEYS WHERE TNAME=REC.TNAME AND COL_NAME=C1.COL_NAME AND COL_VALUE=rec_value and schema_name=source_schema_name;
                       --dbms_output.put_line('tdm_key_id='|| tdm_key_id);
                     exception when no_data_found then
                       tdm_key_id := null;
                     end;
                     IF tdm_key_id is NOT NULL THEN
                     --dbms_output.put_line('tdm_key_id1='|| tdm_key_id);
                        SELECT COL_NEW_VALUE into new_val FROM TDM_KEYS WHERE KEY_ID=tdm_key_id;
                        if new_val IS NOT NULL THEN
                            rec_value :=new_val;
                        END IF;
                     END IF;

                     rec_value := replace(rec_value,'''','''''');

                     --dbms_output.put_line(C1.COL_NAME ||' '||rec_value);

                     IF w_cls IS NULL THEN
                           IF rec_val_date IS NULL THEN
                             if rec_value is not null then
                               w_cls :=' WHERE '||C1.COL_NAME ||' = '''||rec_value||'''' ;
                             else
                               w_cls :=' WHERE '||C1.COL_NAME ||' is null ' ;
                             end if;
                           ELSE
                             if rec_value is not null then
                               w_cls :=' WHERE '||C1.COL_NAME ||' = ' || rec_value ;
                             else
                               w_cls :=' WHERE '||C1.COL_NAME ||' is null ';
                             end if;
                           END IF;
                     ELSE
                           IF rec_val_date IS NULL THEN
                             if rec_value is not null then
                               w_cls := w_cls || ' AND '||C1.COL_NAME ||' = '''||rec_value||'''' ;
                             else
                               w_cls := w_cls || ' AND '||C1.COL_NAME ||' is null ' ;
                             end if;
                            ELSE
                              if rec_value is not null then
                                w_cls :=w_cls || ' AND '||C1.COL_NAME ||' = ' || rec_value ;
                              else
                                w_cls :=w_cls || ' AND '||C1.COL_NAME ||' is null ';
                              end if;
                           END IF;
                    END IF;
                    --dbms_output.put_line('loop_w_cls'||w_cls);
                END LOOP;

            END IF;
            /* END 1 */

             /* START 2
               a) If the constructed where clause is not null run the count query along with the constructed where clause against traget schema.

            */

                IF w_cls IS NOT NULL THEN
                   --IF REC.TNAME = 'CTNT_GRP_T' THEN
                   --dbms_output.put_line('w_cls'|| w_cls);
                   --END IF;*/

                   cnt_qry :=cnt_qry || w_cls;
                    --dbms_output.put_line('cnt_qry'|| cnt_qry);
                    EXECUTE IMMEDIATE cnt_qry into row_cnt;
                    IF row_cnt=1 THEN
                        cons_type :='P';
                       -- cons_qry :='SELECT COLUMN_NAME  FROM ALL_CONS_COLUMNS WHERE CONSTRAINT_NAME =(select CONSTRAINT_NAME from all_constraints where OWNER=''' ||target_schema_name ||'''' ||' AND TABLE_NAME=''' || REC.TNAME || ''' AND CONSTRAINT_TYPE=''' || cons_type ||'''' ||') AND OWNER='''||target_schema_name||'''';
                        my_pkname := pk_name();
                        FOR c1_pkey in (select extractvalue(t2.COLUMN_VALUE,'/*') nodevalues
                                        from xmltable('/ROWSET/ROW/COLUMN_NAME' passing (select pkey_data from tdm_data WHERE DATA_ID=REC.DATA_ID)) t2)
                        LOOP
                           --dbms_output.put_line('c1_pkey.nodevalues'||c1_pkey.nodevalues);
                           cnt :=cnt+1;
                             IF pkey_w_cls IS NULL THEN
                                    pkey_w_cls :=c1_pkey.nodevalues;
                            ELSE
                                    pkey_w_cls :=pkey_w_cls || ','||c1_pkey.nodevalues;
                            END IF;
                           my_pkname.extend(1);
                           select c1_pkey.nodevalues
                             into my_pkname(cnt)
                            from dual;

                           --my_pkname(cnt) :=''||c1_pkey.nodevalues;
                        END LOOP;
                        --dbms_output.put_line('pkey_w_cls' || pkey_w_cls);
                         --dbms_output.put_line('QRY=='|| 'SELECT '|| pkey_w_cls || ' FROM ' || target_schema_name||'.'  ||REC.TNAME || ' '||w_cls);

                        myCtx := dbms_xmlgen.newContext('SELECT '|| pkey_w_cls || ' FROM ' || target_schema_name||'.' ||REC.TNAME || ' '||w_cls);
                        pkey_xml := XmlType(dbms_xmlgen.getXML(myCtx));

                        dbms_xmlgen.setNullHandling(myCtx , dbms_xmlgen.EMPTY_TAG );
                        dbms_xmlgen.closecontext(myCtx);
                        --dbms_output.put_line('pkey_xml' || pkey_xml.getstringval());
                       -- ctx := dbms_xmlgen.newContext(
                        --EXECUTE IMMEDIATE cons_qry BULK COLLECT INTO my_pkname;
                         FOR i IN my_pkname.first .. my_pkname.last
                           LOOP
                               -- EXECUTE IMMEDIATE 'SELECT '||my_pkname(i).pk_col_name || ' FROM ' ||REC.TNAME || ' '||w_cls INTO table_value;
                               --dbms_output.put_line('pk_col_name' || my_pkname(i).pk_col_name);
                                select extractValue(pkey_xml,'/ROWSET/ROW/' || my_pkname(i).pk_col_name) INTO table_value from dual;
                                 --dbms_output.put_line('table_value' ||table_value);
                                select extractValue(INSRT_DATA,'ROW/'||my_pkname(i).pk_col_name) into xml_value from tdm_data where data_id=REC.DATA_ID;
                                --dbms_output.put_line('xml_value' ||xml_value);
                                --IF FLAG = 'Y' THEN
                                    BEGIN
                                         SELECT KEY_ID INTO tdm_key_id  FROM TDM_KEYS WHERE TNAME=REC.TNAME AND SCHEMA_NAME=source_schema_name AND COL_NAME=my_pkname(i).pk_col_name AND COL_VALUE=xml_value;
                                         rec_count_keys := rec_count_keys + 1;
                                    EXCEPTION when no_data_found
                                    then
                                         SELECT KEY_ID INTO tdm_key_id FROM TDM_RFRNTL_KEYS WHERE TNAME=REC.TNAME AND COL_NAME=my_pkname(i).pk_col_name AND COL_VALUE=xml_value AND SCHEMA_NAME=source_schema_name;
                                         rec_count_rfrntl_keys := rec_count_rfrntl_keys + 1;
                                    END;
                                /*IF REC.TNAME = 'CTNT_GRP_T' THEN
                                    dbms_output.put_line('table_value'|| table_value || ' Key Id ' ||tdm_key_id);
                                end if;*/
                                --dbms_output.put_line('TDM Key Id ' ||tdm_key_id || ' table_value'|| table_value);
                                EXECUTE IMMEDIATE 'UPDATE TDM_KEYS SET col_new_value ='''||table_value||'''' ||' WHERE KEY_ID='''||tdm_key_id||''' and col_new_value is null';
                                --commit;
                                --END IF;
                           END LOOP;
                    ELSIF row_cnt>1 THEN
                         --dbms_output.put_line('PopulateAlternateKeys: More than one row returned. Data Id: '||rec.data_id);
                         --dbms_output.put_line('populateAlternateKeys Rec Data Id'||rec.data_id ||'Error Message' ||SQLERRM);
                         err_msg := 'Data Id'||rec.data_id ||'Error Message' ||SQLERRM;
                         IF (REC.TNAME = 'CTLG_ITM_T' AND REC.ENTITY_NAME = 'CATEGORY') THEN
                         --INSERT INTO TDM_ERR VALUES (seq_no,'populateAlternateKeys',REC.DATA_ID || '=More than one row returned. ',sysdate,sysdate,'TDM','TDM');
                         PKG_TESTDATA_ERR.insertError(null,err_msg,'populateAlternateKeys');
                         ELSE
                         PKG_TESTDATA_ERR.insertError(null,err_msg,'populateAlternateKeys');
                         END IF;
                    END IF;


                END IF;
                EXCEPTION WHEN OTHERS THEN
                err_msg := 'data_id='||REC.data_id||' Exception= '||SQLERRM;
                --dbms_output.put_line('populateAlternateKeys '||err_msg);
                --INSERT INTO TDM_ERR VALUES (seq_no,'populateAlternateKeys',SUBSTR(err_msg,1,500),sysdate,sysdate,'TDM','TDM');
                PKG_TESTDATA_ERR.insertError(seq_no,err_msg,'populateAlternateKeys');

              END;
         END LOOP;
         execute immediate 'alter session set nls_date_format='''||date_format||'''';

         --dbms_output.put_line(TARGET_SCHEMA_NAME || ' - rec_count_keys - '||rec_count_keys);
         --dbms_output.put_line(TARGET_SCHEMA_NAME || ' - rec_count_rfrntl_keys' || rec_count_rfrntl_keys);
         --dbms_output.put_line('End Time - '|| systimestamp);
          IF flag='Y' THEN
             PopulateAlternateKeys(SOURCE_SCHEMA_NAME,TARGET_SCHEMA_NAME,'N',seq_no);
          END IF;
          --COMMIT;
         EXCEPTION WHEN OTHERS THEN
         err_num:= SQLCODE;
         err_msg := 'data_id='||data_id||' Exception= '||SQLERRM;
         IF err_num!=01422 THEN
            --dbms_output.put_line('populateAlternateKeys-O'||SQLERRM);
            --INSERT INTO TDM_ERR VALUES (seq_no,'populateAlternateKeys-O',SUBSTR(err_msg,1,500),sysdate,sysdate,'TDM','TDM');
            PKG_TESTDATA_ERR.insertError(seq_no,err_msg,'populateAlternateKeys');
            --COMMIT;
         END IF;
    END;
    
  END pkg_testdata_save;
/
