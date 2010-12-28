CREATE OR REPLACE PACKAGE BODY TDM_USER."PKG_TESTDATA_GEN" AS

     --* Global Variables
/*     l_enqueue_options     DBMS_AQ.enqueue_options_t;
     l_dequeue_options     dbms_aq.dequeue_options_t;
     l_message_properties  DBMS_AQ.message_properties_t;
     l_message_handle      RAW(16);
*/
     no_messages             EXCEPTION;
     PRAGMA EXCEPTION_INIT   (no_messages, -25228);

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
     --return_val VARCHAR2(100);
     tab_name VARCHAR2(200);
     pkey_str VARCHAR2(4000);
     pkey_xml XMLTYPE;
     BEGIN

         cons_type :='P';
         my_pkname := pKeys();
         str :='SELECT TABLE_NAME, COLUMN_NAME, POSITION FROM ALL_CONS_COLUMNS WHERE CONSTRAINT_NAME in 
         (select CONSTRAINT_NAME from all_constraints where OWNER=''' ||sname || ''' 
         AND CONSTRAINT_TYPE=''' || cons_type ||'''' ||') AND OWNER='''||sname||''' order by table_name, position';
         EXECUTE IMMEDIATE str BULK COLLECT INTO my_pkname;
         tab_name := my_pkname(1).table_name;
         my_pknameSplit := pKeys();
         cnt := 0;
         FOR i IN my_pkname.first .. my_pkname.last
         LOOP
         --dbms_output.put_line(my_pkname(i).table_name || '-' || my_pkname(i).pk_col_name || '-'|| my_pkname(i).col_pos);
         if tab_name = my_pkname(i).table_name then
            my_pknameSplit.extend(1);
            cnt := cnt + 1;
            my_pknameSplit(cnt) := my_pkname(i);
           else
            pkey_str := null;
            for ii in my_pknameSplit.first .. my_pknameSplit.last
            loop
                if pkey_str is null then
                    pkey_str := '<ROWSET><ROW><COLUMN_NAME>' || my_pknameSplit(ii).pk_col_name|| '</COLUMN_NAME>';
                    pkey_str := pkey_str || '<POSITION>' || my_pknameSplit(ii).col_pos || '</POSITION></ROW>';
                else
                    pkey_str := pkey_str || '<ROW><COLUMN_NAME>' || my_pknameSplit(ii).pk_col_name|| '</COLUMN_NAME>';
                    pkey_str := pkey_str || '<POSITION>' || my_pknameSplit(ii).col_pos || '</POSITION></ROW>';
                end if;
            end loop;
            pkey_str := pkey_str || '</ROWSET>';
            --dbms_output.put_line(pkey_str);
            pkey_xml := XMLTYPE(pkey_str);
            my_tblpKeys(sname || '.' || tab_name) := pkey_xml;
            tab_name := my_pkname(i).table_name;
            my_pknameSplit := pKeys();
            my_pknameSplit.extend(1);
            cnt := 1;
            my_pknameSplit(1) := my_pkname(i);
         end if;
         END LOOP;
         pkey_str := null;
         for ii in my_pknameSplit.first .. my_pknameSplit.last
         loop
            if pkey_str is null then
                pkey_str := '<ROWSET><ROW><COLUMN_NAME>' || my_pknameSplit(ii).pk_col_name|| '</COLUMN_NAME>';
                pkey_str := pkey_str || '<POSITION>' || my_pknameSplit(ii).col_pos || '</POSITION></ROW>';
            else
                pkey_str := pkey_str || '<ROW><COLUMN_NAME>' || my_pknameSplit(ii).pk_col_name|| '</COLUMN_NAME>';
                pkey_str := pkey_str || '<POSITION>' || my_pknameSplit(ii).col_pos || '</POSITION></ROW>';
            end if; 
         end loop;
         pkey_str := pkey_str || '</ROWSET>';
         --dbms_output.put_line(pkey_str);
         pkey_xml := XMLTYPE(pkey_str);
         my_tblpKeys(sname || '.' || tab_name) := pkey_xml;
         index_type := 'UNIQUE';
        --dbms_output.put_line('Again' || tab_name);
         str :='SELECT TABLE_NAME, COLUMN_NAME, COLUMN_POSITION  FROM ALL_IND_COLUMNS WHERE INDEX_NAME IN
         (select INDEX_NAME from all_indexes where OWNER=''' ||sname ||''' AND uniqueness=''' || index_type ||'''' ||') AND INDEX_OWNER='''||sname||''' order by table_name, COLUMN_POSITION';
         EXECUTE IMMEDIATE str BULK COLLECT INTO my_uiname;
         tab_name := my_uiname(1).table_name;
         my_uinameSplit := pKeys();
         cnt := 0;
         FOR i IN my_uiname.first .. my_uiname.last
         LOOP
         --dbms_output.put_line(my_uiname(i).constraint_name || '-' || my_uiname(i).p_table_name || '-'|| my_uiname(i).c_table_name);
         if tab_name = my_uiname(i).table_name then
            my_uinameSplit.extend(1);
            cnt := cnt + 1;
            my_uinameSplit(cnt) := my_uiname(i);
           else
            pkey_str := null;
            for ii in my_uinameSplit.first .. my_uinameSplit.last
            loop
                 if pkey_str is null then
                    pkey_str := '<ROWSET><ROW><COLUMN_NAME>' || my_uinameSplit(ii).pk_col_name|| '</COLUMN_NAME>';
                    pkey_str := pkey_str || '<POSITION>' || my_uinameSplit(ii).col_pos || '</POSITION></ROW>';
                 else
                    pkey_str := pkey_str || '<ROW><COLUMN_NAME>' || my_uinameSplit(ii).pk_col_name|| '</COLUMN_NAME>';
                    pkey_str := pkey_str || '<POSITION>' || my_uinameSplit(ii).col_pos || '</POSITION></ROW>';
                 end if;   
            end loop;
            pkey_str := pkey_str || '</ROWSET>';
            --dbms_output.put_line(pkey_str);
            pkey_xml := XMLTYPE(pkey_str);
            my_tbluiKeys(sname || '.' || tab_name) := pkey_xml;
            tab_name := my_uiname(i).table_name;
            my_uinameSplit := pKeys();
            my_uinameSplit.extend(1);
            cnt := 1;
            my_uinameSplit(1) := my_uiname(i);
         end if;
         END LOOP;
         pkey_str := null;
         for ii in my_uinameSplit.first .. my_uinameSplit.last
         loop
            if pkey_str is null then
             pkey_str := '<ROWSET><ROW><COLUMN_NAME>' || my_uinameSplit(ii).pk_col_name|| '</COLUMN_NAME>';
             pkey_str := pkey_str || '<POSITION>' || my_uinameSplit(ii).col_pos || '</POSITION></ROW>';
            else  
             pkey_str := pkey_str || '<ROW><COLUMN_NAME>' || my_uinameSplit(ii).pk_col_name|| '</COLUMN_NAME>';
             pkey_str := pkey_str || '<POSITION>' || my_uinameSplit(ii).col_pos || '</POSITION></ROW>';
            end if;
         end loop;
         pkey_str := pkey_str || '</ROWSET>';
         --dbms_output.put_line(pkey_str);
         pkey_xml := XMLTYPE(pkey_str);
         my_tbluiKeys(sname || '.' || tab_name) := pkey_xml;
         my_tbluiKeys(sname || '.' || tab_name) := pkey_xml;
         
         --dbms_output.put_line('MCMBRWIP.CTLG_ITM_T');
         --dbms_output.put_line(my_tblpKeys('MCMBRWIP.CTLG_ITM_T').getstringval());
     END;
  
  PROCEDURE saveChildRelationships(sname in VARCHAR2, ename in VARCHAR2) IS
       my_vRecChld vRecChld;
       vRecChldSplit vRecChld;
       str VARCHAR2(4000);
       tab_name VARCHAR2(200);
       cnt NUMBER;
       
       my_vRecChld_t vRecChld;

    BEGIN
    str := '     select rc.constraint_name, c.table_name c_table_name, c.column_name c_column_name, c.owner c_schema_name,
            p.table_name p_table_name, p.column_name p_column_name, c.position, p.owner p_schema_name
     from all_constraints rc, all_cons_columns c, all_cons_columns p
     where rc.constraint_type = ''R''
     and   rc.owner = ''' || sname || '''
     and   c.owner = ''' || sname || '''
     and   p.owner = ''' || sname || '''
     and   p.constraint_name = rc.r_constraint_name
     and   c.constraint_name = rc.constraint_name
     and   c.position = p.position
     and   not exists ( select 1
                        from tdm_ignr_cnstrnts ic
                        where ic.tname = p.table_name
                        and ic.constraint_name = rc.constraint_name
                        and nvl(schema_name, ''' || sname || ''') = ''' || sname || '''
                        and nvl(entity_name, ''' || ename || ''') = ''' || ename || ''' )
     union select constraint_name, table_name, column_name, nvl(owner,''' || sname || '''), R_TABLE_NAME, R_COLUMN_NAME, position, nvl(r_owner,''' || sname || ''')
     from tdm_referential_constraints cons
     where nvl(r_owner,''' || sname || ''') = ''' || sname || '''
     and nvl(entity_name, ''' || ename || ''') = ''' || ename || '''
     and   not exists ( select 1
                        from tdm_ignr_cnstrnts ic
                        where ic.tname = r_table_name
                        and ic.constraint_name = cons.constraint_name
                        and nvl(schema_name, ''' || sname || ''') = ''' || sname || '''
                        and nvl(entity_name, ''' || ename || ''') = ''' || ename || ''' )
     order by 5,1,6';                        
        
        EXECUTE IMMEDIATE str BULK COLLECT INTO my_vRecChld;
        tab_name := my_vRecChld(1).p_table_name;
        vRecChldSplit := vRecChld();
        cnt := 0;
        --dbms_output.put_line('Hello');
        FOR i IN my_vRecChld.first .. my_vRecChld.last
        LOOP
         --dbms_output.put_line(my_vRecChld(i).constraint_name || '-' || my_vRecChld(i).p_table_name || '-'|| my_vRecChld(i).c_table_name);
         if tab_name = my_vRecChld(i).p_table_name then
            vRecChldSplit.extend(1);
            cnt := cnt + 1;
            vRecChldSplit(cnt) := my_vRecChld(i);
           else
            my_tblRecChld(sname || '.'||tab_name ||'.' ||ename) := vRecChldSplit;
            tab_name := my_vRecChld(i).p_table_name;
            vRecChldSplit := vRecChld();
            vRecChldSplit.extend(1);
            cnt := 1;
            vRecChldSplit(1) := my_vRecChld(i);
         end if;
        END LOOP;
        my_tblRecChld(sname || '.'||tab_name ||'.' ||ename) := vRecChldSplit;
        
        /*my_vRecChld_t := vRecChld();
        my_vRecChld_t := my_tblRecChld('MCMBRWIP.CTLG_ITM_T.PRODUCT');
        dbms_output.put_line(my_vRecChld_t.count);
        for iii in my_vRecChld_t.first .. my_vRecChld_t.last
        loop
             dbms_output.put_line(my_vRecChld_t(iii).constraint_name);
             dbms_output.put_line(my_vRecChld_t(iii).c_table_name);
             dbms_output.put_line(my_vRecChld_t(iii).c_column_name);
             dbms_output.put_line(my_vRecChld_t(iii).c_schema_name);
             dbms_output.put_line(my_vRecChld_t(iii).p_table_name);
             dbms_output.put_line(my_vRecChld_t(iii).p_column_name);
             dbms_output.put_line(my_vRecChld_t(iii).position);
             dbms_output.put_line(my_vRecChld_t(iii).p_schema_name);
             dbms_output.put_line('--------------------------------------------');
        end loop;*/
        
    END;
    
    PROCEDURE saveParentRelationships(sname in VARCHAR2, ename in VARCHAR2) IS
       my_vRecParent vRecParent;
       vRecParentSplit vRecParent;
       str VARCHAR2(4000);
       tab_name VARCHAR2(200);
       cnt NUMBER;
       my_vRecParent_t vRecParent;
    BEGIN
    str := 'select rc.constraint_name, c.table_name c_table_name, c.column_name c_column_name, c.owner c_schema_name,
            p.table_name p_table_name, p.column_name p_column_name, p.position, p.owner p_schema_name
     from all_constraints rc, all_cons_columns c, all_cons_columns p
     where rc.constraint_type = ''R''
     and   rc.owner = ''' || sname || '''
     and   c.owner = ''' || sname || '''
     and   p.owner = ''' || sname || '''
     and   c.constraint_name = rc.constraint_name
     and   p.constraint_name = rc.r_constraint_name
     and   c.position = p.position
     and   not exists ( select 1
                        from tdm_ignr_cnstrnts ic
                        where ic.tname = rc.table_name
                        and ic.constraint_name = rc.constraint_name
                        and nvl(schema_name, ''' || sname || ''') = ''' || sname || '''
                        and nvl(entity_name, ''' || ename || ''') = ''' || ename || ''')
     union select constraint_name, table_name, column_name, nvl(owner,''' || sname || '''), R_TABLE_NAME, R_COLUMN_NAME, position, nvl(r_owner,''' || sname || ''')
     from tdm_referential_constraints cons
     where nvl(owner,''' || sname || ''') = ''' || sname || '''
     and   nvl(entity_name,''' || ename || ''') = ''' || ename || '''
     and   not exists ( select 1
                        from tdm_ignr_cnstrnts ic
                        where ic.tname = table_name
                        and ic.constraint_name = cons.constraint_name
                        and nvl(schema_name, ''' || sname || ''') = ''' || sname || '''
                        and nvl(entity_name, ''' || ename || ''') = ''' || ename || ''')

     order by 2,1,6';                        
        
        EXECUTE IMMEDIATE str BULK COLLECT INTO my_vRecParent;
        tab_name := my_vRecParent(1).c_table_name;
        vRecParentSplit := vRecParent();
        cnt := 0;
        --dbms_output.put_line('Hello');
        FOR i IN my_vRecParent.first .. my_vRecParent.last
        LOOP
         --dbms_output.put_line(my_vRecParent(i).constraint_name || '-' || my_vRecParent(i).c_table_name || '-'|| my_vRecParent(i).p_table_name);
         if tab_name = my_vRecParent(i).c_table_name then
            vRecParentSplit.extend(1);
            cnt := cnt + 1;
            vRecParentSplit(cnt) := my_vRecParent(i);
           else
            my_tblRecParent(sname || '.'||tab_name ||'.' ||ename) := vRecParentSplit;
            tab_name := my_vRecParent(i).c_table_name;
            vRecParentSplit := vRecParent();
            vRecParentSplit.extend(1);
            cnt := 1;
            vRecParentSplit(1) := my_vRecParent(i);
         end if;
        END LOOP;
        my_tblRecParent(sname || '.'||tab_name ||'.' ||ename) := vRecParentSplit;

        /*my_vRecParent_t := vRecParent();
        my_vRecParent_t := my_tblRecParent('MCMBRWIP.CTLG_ITM_T.PRODUCT');
        dbms_output.put_line(my_vRecParent_t.count);
        for iii in my_vRecParent_t.first .. my_vRecParent_t.last
        loop
             dbms_output.put_line(my_vRecParent_t(iii).constraint_name);
             dbms_output.put_line(my_vRecParent_t(iii).c_table_name);
             dbms_output.put_line(my_vRecParent_t(iii).c_column_name);
             dbms_output.put_line(my_vRecParent_t(iii).c_schema_name);
             dbms_output.put_line(my_vRecParent_t(iii).p_table_name);
             dbms_output.put_line(my_vRecParent_t(iii).p_column_name);
             dbms_output.put_line(my_vRecParent_t(iii).position);
             dbms_output.put_line(my_vRecParent_t(iii).p_schema_name);
             dbms_output.put_line('--------------------------------------------');
        end loop;*/
        
    END;
    
FUNCTION getCols(sname IN VARCHAR2, tab_name IN VARCHAR2) RETURN VARCHAR2 IS
    CURSOR tab_col_cursor(tname VARCHAR2, sname varchar2) is
      select column_name
        from all_tab_columns
        where owner = sname and table_name = tname order by tname;
    col_list VARCHAR2(4000);

  BEGIN
    --trace.it ( '-> getCols('||sname||','|| tab_name||')');
    for c1 in tab_col_cursor(tab_name, sname)
    loop
      col_list := col_list || ',' || c1.column_name;
    end loop;
    --trace.it ( 'returning '|| substr(col_list,2) );
    return substr(col_list,2);
  END;

  /*PROCEDURE enqueueData ( p_rec IN tdm_q_type ) IS
  BEGIN

     --* Enqueue the message
     dbms_aq.enqueue ( queue_name          => 'tdm_q',
                       enqueue_options     => l_enqueue_options,
                       message_properties  => l_message_properties,
                       payload             => p_rec,
                       msgid               => l_message_handle
                     );

  END enqueueData;

  PROCEDURE kill IS
     l_tdm_rec    tdm_q_type;
  BEGIN

     l_tdm_rec := tdm_q_type ( null, null, null, null, 'Y' );
     enqueueData ( l_tdm_rec );
     commit;

  END kill;
*/
  PROCEDURE extractData(sname IN varchar2, tab_name IN VARCHAR2, where_clause IN VARCHAR2, ename IN VARCHAR2) IS

     date_format varchar2(50);

     --* Local variables for enqueueing
     --l_tdm_rec    tdm_q_type;
     n_tdm_count  PLS_INTEGER := 0;
     err_seq_no number;
  BEGIN

      --trace.start_trace ( 'TDM','TDM' );
      --trace.it ( '-> extractData('||sname||','||tab_name||','||where_clause||','||ename);
      SELECT value
      into date_format
      FROM v$nls_parameters WHERE parameter ='NLS_DATE_FORMAT';

      execute immediate 'alter session set nls_date_format=''DD-MON-YYYY HH24:MI:SS''';

      /* create payload
      l_tdm_rec := tdm_q_type ( sname, tab_name, where_clause, ename, NULL );
      enqueueData ( l_tdm_rec );
      */
       
      err_seq_no := PKG_TESTDATA_ERR.getErrSeqNo;
      saveInserts(sname,tab_name,where_clause,ename,'Y',null,err_seq_no);
      /*begin
        clean_keys(null);
      exception
      when others then
        null;
      end;*/

      commit;
      execute immediate 'alter session set nls_date_format='''||date_format||'''';
      --trace.it ( '<- extractData');
--      trace.stop_trace;

  END;
/*
   PROCEDURE dequeueData IS

     l_tdm_rec    tdm_q_type;

   BEGIN

     l_dequeue_options.wait := 3;

     --trace.it ( 'starting listening on queue' );
     trace.start_trace ( 'TMDeq', 'TDMDEQ' );
     LOOP

       BEGIN

         DBMS_AQ.dequeue ( queue_name          => 'tdm_q',
                           dequeue_options     => l_dequeue_options,
                           message_properties  => l_message_properties,
                           payload             => l_tdm_rec,
                           msgid               => l_message_handle
                         );

         IF ( l_tdm_rec.kill_flag = 'Y' ) THEN
           --trace.it ( 'Dequeue terminated!' );
           EXIT;
         END IF;

         --trace.it ( '=======>>>  message dequeued  <<<=======' );
         saveInserts(l_tdm_rec.sname, l_tdm_rec.tab_name, l_tdm_rec.where_clause, l_tdm_rec.ename,'N');
/ *
         BEGIN
           clean_keys(l_tdm_rec.sname);
         EXCEPTION
           WHEN OTHERS THEN
             NULL;
         END;
* /
         COMMIT;

       EXCEPTION
         WHEN no_messages THEN
           NULL;

       END;

     END LOOP;
     trace.stop_trace;

   END dequeueData;

*/
   PROCEDURE saveInserts(sname IN varchar2, tab_name IN VARCHAR2,
                         where_clause IN VARCHAR2, ename IN VARCHAR2,data_flag VARCHAR2,
                         calling_tab_name VARCHAR2 default null, err_seqno IN VARCHAR2 default null) IS

     pri_key xmltype;
     table_rule varchar2(4000);
     my_where_clause varchar2(4000);
     my_qual_clause_parent varchar2(4000);
     my_qual_clause_child varchar2(4000);


     type rec_insrt_data is RECORD (
     qry TDM_DATA.qry%type,
     tname TDM_DATA.tname%type,
     insrt_data TDM_DATA.insrt_data%type,
     crt_dttm TDM_DATA.crt_dttm%type,
     lst_updt_dttm TDM_DATA.lst_updt_dttm%type,
     crt_user_id TDM_DATA.crt_user_id%type,
     lst_updt_user_id TDM_DATA.lst_updt_user_id%type,
     process_child varchar2(1),
     process_parent varchar2(1)
     );

     type my_recs is varray(600) of rec_insrt_data;
     v_insrt_recs my_recs;
     type t_insrt_data is REF CURSOR;
     cur_insrt_data t_insrt_data;
     test_cursor t_insrt_data;
     cnt_fetched number;

     insrt_qry varchar2(4000);
     s_pri_key varchar2(4000);
     tmp_number number;
     my_key_id number(9);

     myCtx dbms_xmlgen.ctxHandle;
     strt_time date;
     end_time date;
     flag varchar2(10);

   BEGIN
     --trace.it ( '->saveInserts('||sname||','||tab_name||','||where_clause||','||ename);
     strt_time := sysdate;
     IF data_flag='Y' THEN
        flag :='Y';
     ELSE
       flag:=NULL;
     END IF;
      BEGIN

      tmp_number := 0;
     /* Find if any default where clauses have to be added from tdm_user_tables.
     This is a mechanism to restrict the recursive calls and avoid getting all rows */
      declare
        cursor c_table_rules is
        select and_clause, decode (nvl(process_children,'Y'), 'N', nvl(qual_clause_children,'1=1'),'1=2') qual_clause_children,
               decode (nvl(process_parent,'Y'), 'N', nvl(qual_clause_parent,'1=1'),'1=2') qual_clause_parent
        from tdm_table_rules
        where tname = tab_name
        and nvl(schema_name,sname) = sname
        and nvl(entity_name,ename) = ename;
      begin
        my_where_clause := where_clause;
        my_qual_clause_parent := null;
        my_qual_clause_child  := null;
        --trace.it ( '  open table_rule_rec' );
        for table_rule_rec in c_table_rules
        loop
          if table_rule_rec.and_clause is not null
          then
            my_where_clause := my_where_clause || ' and '||table_rule_rec.and_clause;
          end if;
          my_qual_clause_parent := my_qual_clause_parent || ' and '||table_rule_rec.qual_clause_parent;
          my_qual_clause_child := my_qual_clause_child || ' and '||table_rule_rec.qual_clause_children;
        end loop;
        if my_qual_clause_parent is null /* Both parent and child will be null */
        then
          my_qual_clause_parent := '1=2';
          my_qual_clause_child := '1=2';
        else
          my_qual_clause_parent := '1=1'||my_qual_clause_parent;
          my_qual_clause_child := '1=1'||my_qual_clause_child;
        end if;
        --trace.it ( '  close table_rule_rec' );
      exception when others
      then
         --trace.it ( '  exception '|| SQLCODE );
         my_qual_clause_parent := '1=2';
         my_qual_clause_child  := '1=2';
         my_where_clause := where_clause;
      end;

--     myCtx := dbms_xmlgen.newcontext('select * from '||sname||'.'||tab_name||' '||my_where_clause);
--     dbms_xmlgen.setNullHandling(myCtx , dbms_xmlgen.EMPTY_TAG );

     --dbms_xmlgen.useNullAttributeIndicator(myCtx);

     /** Use the new where clause to find data for this table **/
     cnt_fetched := 0;
     begin
        open test_cursor for 'select count(1) from '||sname||'.'||tab_name||' '||my_where_clause;
        fetch test_cursor into cnt_fetched;
        close test_cursor;
        if (cnt_fetched = 0 ) then
          return;
        end if;
     exception 
     when others then
       return;
     end;
       
     --dbms_output.put_line('Final Where Clause:'||/* Formatted on 10/6/2010 3:32:47 PM (QP5 v5.115.810.9015) */
--my_where_clause||','||tab_name);

     insrt_qry := 'select '''||replace(my_where_clause,'''','''''')||''' qry, '''||tab_name||''' tname, COLUMN_VALUE insrt_data, '||
            'sysdate crt_dttm, sysdate lst_updt_dttm, ''TDM'' crt_user_id, ''TDM'' lst_updt_user_id, '||
            'case when ('||replace(my_qual_clause_child,'''','''''')||') then ''N'' else ''Y'' end process_child, '||
            'case when ('||replace(my_qual_clause_parent,'''','''''')||') then ''N'' else ''Y'' end process_parent '||
     'from TABLE(XMLSEQUENCE(extract(dbms_xmlgen.getXmlType('''|| 'select * from '||sname||'.'||tab_name||' '||replace(my_where_clause,'''','''''')||'''),''/ROWSET/ROW'')))';

     /*insrt_qry := 'select  '''||replace(my_where_clause,'''','''''')||''' qry, '''||tab_name||''' tname, SYS_XMLGEN(COLUMN_VALUE) insrt_data, '||
            'sysdate crt_dttm, sysdate lst_updt_dttm, ''TDM'' crt_user_id, ''TDM'' lst_updt_user_id, '||
            'case when ('||replace(my_qual_clause_child,'''','''''')||') then ''N'' else ''Y'' end process_child, '||
            'case when ('||replace(my_qual_clause_parent,'''','''''')||') then ''N'' else ''Y'' end process_parent '||
     'from XMLTABLE(''/ROWSET/ROW'' PASSING XMLType(dbms_xmlgen.getXML(:xmlCtx)'||')) x_tbl';
*/

     --dbms_output.put_line('Query: '||insrt_qry);
     --trace.it ( '  open cur_insrt_data' );
     begin
     open cur_insrt_data for insrt_qry;
     exception
       when others then
         close cur_insrt_data;
         PKG_TESTDATA_ERR.insertError(err_seqno,'Cannot execute query:'||insrt_qry,'SaveInsert');
         return;
     end;
     --dbms_output.put_line('After Query');

     /** NOTE: Only picking 400 records ... more will be ignored **/
     if tab_name <> 'PG_T' then
     fetch cur_insrt_data
     BULK COLLECT into v_insrt_recs limit 600;
     else
     fetch cur_insrt_data
     BULK COLLECT into v_insrt_recs limit 10;
     end if;
     --trace.it ( '   bulk collected' );
      --dbms_output.put_line('After Fetch');
     if v_insrt_recs.count > 0
     then
         /*if (calling_tab_name is not null)
         then
           dbms_output.put_line(calling_tab_name||' -> '||sname || '-'||tab_name);
         end if;*/
         begin
            pri_key := my_tblpKeys(sname||'.'||tab_name);
            --dbms_output.put_line('Primary Keys ' || pri_key.getstringval());
         exception when others then
            pri_key := my_tbluiKeys(sname||'.'||tab_name);
            --dbms_output.put_line('Unique Keys ' || pri_key.getstringval());
         end;
         /*begin
           select xml_primary_key
           into pri_key
           from tdm_primary_keys
           where schema_name = sname
           and table_name = tab_name;
         exception
           when no_data_found then
           begin
             --trace.it ( 'select xmltype from all constraints' );
             select XmlType(dbms_xmlgen.getXML('select cols.column_name, cols.position '||
             'from all_constraints cons, all_cons_columns cols '||
             'where cons.owner = '''||sname||''' and cons.owner = cols.owner '||
             'and   cons.table_name = '''||tab_name||''' '||
             'and   cons.constraint_type = ''P'' '||
             'and   cons.constraint_name = cols.constraint_name order by cols.position'))
             into pri_key
             from dual;
           exception
             /** To handle CTLG_TREE_PATH_T in the interim until Unique key is converted to primary key **/
         /*    when value_error then
             select XmlType(dbms_xmlgen.getXML('select cols.column_name, cols.column_position position '||
             'from all_indexes indx, all_ind_columns cols '||
             'where indx.owner = '''||sname||''' and indx.owner = cols.index_owner '||
             'and   indx.table_name = '''||tab_name||''' '||
             'and   indx.uniqueness = ''UNIQUE'' '||
             'and   indx.index_name = cols.index_name order by cols.column_position'))
             into pri_key
             from dual;
           end;
           
           begin
             --trace.it ( 'insert into tdm_primary_keys - '|| tab_name );
             insert into tdm_primary_keys (schema_name, table_name, xml_primary_key)
             values (sname, tab_name, pri_key);
           exception
             when others then
               --dbms_output.put_line('Primary Key for '||sname||'.'||tab_name||' already exists in tdm_primary_keys! ');
               null;
           end;
         end;*/

     end if;
     close cur_insrt_data;
     --trace.it ( '  closed cur_insrt_data' );
     --dbms_xmlgen.closecontext(myCtx);
     --dbms_output.put_line('Records Fetched: '||to_char(v_insrt_recs.count)||' First:'||to_char(v_insrt_recs.first)||' Last:'||to_char(v_insrt_recs.last));
     --trace.it ( '  loop through recs' );
     for i in v_insrt_recs.first .. v_insrt_recs.last
     loop
       s_pri_key := '';
       /**
         Find if the key has already been accounted for in TDM_KEYS. If use then use same id else create new id and row.
         Also keep track of row primary key for the table so that we dont call processParent and processChild again
       **/
       --dbms_output.put_line(''||sname||'.'||tab_name);
       declare
         cursor c1 is
         select column_name column_name, EXTRACTVALUE(v_insrt_recs(i).insrt_data, '/ROW/'||pri.column_name) col_value
           from XMLTABLE('/ROWSET/ROW' PASSING pri_key COLUMNS column_name varchar2(100), position varchar2(100)) pri
         order by position;
       begin
         --trace.it ( '   open c1' );
         for pri_rec in c1
         loop
           if pri_rec.col_value is not null
           then
             s_pri_key := s_pri_key || ' and ' || pri_rec.column_name||'='''||pri_rec.col_value||'''';
             --dbms_output.put_line('S_PRI_KEY: '||s_pri_key);
             --trace.it('S_PRI_KEY: '||s_pri_key);
             begin
               select key_id
               into my_key_id
               from tdm_keys
               where nvl(schema_name,sname) = sname
               and   tname = tab_name
               and   col_name = pri_rec.column_name
               and   col_value = pri_rec.col_value
               and rownum = 1;
               --trace.it ( 'got key '|| my_key_id );
             exception when no_data_found
             then
               select tdm_keys_s.nextval
               into my_key_id
               from dual;
               insert into tdm_keys(key_id, tname, col_name, col_value, schema_name, crt_dttm, lst_updt_dttm, crt_user_id,
                           lst_updt_user_id)
               values (my_key_id,tab_name,pri_rec.column_name, pri_rec.col_value,sname,sysdate,sysdate,'TDM','TDM');
               COMMIT;
             end;
           else
             s_pri_key := s_pri_key || ' and ' || pri_rec.column_name||' is null ';
           end if;
          end loop;
          --trace.it ( '   close c1' );
       end;

       /*
         Check if data has been processed before by using the stringified primary key where clause created above
       */
       --trace.it ( 'select count(1) from tdm_data' );
       select count(1)
       into tmp_number
       from tdm_data
       where tname = v_insrt_recs(i).tname
       and nvl(schema_name,sname) = sname
       and   pkey_string = s_pri_key;
       --dbms_output.put_line('Table:'||v_insrt_recs(i).tname||', Where Clause:'||s_pri_key||', Found in TDM_DATA:'||to_char(tmp_number));
  end_time := sysdate;
  --dbms_output.put_line('TNAME:'||tab_name||', WHERE_CLAUSE:'||where_clause);
  --dbms_output.put_line('SaveInsert executed in :'||to_char((end_time-strt_time)*3600*24,'99.99')||' secs.'||to_char(end_time)||'-'||to_char(strt_time));

       if tmp_number is not null and tmp_number = 0
       then
         --trace.it ( 'insert into tdm_data' );
         insert into tdm_data (data_id, qry, tname, insrt_data,
                           pkey_data, crt_dttm, lst_updt_dttm, crt_user_id,
                           lst_updt_user_id,pkey_string, schema_name,data_flag,entity_name)
         values (TDM_DATA_S.nextval, v_insrt_recs(i).qry, v_insrt_recs(i).tname, v_insrt_recs(i).insrt_data,
             pri_key, v_insrt_recs(i).crt_dttm, v_insrt_recs(i).lst_updt_dttm, v_insrt_recs(i).crt_user_id,
             v_insrt_recs(i).lst_updt_user_id,s_pri_key,sname,flag,ename);
         --dbms_output.put_line('processParents called for '||v_insrt_recs(i).tname||'. Processing Row '||to_char(i)||' of '||to_char(v_insrt_recs.count));
         commit;
         processParents(sname, v_insrt_recs(i).tname,v_insrt_recs(i).insrt_data,ename);
         if v_insrt_recs(i).process_child = 'Y'
         then
           --dbms_output.put_line('processChildren called for '||v_insrt_recs(i).tname||'. Processing Row '||to_char(i)||' of '||to_char(v_insrt_recs.count));
           processChildren(sname, v_insrt_recs(i).tname,v_insrt_recs(i).insrt_data, ename);
         else
           null;
           --dbms_output.put_line('processChildren not called for '||v_insrt_recs(i).tname||' because of TDM_TABLE_RULES. Processing Row '||to_char(i)||' of '||to_char(v_insrt_recs.count));
         end if;
       end if;
     end loop;
  exception when value_error then
    null;
    --dbms_output.put_line('Value Error');
     begin
       --trace.it ('VALUE_ERROR - close cur_instr_data' );
       close cur_insrt_data;
     exception when others then
       --trace.it ( 'OTHERS' );
       null;
     end;
     begin
       --trace.it ( 'closecontext(myCtx)' );
       dbms_xmlgen.closecontext(myCtx);
     exception when others then
  end_time := sysdate;
  --dbms_output.put_line('TNAME:'||tab_name||', WHERE_CLAUSE:'||where_clause);
  --dbms_output.put_line('SaveInsert executed in :'||to_char((end_time-strt_time)*3600*24,'99.99')||' secs.'||to_char(end_time)||'-'||to_char(strt_time));

       null;
     end;

  END;
  END;
    

  PROCEDURE processParents(sname IN VARCHAR2, tab_name IN VARCHAR2, doc IN xmlType, ename IN VARCHAR2) IS
    /*cursor cur_parents(sname varchar2, tname varchar2, ename varchar2) is
     select rc.constraint_name, c.table_name c_table_name, c.column_name c_column_name, c.owner c_schema_name,
            p.table_name p_table_name, p.column_name p_column_name, p.position, p.owner p_schema_name
     from all_constraints rc, all_cons_columns c, all_cons_columns p
     where rc.constraint_type = 'R'
     and   rc.table_name = tname
     and   rc.owner = sname
     and   c.owner = sname
     and   p.owner = sname
     and   c.constraint_name = rc.constraint_name
     and   p.constraint_name = rc.r_constraint_name
     and   c.position = p.position
     and   not exists ( select 1
                        from tdm_ignr_cnstrnts ic
                        where ic.tname = tab_name
                        and ic.constraint_name = rc.constraint_name
                        and nvl(schema_name, sname) = sname
                        and nvl(entity_name, ename) = ename)
     union select constraint_name, table_name, column_name, nvl(owner,sname), R_TABLE_NAME, R_COLUMN_NAME, position, nvl(r_owner,sname)
     from tdm_referential_constraints cons
     where nvl(owner,sname) = sname
     and   table_name = tname
     and   nvl(entity_name,ename) = ename
     and   not exists ( select 1
                        from tdm_ignr_cnstrnts ic
                        where ic.tname = tab_name
                        and ic.constraint_name = cons.constraint_name
                        and nvl(schema_name, sname) = sname
                        and nvl(entity_name, ename) = ename)

     order by 1,6;*/
--     order by rc.constraint_name,p.position;
     TYPE parents_rec is RECORD
     (
       tname varchar2(30),
       w_cls   varchar2(4000),
       sname varchar2(30)
     );
     cons_nm user_constraints.constraint_name%TYPE;
     sel_qry_where VARCHAR2(4000);
     t_nm user_constraints.table_name%type;
     s_nm varchar2(30);
     and_string varchar2(6);
     qry_value varchar2(100);
     type parents is varray(4000) of parents_rec;
     my_parents parents;
     cnt number;
     my_pkey_id number;
     my_rkey_id number;
     strt_time date;
     end_time date;
     cur_parents vRecParent;

   BEGIN
     --trace.it ( '-> processParent('||sname||','||tab_name||')' );
   BEGIN
     strt_time := sysdate;
     my_parents := parents();
     cons_nm := null;
     sel_qry_where := 'where ';
     s_nm := null;
     t_nm := null;
     and_string := null;
     cnt := 1;
     --trace.it ( '  open cur_parents' );
     cur_parents := vRecParent();
     --dbms_output.put_line('GG:'||sname ||'.'|| tab_name||'.'|| ename);
     begin
     cur_parents := my_tblRecParent(sname ||'.'|| tab_name||'.'|| ename);
     exception
       when no_data_found then
         return;
     end;
     for ii in cur_parents.first .. cur_parents.last
     --for rec in cur_parents(sname, tab_name, ename)
     loop
       my_pkey_id := null;
       my_rkey_id := null;
       --if (cons_nm is not null and cons_nm != rec.constraint_name)
       if (cons_nm is not null and cons_nm != cur_parents(ii).constraint_name)
       then
         /** Loop entered means - Processed all columns in a constraint **/
         if (sel_qry_where != 'where ')
         then
          --dbms_output.put_line('Parent row added in VARRAY FOR PROCESSING: Table: '||t_nm||' Where: '||sel_qry_where);
           /** Loop enetered means - that the foreign key is not null **/
           my_parents.extend(1);
           select t_nm, sel_qry_where, s_nm
           into my_parents(cnt)
           from dual;
           cnt := cnt + 1;
         end if;
         and_string := null;
         sel_qry_where := 'where ';
       end if;
       cons_nm := cur_parents(ii).constraint_name;
       t_nm := cur_parents(ii).p_table_name;
       s_nm := cur_parents(ii).p_schema_name;

       begin
         --trace.it ( '   extractValue '||rec.c_column_name );
         select EXTRACTVALUE(doc,'/ROW/'||cur_parents(ii).c_column_name)
         into qry_value
         from dual;
         if qry_value is not null
         then
           sel_qry_where := sel_qry_where || and_string ||cur_parents(ii).p_column_name||' = '''||qry_value||''' ';
           --trace.it ( '   sql_qry_where='|| sel_qry_where );
           and_string := ' and ';
           --dbms_output.put_line('extvl where:'||sel_qry_where);
           /**Primary Key **/
           begin
             --trace.it ( 'select key_id from tdm_keys' );
             select key_id
             into my_pkey_id
             from tdm_keys
             where nvl(schema_name,cur_parents(ii).p_schema_name) = cur_parents(ii).p_schema_name
             and   tname = cur_parents(ii).p_table_name
             and   col_name = cur_parents(ii).p_column_name
             and   col_value = qry_value
             and rownum = 1;
             --dbms_output.put_line('Primary Key found in TDM_KEY - Column: '||rec.p_column_name|| ', Value: '|| qry_value|| ' Id: '||my_pkey_id);

           exception when no_data_found
           then
             select tdm_keys_s.nextval
             into my_pkey_id
             from dual;
             --trace.it ( 'insert into tdm_keys' );
             insert into tdm_keys(key_id, tname, col_name, col_value, schema_name, crt_dttm, lst_updt_dttm, crt_user_id,
                           lst_updt_user_id)
             values (my_pkey_id,cur_parents(ii).p_table_name,cur_parents(ii).p_column_name, qry_value,cur_parents(ii).p_schema_name, sysdate,sysdate,'TDM','TDM');
             --dbms_output.put_line('Primary Key Inserted in TDM_KEY - Column: '||rec.p_column_name|| ', Value: '|| qry_value|| ' Id: '||my_pkey_id);
             COMMIT;
           
           end;
           /** Referential Key **/
           begin
             --trace.it ( 'select key_id from tdm_rfrntl_keys' );
             select key_id
             into my_rkey_id
             from tdm_rfrntl_keys
             where nvl(schema_name,cur_parents(ii).c_schema_name) = cur_parents(ii).c_schema_name
             and   tname = cur_parents(ii).c_table_name
             and   col_name = cur_parents(ii).c_column_name
             and   col_value = qry_value
             and rownum = 1;
           exception when no_data_found
           then
             select tdm_rfrntl_keys_s.nextval
             into my_rkey_id
             from dual;
             --trace.it ( 'insert into tdm_rfrntl_keys' );
             insert into tdm_rfrntl_keys(rkey_id, tname, col_name, col_value, key_id, schema_name, crt_dttm, lst_updt_dttm, crt_user_id,
                           lst_updt_user_id)
             values (my_rkey_id,cur_parents(ii).c_table_name,cur_parents(ii).c_column_name, qry_value, my_pkey_id, cur_parents(ii).c_schema_name, sysdate,sysdate,'TDM','TDM');
             COMMIT;
             --dbms_output.put_line('Referential Key found in TDM_KEY - Column: '||rec.c_column_name|| ', Value: '|| qry_value|| ' Id: '||my_rkey_id);
           end;
         end if;
      exception when value_error then
        --dbms_output.put_line('Foreign Key value is null');
        null;
      end;
     end loop;
     --trace.it ( 'end loop' );

     /** Do work for last constraint **/
     if (cons_nm is not null)
     then
       /** Loop entered means - Processed all columns in a constraint **/
       if (sel_qry_where != 'where ')
       then
         --dbms_output.put_line('Parent row added in VARRAY FOR PROCESSING: Table: '||t_nm||' Where: '||sel_qry_where);
         /** Loop enetered means - that the foreign key is not null **/
         my_parents.extend(1);
         select t_nm, sel_qry_where, s_nm
         into my_parents(cnt)
         from dual;
         cnt := cnt + 1;
       end if;
     end if;
    end_time := sysdate;
    --trace.it ( 'process parent executed in '|| to_char((end_time-strt_time)*3600*24,'99.99')||' secs');
    --dbms_output.put_line('Process Parent executed in :'||to_char((end_time-strt_time)*3600*24,'99.99')||' secs');
    --trace.it('Process Parent executed in :'||to_char((end_time-strt_time)*3600*24,'99.99')||' secs');
     /** If my_parents is null then the below code will throw value error and that is fine
         As there is nothing to process.
     **/
     --trace.it ( 'loop through my_parents('||my_parents.count||')');
     for i in my_parents.first .. my_parents.last
     loop
--       --dbms_output.put_line('SaveInsert Called:'||my_parents(i).tname||','||my_parents(i).w_cls);
       saveInserts(my_parents(i).sname, my_parents(i).tname, my_parents(i).w_cls, ename,'N',tab_name);
     end loop;
  exception when value_error then
    end_time := sysdate;
    --dbms_output.put_line('Process Parent executed in :'||to_char((end_time-strt_time)*3600*24,'99.99')||' secs');
    --dbms_output.put_line('Value Error in ProcessParent');
    --trace.it ( '<- processParent('||sname||','||tab_name||')' );
  END;

  END;

  PROCEDURE processChildren(sname IN VARCHAR2, tab_name IN VARCHAR2, doc IN xmlType, ename IN VARCHAR2) IS
    
/*     select constraint_name, c_table_name, c_column_name
          , p_table_name, p_column_name
       from tdm_cons_mv mv
      where p_table_name = tname
        and not exists ( select 1
                           from  tdm_ignr_cnstrnts ic
                        where ic.tname = tab_name
                        and ic.constraint_name = mv.constraint_name )
     order by constraint_name, p_position;
*/
     /*cursor cur_children(sname varchar2, tname varchar2, ename varchar2) is
     select rc.constraint_name, c.table_name c_table_name, c.column_name c_column_name, c.owner c_schema_name,
            p.table_name p_table_name, p.column_name p_column_name, c.position, p.owner p_schema_name
     from all_constraints rc, all_cons_columns c, all_cons_columns p
     where rc.constraint_type = 'R'
     and   rc.owner = sname
     and   c.owner = sname
     and   p.owner = sname
     and   p.constraint_name = rc.r_constraint_name
     and   p.table_name = tname
     and   c.constraint_name = rc.constraint_name
     and   c.position = p.position
     and   not exists ( select 1
                        from tdm_ignr_cnstrnts ic
                        where ic.tname = tab_name
                        and ic.constraint_name = rc.constraint_name
                        and nvl(schema_name, sname) = sname
                        and nvl(entity_name, ename) = ename )
     union select constraint_name, table_name, column_name, nvl(owner,sname), R_TABLE_NAME, R_COLUMN_NAME, position, nvl(r_owner,sname)
     from tdm_referential_constraints cons
     where nvl(r_owner,sname) = sname
     and   r_table_name = tname
     and nvl(entity_name, ename) = ename
     and   not exists ( select 1
                        from tdm_ignr_cnstrnts ic
                        where ic.tname = tab_name
                        and ic.constraint_name = cons.constraint_name
                        and nvl(schema_name, sname) = sname
                        and nvl(entity_name, ename) = ename )
     order by 1,6;*/
--     order by rc.constraint_name,p.position;

     TYPE childs_rec is RECORD
     (
       tname varchar2(30),
       w_cls   varchar2(4000),
       sname varchar2(30)
     );
     cons_nm user_constraints.constraint_name%TYPE;
     sel_qry_where VARCHAR2(4000);
     t_nm user_constraints.table_name%type;
     s_nm varchar2(30);
     and_string varchar2(6);
     qry_value varchar2(100);
     type children is varray(4000) of childs_rec;
     my_children children;
     cnt number;
     my_pkey_id number;
     my_rkey_id number;
     strt_time date;
     end_time date;
     cur_children vRecChld;

   BEGIN
     --trace.it ( '-> processChildren('||sname||','||tab_name||','||ename||')');
   BEGIN
     strt_time := sysdate;
     my_children := children();
     cons_nm := null;
     sel_qry_where := 'where ';
     t_nm := null;
     s_nm := null;
     and_string := null;
     cnt := 1;
     --trace.it ( 'loop through children' );
     
     cur_children := vRecChld();
     begin
     cur_children := my_tblRecChld(sname ||'.' ||tab_name||'.'||ename);
     exception
     when no_data_found then
         return;
     end;
     for ii in cur_children.first .. cur_children.last
     --for rec in cur_children(sname, tab_name, ename)
     loop
       --if (cons_nm is not null and cons_nm != rec.constraint_name)
       if (cons_nm is not null and cons_nm != cur_children(ii).constraint_name)
       then
         if (sel_qry_where != 'where ')
         then
           --dbms_output.put_line('Adding to varray:'||t_nm||','||sel_qry_where);
           my_children.extend(1);
           select t_nm, sel_qry_where, s_nm
           into my_children(cnt)
           from dual;
           cnt := cnt + 1;
         end if;
         and_string := null;
         sel_qry_where := 'where ';
       end if;
       cons_nm := cur_children(ii).constraint_name;
       t_nm := cur_children(ii).c_table_name;
       s_nm := cur_children(ii).c_schema_name;
       --dbms_output.put_line('Parent'||cur_children(ii).p_schema_name ||'-' || cur_children(ii).p_table_name||'-'||cur_children(ii).p_column_name);
       --dbms_output.put_line('Children'||cur_children(ii).c_schema_name ||'-' || cur_children(ii).c_table_name||'-'||cur_children(ii).c_column_name);

       begin
         --trace.it ( 'extractValue'|| rec.p_column_name );
         select EXTRACTVALUE(doc,'/ROW/'||cur_children(ii).p_column_name)
         into qry_value
         from dual;
         if qry_value is not null
         then
           sel_qry_where := sel_qry_where || and_string ||cur_children(ii).c_column_name||' = '''||qry_value||''' ';
           and_string := ' and ';
         end if;
         --dbms_output.put_line('Qry_value:'||qry_value||'. P_COLUMN_NAME:'||cur_children(ii).p_column_name||':'||sname||':'||tab_name||doc.extract('/ROW').getstringval());
           /**Primary Key **/
           begin
             --trace.it ( 'select key_id from tdm_keys' );
             select key_id
             into my_pkey_id
             from tdm_keys
             where nvl(schema_name,cur_children(ii).p_schema_name) = cur_children(ii).p_schema_name
             and   tname = cur_children(ii).p_table_name
             and   col_name = cur_children(ii).p_column_name
             and   col_value = qry_value
             and rownum = 1;
           exception when no_data_found
           then
             /* This shd never happen for ProcessChildren **/
             select tdm_keys_s.nextval
             into my_pkey_id
             from dual;
             --trace.it ( 'insert into tdm_keys' );
             insert into tdm_keys(key_id, tname, col_name, col_value, schema_name, crt_dttm, lst_updt_dttm, crt_user_id,
                           lst_updt_user_id)
             values (my_pkey_id,cur_children(ii).p_table_name,cur_children(ii).p_column_name, qry_value,cur_children(ii).p_schema_name, sysdate,sysdate,'TDM','TDM');
             COMMIT;
           end;
           /** Referential Key **/
           begin
             --trace.it ( 'select key_id from tdm_rfrntl_keys' );
             select key_id
             into my_rkey_id
             from tdm_rfrntl_keys
             where nvl(schema_name,cur_children(ii).c_schema_name) = cur_children(ii).c_schema_name
             and   tname = cur_children(ii).c_table_name
             and   col_name = cur_children(ii).c_column_name
             and   col_value = qry_value
             and rownum = 1;
           exception when no_data_found
           then
             select tdm_rfrntl_keys_s.nextval
             into my_rkey_id
             from dual;
             --trace.it ( 'insert into tdm_rfrntl_keys' );
             insert into tdm_rfrntl_keys(rkey_id, tname, col_name, col_value, key_id, schema_name, crt_dttm, lst_updt_dttm, crt_user_id,
                           lst_updt_user_id)
             values (my_rkey_id,cur_children(ii).c_table_name,cur_children(ii).c_column_name, qry_value, my_pkey_id, cur_children(ii).c_schema_name, sysdate,sysdate,'TDM','TDM');
             COMMIT;
           end;
       exception when value_error
       then
         --dbms_output.put_line('Foreign key is null');
         null;
       end;
     end loop;

     if cons_nm is not null
     then
       if (sel_qry_where != 'where ')
       then
         --dbms_output.put_line('Adding to varray:'||t_nm||','||sel_qry_where);
         my_children.extend(1);
         select t_nm, sel_qry_where, s_nm
         into my_children(cnt)
         from dual;
         cnt := cnt + 1;
       end if;
       and_string := null;
       sel_qry_where := 'where ';
       end if;
    end_time := sysdate;
    --dbms_output.put_line('Process Children executed in :'||to_char((end_time-strt_time)*3600*24,'99.99')||' secs');
    --trace.it ( 'Process Children count='|| my_children.count );
     for i in my_children.first .. my_children.last
     loop
       saveInserts(my_children(i).sname, my_children(i).tname, my_children(i).w_cls, ename,'N',tab_name);
--       --dbms_output.put_line('idx:'||my_children(i).tname||','||my_children(i).w_cls);
     end loop;
    exception when value_error then
      end_time := sysdate;
      --dbms_output.put_line('Process Children executed in :'||to_char((end_time-strt_time)*3600*24,'99.99')||' secs');
    --dbms_output.put_line('Value Error in Process Children');
  END;
    --trace.it ( '<- processChildren');
--  end_time := sysdate;
  --dbms_output.put_line('Process Children executed in :'||to_char((end_time-strt_time)*3600*24,'99.99')||' secs');
  END;

  PROCEDURE getData(sname IN VARCHAR2, ename IN VARCHAR2, tname IN VARCHAR2, w_cls IN VARCHAR2)
  IS
  BEGIN
    null;
  END;

  PROCEDURE clean_keys(sname IN varchar2)
  IS
    cursor c1(sname varchar2) is
    select k.key_id , k.tname, k.col_name, k.col_value, rk.rkey_id, rk.key_id r_key_id
    from tdm_keys k, tdm_rfrntl_keys rk
    where k.tname = rk.tname
    and   k.col_name = rk.col_name
    and   K.COL_VALUE = RK.COL_VALUE
    and   k.schema_name = rk.schema_name
    and   k.schema_name = nvl(sname,k.schema_name)
    order by k.key_id asc;
     strt_time date;
     end_time date;

   BEGIN
     --trace.it ( '-> clean_keys' );
     strt_time := sysdate;

    delete tdm_rfrntl_keys rk
    where exists (select 1
              from tdm_keys k
              where k.key_id = rk.key_id
              and   k.tname = rk.tname
              and   k.col_name = rk.col_name
              and   k.col_value = rk.col_value
              and   k.schema_name = rk.schema_name);
    --COMMIT;
    delete tdm_rfrntl_keys a
    where exists (select 1
                  from tdm_rfrntl_keys b
                  where b.schema_name = a.schema_name
                  and   b.tname = a.tname
                  and   b.col_name = a.col_name
                  and   b.col_value = a.col_value
                  and   b.key_id = a.key_id
                  and   b.rkey_id > a.rkey_id);
    --COMMIT;
    for rec in c1(sname)
    loop
      update tdm_rfrntl_keys
      set key_id = rec.r_key_id
      where key_id = rec.key_id;
      
      delete tdm_keys
      where key_id = rec.key_id;
      --COMMIT;
    end loop;
  end_time := sysdate;
  --dbms_output.put_line('Clean Keys executed in :'||to_char((end_time-strt_time)*3600*24,'99.99')||' secs');
  --trace.it ('Clean Keys executed in :'||to_char((end_time-strt_time)*3600*24,'99.99')||' secs');
  --trace.it ( '<- clean_keys' );
  END;


PROCEDURE regenerateData(sname IN varchar2 default null, del_tmp_table in varchar2 default 'Y') IS
 run_status varchar2(10);
 tmp_sname VARCHAR2(20);
 err_msg VARCHAR2(1000);
 seq_no VARCHAR2(100);
 cnt number;
 cnt1 number;
 str VARCHAR2(200);
 
 BEGIN
    select TDM_ERR_S.nextval into seq_no from DUAL;
    tmp_sname := 'TDM_REGEN';
    --SELECT COUNT(*) into cnt FROM TDM_STATUS WHERE SCHEMA_NAME=tmp_sname;
    --dbms_output.put_line(cnt);

    /*IF cnt=0 THEN
        INSERT INTO TDM_STATUS VALUES (tmp_sname,'Y');
        COMMIT;
    END IF;*/

    SELECT STATUS INTO run_status FROM TDM_STATUS WHERE SCHEMA_NAME=tmp_sname;
    dbms_output.put_line(run_status);
    IF run_status='Y' THEN
      BEGIN
        --execute immediate 'UPDATE TDM_STATUS SET STATUS=''N''' ||' WHERE SCHEMA_NAME='''||tmp_sname||'''';
        if del_tmp_table = 'Y' then
          BEGIN
            --dbms_output.put_line('test');
            execute immediate 'DELETE FROM TDM_REGEN_DATA';
          EXCEPTION WHEN OTHERS THEN
            null;
          END;
          execute immediate ' INSERT INTO TDM_REGEN_DATA (SELECT DISTINCT qry,schema_name,tname,entity_name FROM tdm_data WHERE DATA_FLAG=''Y'''||')';

          BEGIN
              --EXECUTE IMMEDIATE 'DELETE FROM TDM_RFRNTL_KEYS';
              --EXECUTE IMMEDIATE 'DELETE FROM TDM_KEYS';
              --EXECUTE IMMEDIATE 'DELETE FROM TDM_DATA';
              EXECUTE IMMEDIATE 'TRUNCATE TABLE TDM_RFRNTL_KEYS';
              EXECUTE IMMEDIATE 'TRUNCATE TABLE TDM_KEYS';
              EXECUTE IMMEDIATE 'TRUNCATE TABLE TDM_DATA';
          EXCEPTION WHEN OTHERS THEN
            PKG_TESTDATA_ERR.insertError(seq_no,'Error deleting old data','RegenerateTDMDATA');
            raise;
          END;

        end if;
        preOperations(sname);
        --preOpsFlag := 'Y';
       cnt1 := 0;
       FOR REC in (select qry,schema_name,tname,entity_name FROM TDM_REGEN_DATA WHERE SCHEMA_NAME = nvl(sname,SCHEMA_NAME)
                   minus
                   select qry,schema_name,tname,entity_name FROM TDM_DATA WHERE SCHEMA_NAME = nvl(sname,SCHEMA_NAME))
        LOOP
            BEGIN
              cnt1 := cnt1 + 1;
              PKG_TESTDATA_GEN.EXTRACTDATA (REC.schema_name,REC.tname,REC.qry,REC.entity_name);
            EXCEPTION
            WHEN OTHERS then
               err_msg :=rec.schema_name||'-'||rec.tname||'-'||rec.qry||'-'||rec.entity_name||':'||' Exception= '||SQLERRM;
               --INSERT INTO TDM_ERR VALUES (seq_no,'RegenerateTDMDATA',err_msg,sysdate,sysdate,'TDM','TDM');
               PKG_TESTDATA_ERR.insertError(seq_no,err_msg,'RegenerateTDMDATA');
            END;
        END LOOP;
        dbms_output.put_line(sname || '- Total no of root records processed ' ||cnt1 ||'-' ||systimestamp);
--       execute immediate 'DELETE FROM TDM_REGEN_DATA';
        --execute immediate 'UPDATE TDM_STATUS SET STATUS=''Y'''|| ' WHERE SCHEMA_NAME='''||tmp_sname||'''';
        EXCEPTION when others then
          --execute immediate 'UPDATE TDM_STATUS SET STATUS=''Y'''|| ' WHERE SCHEMA_NAME='''||tmp_sname||'''';
          err_msg :=' Exception= '||SQLERRM;
          --INSERT INTO TDM_ERR VALUES (seq_no,'RegenerateTDMDATA',err_msg,sysdate,sysdate,'TDM','TDM');
          PKG_TESTDATA_ERR.insertError(seq_no,err_msg,'RegenerateTDMDATA');
      END;
    ELSE
        err_msg :='Regeneration of TDM Datas already Running.';
        PKG_TESTDATA_ERR.insertError(seq_no,err_msg,'RegenerateTDMDATA');
    END IF;
 END;

  function getMViewBaseTable(mview_nm IN varchar2, owner_nm IN varchar2) return varchar2
IS
  c1 varchar2(4000);
  n1 number;
  my_mview_target mview_target;
BEGIN
  /** THIS PROCEDURE ASSUMES THE MVIEW's QUERY IS < 4000 chars **/
  /** Also the MVIEW IS BASED ON ONLY ONE TABLE **/
  select query
  into c1
  from all_mviews
  where mview_name = mview_nm
  and   owner = owner_nm;
  c1 := ltrim(substr(c1, instr(lower(c1),'from')+5));
  n1 := instr(c1,' ');
  select substr(c1,1,decode(n1,0,length(c1),n1-1))
  into c1
  from dual;
  dbms_output.put_line(c1);
  my_mview_target.owner := substr(c1,1,instr(c1,'.')-1);
  my_mview_target.table_name := substr(c1,instr(c1,'.')+1);
--  my_mview_target := (sname, tab_name);
  return upper(my_mview_target.owner||'.'||my_mview_target.table_name);
END;

PROCEDURE processMviews(sname IN varchar2)
IS
BEGIN
  null;
END;

PROCEDURE test_category
is
begin
PKG_TESTDATA_GEN.SAVEPRIMARYCOLUMNS('MCMBRWIP');
PKG_TESTDATA_GEN.SAVEPRIMARYCOLUMNS('BRINV');
PKG_TESTDATA_GEN.SAVEPRIMARYCOLUMNS('TOOLSVC');
PKG_TESTDATA_GEN.SAVECHILDRELATIONSHIPS('MCMBRWIP','CATEGORY');
PKG_TESTDATA_GEN.SAVECHILDRELATIONSHIPS('BRINV','CATEGORY');
PKG_TESTDATA_GEN.SAVECHILDRELATIONSHIPS('TOOLSVC','CATEGORY');
PKG_TESTDATA_GEN.SAVEPARENTRELATIONSHIPS('MCMBRWIP','CATEGORY');
PKG_TESTDATA_GEN.SAVEPARENTRELATIONSHIPS('BRINV','CATEGORY');
PKG_TESTDATA_GEN.SAVEPARENTRELATIONSHIPS('TOOLSVC','CATEGORY');
PKG_TESTDATA_GEN.EXTRACTDATA('MCMBRWIP','REL_CTLG_ITM_T','where ctlg_itm_2_id in (select ctlg_itm_id from MCMBRWIP.ctlg_tree_path_t where ctlg_itm_styp_id > 12 start with bus_id = ''768388'' connect by par_ctlg_itm_id = prior ctlg_itm_id) and rel_ctlg_itm_typ_id = 1','CATEGORY');
end;


PROCEDURE test_product
is
BEGIN
PKG_TESTDATA_GEN.SAVEPRIMARYCOLUMNS('MCMBRWIP');
PKG_TESTDATA_GEN.SAVEPRIMARYCOLUMNS('BRINV');
PKG_TESTDATA_GEN.SAVEPRIMARYCOLUMNS('TOOLSVC');
PKG_TESTDATA_GEN.SAVEPRIMARYCOLUMNS('PPCWIP');
PKG_TESTDATA_GEN.SAVEPRIMARYCOLUMNS('PPCAPR');
PKG_TESTDATA_GEN.SAVECHILDRELATIONSHIPS('MCMBRWIP','PRODUCT');
PKG_TESTDATA_GEN.SAVECHILDRELATIONSHIPS('BRINV','PRODUCT');
PKG_TESTDATA_GEN.SAVECHILDRELATIONSHIPS('TOOLSVC','PRODUCT');
PKG_TESTDATA_GEN.SAVECHILDRELATIONSHIPS('PPCWIP','PRODUCT');
PKG_TESTDATA_GEN.SAVECHILDRELATIONSHIPS('MCMBRWIP','PRODUCT');
PKG_TESTDATA_GEN.SAVEPARENTRELATIONSHIPS('MCMBRWIP','PRODUCT');
PKG_TESTDATA_GEN.SAVEPARENTRELATIONSHIPS('BRINV','PRODUCT');
PKG_TESTDATA_GEN.SAVEPARENTRELATIONSHIPS('TOOLSVC','PRODUCT');
PKG_TESTDATA_GEN.SAVEPARENTRELATIONSHIPS('PPCWIP','PRODUCT');
PKG_TESTDATA_GEN.SAVEPARENTRELATIONSHIPS('PPCAPR','PRODUCT');
PKG_TESTDATA_GEN.EXTRACTDATA('MCMBRWIP','CTLG_ITM_T','where bus_id = ''719652'' and ctlg_itm_styp_id > 12','PRODUCT');
END;

PROCEDURE preOperations(sname in VARCHAR2)
is
 TYPE vSchema is varray(200) of VARCHAR2(100);
 TYPE vEntity is varray(20) of VARCHAR2(100);
 my_vSchema vSchema; 
 my_vEntity vEntity;
BEGIN
        dbms_output.put_line('Save Start Time ' || systimestamp);
        my_vSchema := vSchema();
        my_vEntity := vEntity('PRODUCT','CATEGORY','RELATED_ITEM');
        --str := 'SELECT distinct owner FROM TDM_referential_constraints where owner is not null union SELECT distinct(r_owner) owner FROM TDM_referential_constraints where r_owner is not null order by owner';
        --EXECUTE IMMEDIATE str BULK COLLECT INTO my_vSchema;
        my_vSchema.extend(6);
        my_vSchema(1) := 'TOOLSVC';
        my_vSchema(2) := replace(sname,'APR','WIP');
        my_vSchema(3) := replace(sname,'WIP','APR');
        my_vSchema(4) := 'PPCWIP';
        my_vSchema(5) := 'PPCAPR';
        my_vSchema(6) := replace(replace(my_vSchema(3),'APR','INV'),'MCM','');
        FOR i IN my_vSchema.first .. my_vSchema.last
        LOOP
            --dbms_output.put_line(my_vSchema(i));
           --dbms_output.put_line('Save PC Start Time ' || systimestamp);
            savePrimaryColumns(my_vSchema(i));
            --dbms_output.put_line('Save PC End Time Time ' || systimestamp);
            FOR ii IN my_vEntity.first .. my_vEntity.last
            loop
                saveChildRelationships(my_vSchema(i),my_vEntity(ii));
                saveParentRelationships(my_vSchema(i),my_vEntity(ii));
            end loop;
        END LOOP;
        dbms_output.put_line('Save End Time ' || systimestamp);  
END;

END PKG_TESTDATA_GEN;
/
