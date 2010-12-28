CREATE OR REPLACE PACKAGE BODY TDM_USER."PKG_TESTDATA_CATALOG" AS

  PROCEDURE extractProduct(style_bus_id in varchar2, source_schema in varchar2) IS
    schema_name_1 varchar2(30);
    schema_name_2 varchar2(30);
  BEGIN
    
    if source_schema like '%MCM%WIP%' then
      schema_name_1 := source_schema;
      schema_name_2 := replace(source_schema,'WIP','APR');
    elsif source_schema like '%MCM%APR%' then
      schema_name_1 := replace(source_schema,'APR','WIP');
      schema_name_2 := source_schema;
    else
      schema_name_1 := null;
      schema_name_2 := null;
    end if;
    if schema_name_1 is null then
      PKG_TESTDATA_GEN.preOperations(source_schema);
      PKG_TESTDATA_GEN.extractData(source_schema, 'CTLG_ITM_T','where bus_id = '''||style_bus_id||'''','PRODUCT');
    else
      PKG_TESTDATA_GEN.preOperations(schema_name_1);
      PKG_TESTDATA_GEN.preOperations(schema_name_2);
      PKG_TESTDATA_GEN.extractData(schema_name_1, 'CTLG_ITM_T','where bus_id = '''||style_bus_id||'''','PRODUCT');
      PKG_TESTDATA_GEN.extractData(schema_name_2, 'CTLG_ITM_T','where bus_id = '''||style_bus_id||'''','PRODUCT');
      makeAprWipDependency(schema_name_1);
      commit;
    end if;
  END;

  PROCEDURE extractProductCategories(style_bus_id in varchar2, source_schema in varchar2) IS
    w_cls varchar2(300);
    schema_name_1 varchar2(30);
    schema_name_2 varchar2(30);
  BEGIN
    if source_schema like '%MCM%WIP%' then
      schema_name_1 := source_schema;
      schema_name_2 := replace(source_schema,'WIP','APR');
    elsif source_schema like '%MCM%APR%' then
      schema_name_1 := replace(source_schema,'APR','WIP');
      schema_name_2 := source_schema;
    else
      schema_name_1 := null;
      schema_name_2 := null;
    end if;
    if schema_name_1 is null then
      PKG_TESTDATA_GEN.preOperations(source_schema);
      w_cls := 'where ctlg_itm_2_id in (select ctlg_itm_id from '||source_schema||'.ctlg_tree_path_t where ctlg_itm_styp_id > 12 start with bus_id = '''||style_bus_id||''' connect by par_ctlg_itm_id = prior ctlg_itm_id)' ;
      PKG_TESTDATA_GEN.extractData(source_schema, 'REL_CTLG_ITM_T',w_cls,'CATEGORY');
    else
      PKG_TESTDATA_GEN.preOperations(schema_name_1);
      PKG_TESTDATA_GEN.preOperations(schema_name_2);
      w_cls := 'where ctlg_itm_2_id in (select ctlg_itm_id from '||schema_name_1||'.ctlg_tree_path_t where ctlg_itm_styp_id > 12 start with bus_id = '''||style_bus_id||''' connect by par_ctlg_itm_id = prior ctlg_itm_id)' ;
      PKG_TESTDATA_GEN.extractData(schema_name_1, 'REL_CTLG_ITM_T',w_cls,'CATEGORY');
      w_cls := 'where ctlg_itm_2_id in (select ctlg_itm_id from '||schema_name_2||'.ctlg_tree_path_t where ctlg_itm_styp_id > 12 start with bus_id = '''||style_bus_id||''' connect by par_ctlg_itm_id = prior ctlg_itm_id)' ;
      PKG_TESTDATA_GEN.extractData(schema_name_2, 'REL_CTLG_ITM_T',w_cls,'CATEGORY');
      makeAprWipDependency(schema_name_1);
      commit;
    end if;


  END;

  PROCEDURE extractProductAndCategories(style_bus_id in varchar2, source_schema in varchar2) IS
    w_cls varchar2(300);
    schema_name_1 varchar2(30);
    schema_name_2 varchar2(30);
  BEGIN
    if source_schema like '%MCM%WIP%' then
      schema_name_1 := source_schema;
      schema_name_2 := replace(source_schema,'WIP','APR');
    elsif source_schema like '%MCM%APR%' then
      schema_name_1 := replace(source_schema,'APR','WIP');
      schema_name_2 := source_schema;
    else
      schema_name_1 := null;
      schema_name_2 := null;
    end if;
    if schema_name_1 is null then
      PKG_TESTDATA_GEN.preOperations(source_schema);
      PKG_TESTDATA_GEN.extractData(source_schema, 'CTLG_ITM_T','where bus_id = '''||style_bus_id||'''','PRODUCT');
      w_cls := 'where ctlg_itm_2_id in (select ctlg_itm_id from '||source_schema||'.ctlg_tree_path_t where ctlg_itm_styp_id > 12 start with bus_id = '''||style_bus_id||''' connect by par_ctlg_itm_id = prior ctlg_itm_id)' ;
      PKG_TESTDATA_GEN.extractData(source_schema, 'REL_CTLG_ITM_T',w_cls,'CATEGORY');
    else
      PKG_TESTDATA_GEN.preOperations(schema_name_1);
      PKG_TESTDATA_GEN.preOperations(schema_name_2);
      PKG_TESTDATA_GEN.extractData(schema_name_1, 'CTLG_ITM_T','where bus_id = '''||style_bus_id||'''','PRODUCT');
      w_cls := 'where ctlg_itm_2_id in (select ctlg_itm_id from '||schema_name_1||'.ctlg_tree_path_t where ctlg_itm_styp_id > 12 start with bus_id = '''||style_bus_id||''' connect by par_ctlg_itm_id = prior ctlg_itm_id)' ;
      PKG_TESTDATA_GEN.extractData(schema_name_1, 'REL_CTLG_ITM_T',w_cls,'CATEGORY');
      PKG_TESTDATA_GEN.extractData(schema_name_2, 'CTLG_ITM_T','where bus_id = '''||style_bus_id||'''','PRODUCT');
      w_cls := 'where ctlg_itm_2_id in (select ctlg_itm_id from '||schema_name_2||'.ctlg_tree_path_t where ctlg_itm_styp_id > 12 start with bus_id = '''||style_bus_id||''' connect by par_ctlg_itm_id = prior ctlg_itm_id)' ;
      PKG_TESTDATA_GEN.extractData(schema_name_2, 'REL_CTLG_ITM_T',w_cls,'CATEGORY');
      makeAprWipDependency(schema_name_1);
      commit;
    end if;
  END;

  /*

  PROCEDURE extractProductEtAl (style_bus_id in varchar2, source_schema in varchar2) IS
    cii number;
    type my_cursor is ref cursor;
    c_get_cii my_cursor;
    c_get_styles my_cursor;
    c_get_related_items my_cursor;
    schema_name_1 varchar2(30);
    schema_name_2 varchar2(30);
  BEGIN
    if source_schema like '%MCM%WIP%' then
      schema_name_1 := source_schema;
      schema_name_2 := replace(source_schema,'WIP','APR');
    elsif source_schema like '%MCM%APR%' then
      schema_name_1 := replace(source_schema,'APR','WIP');
      schema_name_2 := source_schema;
    else
      schema_name_1 := null;
      schema_name_2 := null;
    end if;
    if schema_name_1 is null then
    open c_get_cii for 'select ctlg_itm_id from '||source_schema||'.'||'ctlg_itm_t where bus_id = '''||style_bus_id||'''';
    fetch c_get_cii into cii;
    close c_get_cii;
  END;
  */
  PROCEDURE extractOutfit (outf_bus_id in varchar2, source_schema in varchar2) IS
    w_cls varchar2(300);
    schema_name_1 varchar2(30);
    schema_name_2 varchar2(30);
  BEGIN
    if source_schema like '%MCM%WIP%' then
      schema_name_1 := source_schema;
      schema_name_2 := replace(source_schema,'WIP','APR');
    elsif source_schema like '%MCM%APR%' then
      schema_name_1 := replace(source_schema,'APR','WIP');
      schema_name_2 := source_schema;
    else
      schema_name_1 := null;
      schema_name_2 := null;
    end if;
    if schema_name_1 is null then
      PKG_TESTDATA_GEN.preOperations(source_schema);
      w_cls := 'where bus_id = '''||outf_bus_id||'''' ;
      PKG_TESTDATA_GEN.extractData(source_schema, 'CTLG_ITM_T',w_cls,'CATEGORY');
      w_cls := 'where ctlg_itm_1_id in (select ctlg_itm_id from '||source_schema||'.ctlg_itm_t where bus_id = '''||outf_bus_id||''') ' ;
      PKG_TESTDATA_GEN.extractData(source_schema, 'REL_CTLG_ITM_T',w_cls,'PRODUCT');
    else
      PKG_TESTDATA_GEN.preOperations(schema_name_1);
      PKG_TESTDATA_GEN.preOperations(schema_name_2);
      w_cls := 'where bus_id = '''||outf_bus_id||'''' ;
      PKG_TESTDATA_GEN.extractData(schema_name_1, 'CTLG_ITM_T',w_cls,'CATEGORY');
      w_cls := 'where ctlg_itm_1_id in (select ctlg_itm_id from '||schema_name_1||'.ctlg_itm_t where bus_id = '''||outf_bus_id||''') ' ;
      PKG_TESTDATA_GEN.extractData(schema_name_1, 'REL_CTLG_ITM_T',w_cls,'PRODUCT');
      w_cls := 'where bus_id = '''||outf_bus_id||'''' ;
      PKG_TESTDATA_GEN.extractData(schema_name_2, 'CTLG_ITM_T',w_cls,'CATEGORY');
      w_cls := 'where ctlg_itm_1_id in (select ctlg_itm_id from '||schema_name_2||'.ctlg_itm_t where bus_id = '''||outf_bus_id||''') ' ;
      PKG_TESTDATA_GEN.extractData(schema_name_2, 'REL_CTLG_ITM_T',w_cls,'PRODUCT');
      makeAprWipDependency(schema_name_1);
      commit;
    end if;
  END;

  PROCEDURE extractRelatedStyles (style_bus_id in varchar2, source_schema in varchar2) IS
    w_cls varchar2(300);
    schema_name_1 varchar2(30);
    schema_name_2 varchar2(30);
  BEGIN
    if source_schema like '%MCM%WIP%' then
      schema_name_1 := source_schema;
      schema_name_2 := replace(source_schema,'WIP','APR');
    elsif source_schema like '%MCM%APR%' then
      schema_name_1 := replace(source_schema,'APR','WIP');
      schema_name_2 := source_schema;
    else
      schema_name_1 := null;
      schema_name_2 := null;
    end if;
    if schema_name_1 is null then
      PKG_TESTDATA_GEN.preOperations(source_schema);
      w_cls := 'where ctlg_itm_1_id in (select ctlg_itm_id from '||source_schema||'.ctlg_itm_t where bus_id = '''||style_bus_id||''')' ;
      PKG_TESTDATA_GEN.extractData(source_schema, 'REL_CTLG_ITM_T',w_cls,'RELATED_ITEM');
      w_cls := 'where ctlg_itm_id in (select ctlg_itm_2_id from '||source_schema||'.rel_ctlg_itm_t where ctlg_itm_1_id in (select ctlg_itm_id from '||source_schema||'.ctlg_itm_t where bus_id = '''||style_bus_id||'''))' ;
      PKG_TESTDATA_GEN.extractData(source_schema, 'CTLG_ITM_T',w_cls,'PRODUCT');
    else
      PKG_TESTDATA_GEN.preOperations(schema_name_1);
      PKG_TESTDATA_GEN.preOperations(schema_name_2);
      w_cls := 'where ctlg_itm_1_id in (select ctlg_itm_id from '||schema_name_1||'.ctlg_itm_t where bus_id = '''||style_bus_id||''')' ;
      PKG_TESTDATA_GEN.extractData(schema_name_1, 'REL_CTLG_ITM_T',w_cls,'RELATED_ITEM');
      w_cls := 'where ctlg_itm_id in (select ctlg_itm_2_id from '||schema_name_1||'.rel_ctlg_itm_t where ctlg_itm_1_id in (select ctlg_itm_id from '||schema_name_1||'.ctlg_itm_t where bus_id = '''||style_bus_id||'''))' ;
      PKG_TESTDATA_GEN.extractData(schema_name_1, 'CTLG_ITM_T',w_cls,'PRODUCT');
      w_cls := 'where ctlg_itm_1_id in (select ctlg_itm_id from '||schema_name_2||'.ctlg_itm_t where bus_id = '''||style_bus_id||''')' ;
      PKG_TESTDATA_GEN.extractData(schema_name_2, 'REL_CTLG_ITM_T',w_cls,'RELATED_ITEM');
      w_cls := 'where ctlg_itm_id in (select ctlg_itm_2_id from '||schema_name_2||'.rel_ctlg_itm_t where ctlg_itm_1_id in (select ctlg_itm_id from '||schema_name_2||'.ctlg_itm_t where bus_id = '''||style_bus_id||'''))' ;
      PKG_TESTDATA_GEN.extractData(schema_name_2, 'CTLG_ITM_T',w_cls,'PRODUCT');
      makeAprWipDependency(schema_name_1);
      commit;
    end if;
  END;

  PROCEDURE extractCategoryProducts(cat_bus_id varchar2, source_schema in varchar2) IS
    w_cls varchar2(300);
    schema_name_1 varchar2(30);
    schema_name_2 varchar2(30);
  BEGIN
    if source_schema like '%MCM%WIP%' then
      schema_name_1 := source_schema;
      schema_name_2 := replace(source_schema,'WIP','APR');
    elsif source_schema like '%MCM%APR%' then
      schema_name_1 := replace(source_schema,'APR','WIP');
      schema_name_2 := source_schema;
    else
      schema_name_1 := null;
      schema_name_2 := null;
    end if;
      /** Exactly Same as Outfit **/
    if schema_name_1 is null then
      PKG_TESTDATA_GEN.preOperations(source_schema);
      w_cls := 'where bus_id = '''||cat_bus_id||'''' ;
      PKG_TESTDATA_GEN.extractData(source_schema, 'CTLG_ITM_T',w_cls,'CATEGORY');
      w_cls := 'where ctlg_itm_1_id in (select ctlg_itm_id from '||source_schema||'.ctlg_itm_t where bus_id = '''||cat_bus_id||''') ' ;
      PKG_TESTDATA_GEN.extractData(source_schema, 'REL_CTLG_ITM_T',w_cls,'PRODUCT');
    else
      PKG_TESTDATA_GEN.preOperations(schema_name_1);
      PKG_TESTDATA_GEN.preOperations(schema_name_2);
      w_cls := 'where bus_id = '''||cat_bus_id||'''' ;
      PKG_TESTDATA_GEN.extractData(schema_name_1, 'CTLG_ITM_T',w_cls,'CATEGORY');
      w_cls := 'where ctlg_itm_1_id in (select ctlg_itm_id from '||schema_name_1||'.ctlg_itm_t where bus_id = '''||cat_bus_id||''') ' ;
      PKG_TESTDATA_GEN.extractData(schema_name_1, 'REL_CTLG_ITM_T',w_cls,'PRODUCT');
      w_cls := 'where bus_id = '''||cat_bus_id||'''' ;
      PKG_TESTDATA_GEN.extractData(schema_name_2, 'CTLG_ITM_T',w_cls,'CATEGORY');
      w_cls := 'where ctlg_itm_1_id in (select ctlg_itm_id from '||schema_name_2||'.ctlg_itm_t where bus_id = '''||cat_bus_id||''') ' ;
      PKG_TESTDATA_GEN.extractData(schema_name_2, 'REL_CTLG_ITM_T',w_cls,'PRODUCT');
      makeAprWipDependency(schema_name_1);
      commit;
    end if;
  END;

  PROCEDURE extractNonMerchCategories(source_schema in varchar2) IS
    schema_name_1 varchar2(30);
    schema_name_2 varchar2(30);
  BEGIN
    if source_schema like '%MCM%WIP%' then
      schema_name_1 := source_schema;
      schema_name_2 := replace(source_schema,'WIP','APR');
    elsif source_schema like '%MCM%APR%' then
      schema_name_1 := replace(source_schema,'APR','WIP');
      schema_name_2 := source_schema;
    else
      schema_name_1 := null;
      schema_name_2 := null;
    end if;
    if schema_name_1 is null then
      PKG_TESTDATA_GEN.preOperations(source_schema);
      PKG_TESTDATA_GEN. EXTRACTDATA (source_schema, 'CTLG_ITM_COLL_T', 'where catg_dply_typ_id in (30,31,32,33,34,35,36)', 'CATEGORY' );
    else
      PKG_TESTDATA_GEN.preOperations(schema_name_1);
      PKG_TESTDATA_GEN.preOperations(schema_name_2);
      PKG_TESTDATA_GEN. EXTRACTDATA (schema_name_1, 'CTLG_ITM_COLL_T', 'where catg_dply_typ_id in (30,31,32,33,34,35,36)', 'CATEGORY' );
      PKG_TESTDATA_GEN. EXTRACTDATA (schema_name_2, 'CTLG_ITM_COLL_T', 'where catg_dply_typ_id in (30,31,32,33,34,35,36)', 'CATEGORY' );
      makeAprWipDependency(schema_name_1);
      commit;
    end if;
  END;

    PROCEDURE makeAprWipDependency(wip_schema_name IN VARCHAR2) IS
      apr_schema_name varchar2(30);
    BEGIN
      apr_schema_name := replace(wip_schema_name, 'WIP','APR');
      PKG_TESTDATA_GEN.clean_keys(null);
      insert into tdm_rfrntl_keys (rkey_id, tname, col_name, col_value, schema_name, key_id, crt_user_id, crt_dttm, lst_updt_dttm,lst_updt_user_id)
      select tdm_rfrntl_keys_s.nextval,apr_k.tname,apr_k.col_name, apr_k.col_value, apr_schema_name, wip_k.key_id,
             'TDM_APR', sysdate, sysdate, 'TDM_APR'
      from tdm_keys apr_k, tdm_keys wip_k
                    where apr_k.tname = wip_k.tname
                    and   apr_k.col_name = wip_k.col_name
                    and   apr_k.col_value = wip_k.col_value
                    and   apr_k.schema_name = apr_schema_name
                    and   wip_k.schema_name = wip_schema_name;

      PKG_TESTDATA_GEN.clean_keys( apr_schema_name);

      --commit;

    END;

    PROCEDURE regenerateData (sname in varchar2 default null, del_tmp_table in varchar2 default 'Y') IS
    BEGIN
      PKG_TESTDATA_GEN.REGENERATEDATA(sname, del_tmp_table);
      --PKG_TESTDATA_GEN.clean_keys(null);
      for rec in (select distinct schema_name from tdm_data where schema_name like '%WIP%')
      loop
        makeAprWipDependency(rec.schema_name);
      end loop;
    END;
    
    PROCEDURE makewipaprdependencyAll IS
    BEGIN
      for rec in (select distinct schema_name from tdm_data where schema_name like '%WIP%')
      loop
        makeAprWipDependency(rec.schema_name);
      end loop;
    END;
    
END;
/
