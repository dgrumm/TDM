CREATE OR REPLACE PACKAGE BODY TDM_USER."PKG_TESTDATA_CATALOG_SAVE" AS

  PROCEDURE saveWipAprData(wip_schema_name IN VARCHAR2, my_schema_map IN schema_map default load_tools_schema_map ) IS
      apr_schema_name varchar2(30);
      inv_schema_name varchar2(30);
      type schema_array is varray(6) of varchar2(30);
      s_arr schema_array;
  BEGIN

      apr_schema_name := replace(wip_schema_name, 'WIP','APR');
      inv_schema_name := replace(wip_schema_name, 'MCM','');
      inv_schema_name := replace(inv_schema_name, 'WIP','INV');

      s_arr := schema_array();
      s_arr.extend(6);
      s_arr(1) := 'TOOLSVC';
      s_arr(2) := wip_schema_name;
      s_arr(3) := apr_schema_name;
      s_arr(4) := 'PPCWIP';
      s_arr(5) := 'PPCAPR';
      s_arr(6) := inv_schema_name;

      BEGIN
        dbms_output.put_line('S_arr(2): Start Time'|| systimestamp);
        TDM_USER.PKG_TESTDATA_SAVE.SAVETDMDATATOTARGETSCHEMA(s_arr(2),my_schema_map(s_arr(2)),'Y');
        dbms_output.put_line('S_arr(2): End Time'|| systimestamp);
      exception
      when no_data_found then
             dbms_output.put_line('S_arr(2): '|| SQLCODE || '-' || SQLERRM);
      END;
      BEGIN
        dbms_output.put_line('S_arr(3): Start Time'|| systimestamp);
        TDM_USER.PKG_TESTDATA_SAVE.SAVETDMDATATOTARGETSCHEMA(s_arr(3),my_schema_map(s_arr(3)),'Y');
        dbms_output.put_line('S_arr(3): End Time'|| systimestamp);
      exception
      when no_data_found then
             dbms_output.put_line('S_arr(3): '|| SQLCODE || '-' || SQLERRM);
      END;
      /*for i in s_arr.first .. s_arr.last
      loop
           --TOOLSSVC , WIP , APR , INV
           begin
             dbms_output.put_line('S_arr(i): '||s_arr(i));
             TDM_USER.PKG_TESTDATA_SAVE.SAVETDMDATATOTARGETSCHEMA (s_arr(i),my_schema_map(s_arr(i)),'Y');
           exception
           when no_data_found then
             null;
           end;
           /*
           TDM_USER.PKG_TESTDATA_SAVE.SAVETDMDATATOTARGETSCHEMA (wip_schema_name,my_schema_map(wip_schema_name),'Y');
           TDM_USER.PKG_TESTDATA_SAVE.SAVETDMDATATOTARGETSCHEMA (apr_schema_name,my_schema_map(apr_schema_name),'Y');
           TDM_USER.PKG_TESTDATA_SAVE.SAVETDMDATATOTARGETSCHEMA ('PPCAPR',my_schema_map('PPCAPR'),'Y');
           TDM_USER.PKG_TESTDATA_SAVE.SAVETDMDATATOTARGETSCHEMA (inv_schema_name, my_schema_map(inv_schema_name), 'Y');
           */
      --end loop;
  END;

  PROCEDURE saveEcomCatalogData(apr_schema_name IN VARCHAR2, my_schema_map IN schema_map default load_ecom_schema_map) IS
    inv_schema_name varchar2(30);
    t_apr_schema_name varchar2(30);
    wip_schema_name varchar2(30);
    BEGIN
      t_apr_schema_name := replace(apr_schema_name, 'WIP','APR'); /* Just ensuring */
      wip_schema_name := replace(t_apr_schema_name, 'APR','WIP');
      inv_schema_name := replace(t_apr_schema_name, 'MCM','');
      inv_schema_name := replace(inv_schema_name, 'APR','INV');

      PKG_TESTDATA_SAVE.POPULATEALTERNATEKEYS('TOOLSVC',my_schema_map(apr_schema_name || '_TOOLSVC'),'Y',0);
      PKG_TESTDATA_SAVE.POPULATEALTERNATEKEYS( wip_schema_name,my_schema_map(apr_schema_name),'Y',0);
      PKG_TESTDATA_SAVE.SAVETDMDATATOTARGETSCHEMA (apr_schema_name,my_schema_map(apr_schema_name),'N');
      PKG_TESTDATA_SAVE.SAVETDMDATATOTARGETSCHEMA (inv_schema_name,my_schema_map(inv_schema_name),'N');
--      PKG_TESTDATA_SAVE.SAVETDMDATATOTARGETSCHEMA ('PPCAPR',my_schema_map('PPCAPR'),'N');
    END;
 
PROCEDURE saveEcomCatalogDataPPC(my_schema_map IN schema_map default load_ecom_schema_map)
IS
BEGIN
      DBMS_OUTPUT.put_line ('PPCWIP Start Time' || SYSTIMESTAMP);
      TDM_USER.PKG_TESTDATA_SAVE.SAVETDMDATATOTARGETSCHEMA ('PPCAPR',my_schema_map('PPCAPR'),'N');
      DBMS_OUTPUT.put_line ('PPCWIP End Time' || SYSTIMESTAMP);
END;
   
/* Formatted on 10/20/2010 11:44:12 AM (QP5 v5.115.810.9015) */
PROCEDURE saveWipAprDataTOOLSVC
IS
BEGIN
      DBMS_OUTPUT.put_line ('TOOLSVC Start Time' || SYSTIMESTAMP);
      TDM_USER.PKG_TESTDATA_SAVE.SAVETDMDATATOTARGETSCHEMA ('TOOLSVC','TOOLSVC','Y');
      DBMS_OUTPUT.put_line ('TOOLSVC End Time' || SYSTIMESTAMP);
END;

PROCEDURE saveWipAprDataPPC
IS
BEGIN
      DBMS_OUTPUT.put_line ('PPCWIP Start Time' || SYSTIMESTAMP);
      TDM_USER.PKG_TESTDATA_SAVE.SAVETDMDATATOTARGETSCHEMA ('PPCWIP','PPCWIP','Y');
      DBMS_OUTPUT.put_line ('PPCWIP End Time' || SYSTIMESTAMP);

      DBMS_OUTPUT.put_line ('PPCAPR Start Time' || SYSTIMESTAMP);
      TDM_USER.PKG_TESTDATA_SAVE.SAVETDMDATATOTARGETSCHEMA ('PPCAPR','PPCAPR','Y');
      DBMS_OUTPUT.put_line ('PPCAPR End Time' || SYSTIMESTAMP);

END;
  
  PROCEDURE saveWipAprDataForABrand(wip_schema_name IN VARCHAR2) IS
      apr_schema_name varchar2(30);
      inv_schema_name varchar2(30);
      type schema_array is varray(6) of varchar2(30);
      jobStatus VARCHAR2(30);
      s_arr schema_array;
  BEGIN

      apr_schema_name := replace(wip_schema_name, 'WIP','APR');
      inv_schema_name := replace(wip_schema_name, 'MCM','');
      inv_schema_name := replace(inv_schema_name, 'WIP','INV');

        dbms_output.put_line(wip_schema_name || ' Start Time'|| systimestamp);
        TDM_USER.PKG_TESTDATA_SAVE.SAVETDMDATATOTARGETSCHEMA(wip_schema_name,wip_schema_name,'Y');
        dbms_output.put_line(wip_schema_name || ' End Time'|| systimestamp);
        --select job_status  into jobStatus from tdm_status where schema_name = 'wip_schema_name';
        --IF jobStatus = 'SUCCESS' THEN
        dbms_output.put_line(apr_schema_name || ' Start Time'|| systimestamp);
        TDM_USER.PKG_TESTDATA_SAVE.SAVETDMDATATOTARGETSCHEMA(apr_schema_name,apr_schema_name,'Y');
        dbms_output.put_line(apr_schema_name || ' End Time'|| systimestamp);
        --END IF;
       
        dbms_output.put_line(inv_schema_name || ' Start Time'|| systimestamp);
        TDM_USER.PKG_TESTDATA_SAVE.SAVETDMDATATOTARGETSCHEMA(inv_schema_name,inv_schema_name,'Y');
        dbms_output.put_line(inv_schema_name || ' End Time'|| systimestamp);
  END;
  
 FUNCTION load_tools_schema_map RETURN schema_map
 is
    my_schema_map schema_map;
  begin
    for rec in (select distinct schema_name from tdm_data where schema_name not in ('TOOLSVC','MCMGAPWIP','MCMGAPAPR','PPCWIP','PPCAPR'))
    loop
      my_schema_map(rec.schema_name) :=  rec.schema_name;
    end loop;
    return my_schema_map;
  end;

 FUNCTION load_test_schema_map RETURN schema_map
 is
    my_schema_map schema_map;
  begin
--    my_schema_map('TOOLSVC') := 'TDM_TOOLSVC';
--    my_schema_map('PPCWIP') := 'TDM_PPCWIP';
--    my_schema_map('PPCAPR') := 'TDM_PPCAPR';
    my_schema_map('MCMBRWIP') := 'TDM_MCMBRWIP';

 /*
    my_schema_map('ATINV') := 'TDM_ATINV';
    my_schema_map('BRINV') := 'TDM_BRINV';
--    my_schema_map('MCMBRAPR') := 'TDM_MCMBRAPR';
    my_schema_map('MCMATWIP') := 'TDM_MCMATWIP';
--    my_schema_map('MCMATAPR') := 'TDM_MCMATAPR';
 */
    return my_schema_map;
  end;

  FUNCTION load_ecom_schema_map RETURN schema_map
 is
    my_schema_map schema_map;
  begin

    my_schema_map('PPCAPR') := 'PPCA';

    my_schema_map('MCMBRAPR') := 'BRCATA';
    my_schema_map('MCMATAPR') := 'ATCATA';
    my_schema_map('MCMBGAPR') := 'BGCATA';
    my_schema_map('MCMONAPR') := 'ONCATA';
    my_schema_map('MCMGAPAPR') := 'GAPCATA';
    my_schema_map('EU_MCMBRAPR') := 'EU_BRCATA';
    my_schema_map('EU_MCMGAPAPR') := 'EU_GAPCATA';
    my_schema_map('CA_MCMBRAPR') := 'CA_BRCATA';
    my_schema_map('CA_MCMGAPAPR') := 'CA_GAPCATA';
    my_schema_map('CA_MCMONAPR') := 'CA_ONCATA';

    my_schema_map('BRINV') := 'BRINV';
    my_schema_map('ATINV') := 'ATINV';
    my_schema_map('BGINV') := 'BGINV';
    my_schema_map('ONINV') := 'ONINV';
    my_schema_map('GAPINV') := 'GAPINV';
    my_schema_map('BRINV') := 'EU_BRINV';
    my_schema_map('ATINV') := 'EU_GAPINV';
    my_schema_map('BGINV') := 'CA_BRINV';
    my_schema_map('ONINV') := 'CA_GAPINV';
    my_schema_map('GAPINV') := 'CA_ONINV';
    
    my_schema_map('MCMBRAPR_TOOLSVC') := 'BRTOOLSVCA';
    my_schema_map('MCMATAPR_TOOLSVC') := 'ATTOOLSVCA';
    my_schema_map('MCMBGAPR_TOOLSVC') := 'BGTOOLSVCA';
    my_schema_map('MCMONAPR_TOOLSVC') := 'ONTOOLSVCA';
    my_schema_map('MCMGAPAPR_TOOLSVC') := 'GAPTOOLSVCA';
    my_schema_map('EU_MCMBRAPR_TOOLSVC') := 'EU_BRTOOLSVCA';
    my_schema_map('EU_MCMGAPAPR_TOOLSVC') := 'EU_GAPTOOLSVCA';
    my_schema_map('CA_MCMBRAPR_TOOLSVC') := 'CA_BRTOOLSVCA';
    my_schema_map('CA_MCMGAPAPR_TOOLSVC') := 'CA_GAPTOOLSVCA';
    my_schema_map('CA_MCMONAPR_TOOLSVC') := 'CA_ONTOOLSVCA';

    return my_schema_map;
  end;

END PKG_TESTDATA_CATALOG_SAVE;
/
