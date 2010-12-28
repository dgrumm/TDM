CREATE OR REPLACE PROCEDURE          REGEN
IS
   CURSOR cQuery
   IS
      SELECT   qry, schema_name                               
        FROM   tdm_data
       WHERE   qry LIKE '%BUS%';

   qryString      VARCHAR2 (500);
   schemaString   VARCHAR2 (500);
   
   -- delete from the table before the cursor read?  Could be crazy.
   -- delete from tdm_keys 
   
   begin
     for r_cQuery in cQuery loop      
                            
       qryString := REPLACE(REPLACE(REPLACE(UPPER(r_cQuery.qry),' '),'WHEREBUS_ID='), 'ANDTO_NUMBER(CTLG_ITM_STYP_ID)>12');
       dbms_output.put_line('qryString: '|| qryString);
       schemaString := REPLACE(UPPER(r_cQuery.schema_name),' ');
       dbms_output.put_line('schemaString: '|| schemaString);
 
          Schema_data.extend;
          Schema_data(Schema_data.count) := schemaString;
          Query_data.extend;
          Query_data(Query_data.count) := qryString;         
     end loop;
     
 
      FOR i IN 1 .. Schema_data.COUNT LOOP
            dbms_output.put_line(TO_CHAR(i) || ' ' || Schema_data(i));
            --delete from tdm_keys where schema_name = Schema_data(i);
      END LOOP;
      
      FOR i IN 1 .. Query_data.COUNT LOOP
            dbms_output.put_line(TO_CHAR(i) || ' ' || Query_data(i)|| ' ' ||Schema_data(i));
            --exec TDM_USER.PKG_TESTDATA_GEN.EXTRACTDATA (Schema_data(i), 'CTLG_ITM_T', 'WHERE BUS_ID = '||Query_data(i)||'', 'PRODUCT');

      END LOOP; 


    -- need to loop through the schema names? or just delete all rows?
    -- delete from tdm_keys where schema_name = schemaString;

    -- Call the gen package for each qryString in a loop.
    --exec TDM_USER.PKG_TESTDATA_GEN.EXTRACTDATA (schemaString, 'CTLG_ITM_T', 'WHERE BUS_ID = '||qryString||'', 'PRODUCT');


END;
/


