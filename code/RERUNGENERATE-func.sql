CREATE OR REPLACE Function RERUNGENERATE
  
    RETURN varchar2
IS
    total_val number(6);

    cursor c1 is
    select qry --into qryString
    from tdm_data
    where qry like '%BUS%';

BEGIN

    total_val := 0;

    FOR qry_rec in c1
    LOOP
        --total_val := total_val + employee_rec.monthly_income;
        -- qry_rec = qry_rec + ' ' + qry_rec;
    END LOOP;

    RETURN qry_rec; 

END;
/
