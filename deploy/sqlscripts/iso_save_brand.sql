set serveroutput on
WHENEVER SQLERROR EXIT 3
VARIABLE ret NUMBER
BEGIN
  :ret := 0;
   PKG_TESTDATA_CATALOG_SAVE.saveEcomCatalogData('&1');
END;
/
EXIT :ret

