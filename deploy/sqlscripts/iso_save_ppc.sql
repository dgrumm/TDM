set serveroutput on
WHENEVER SQLERROR EXIT 3
VARIABLE ret NUMBER
BEGIN
  :ret := 0;
   PKG_TESTDATA_CATALOG_SAVE.saveEcomCatalogDataPPC;
END;
/
EXIT :ret

