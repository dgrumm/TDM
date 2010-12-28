CREATE OR REPLACE PACKAGE TDM_USER."PKG_TESTDATA_CATALOG_SAVE" AS
/******************************************************************************
   NAME:       PKG_TESTDATA_ECOM_SAVE
   PURPOSE:

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        2/23/2010             1. Created this package.
******************************************************************************/
  TYPE schema_map is table of tdm_data.schema_name%type
    index by tdm_data.schema_name%type;

  FUNCTION load_tools_schema_map return schema_map;
  FUNCTION load_ecom_schema_map return schema_map;
  FUNCTION load_test_schema_map return schema_map;
  PROCEDURE saveWipAprData(wip_schema_name IN VARCHAR2, my_schema_map IN schema_map default load_tools_schema_map);
  PROCEDURE saveEcomCatalogData(apr_schema_name IN VARCHAR2, my_schema_map IN schema_map default load_ecom_schema_map);
  PROCEDURE saveWipAprDataForABrand(wip_schema_name IN VARCHAR2);
  PROCEDURE saveWipAprDataPPC;
  PROCEDURE saveWipAprDataTOOLSVC;
  PROCEDURE saveEcomCatalogDataPPC(my_schema_map IN schema_map default load_ecom_schema_map);

END PKG_TESTDATA_CATALOG_SAVE;
/
