CREATE OR REPLACE PACKAGE TDM_USER."PKG_TESTDATA_CATALOG" AS
/******************************************************************************
   NAME:       PKG_TESTDATA_ECOM
   PURPOSE:

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        2/9/2010             1. Created this package.
******************************************************************************/

  --FUNCTION MyFunction(Param1 IN NUMBER) RETURN NUMBER;
  PROCEDURE extractProduct(style_bus_id in varchar2, source_schema in varchar2);
  PROCEDURE extractProductCategories(style_bus_id in varchar2, source_schema in varchar2);
  PROCEDURE extractProductAndCategories(style_bus_id in varchar2, source_schema in varchar2);
  PROCEDURE extractOutfit (outf_bus_id in varchar2, source_schema in varchar2);
  PROCEDURE extractRelatedStyles (style_bus_id in varchar2, source_schema in varchar2);
  PROCEDURE extractCategoryProducts(cat_bus_id varchar2, source_schema in varchar2);
  PROCEDURE extractNonMerchCategories(source_schema in varchar2);
  PROCEDURE makeAprWipDependency(wip_schema_name IN VARCHAR2);
  PROCEDURE regenerateData (sname in varchar2 default null, del_tmp_table in varchar2 default 'Y');
  PROCEDURE makewipaprdependencyAll;  
END PKG_TESTDATA_CATALOG;
/
