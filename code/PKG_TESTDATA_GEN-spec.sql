CREATE OR REPLACE PACKAGE TDM_USER."PKG_TESTDATA_GEN" AS
/******************************************************************************
   NAME:       PKG_TESTDATA_GEN
   PURPOSE:    Capture and Create Test Data for schema

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        11/12/2009   ggupta          1. Created this package.
******************************************************************************/

   TYPE pKey is RECORD
   (
       table_name VARCHAR2(200),
       pk_col_name VARCHAR2(100),
       col_pos VARCHAR2(1)
   );
   TYPE pKeys is varray(1000) of pKey;
   TYPE tblpKeys IS TABLE of XMLTYPE index by VARCHAR2(4000);
   my_tbluiKeys tblpKeys;
   my_tblpKeys tblpKeys;
 
  TYPE recChld is RECORD
  (
     constraint_name VARCHAR2(100),
     c_table_name VARCHAR2(100),
     c_column_name VARCHAR2(100),
     c_schema_name VARCHAR2(100),
     p_table_name VARCHAR2(200),
     p_column_name VARCHAR2(100),
     position VARCHAR2(1),
     p_schema_name VARCHAR2(100)
  );

  TYPE vRecChld is varray(500) of recChld;
  TYPE tblRecChld is TABLE of vRecChld index by VARCHAR2(6000);
  my_tblRecChld tblRecChld;
  
  TYPE recParent is RECORD
  (
     constraint_name VARCHAR2(100),
     c_table_name VARCHAR2(100),
     c_column_name VARCHAR2(100),
     c_schema_name VARCHAR2(100),
     p_table_name VARCHAR2(200),
     p_column_name VARCHAR2(100),
     position VARCHAR2(1),
     p_schema_name VARCHAR2(100)
  );

  TYPE vRecParent is varray(500) of recParent;
  TYPE tblRecParent is TABLE of vRecParent index by VARCHAR2(4000);
  my_tblRecParent tblRecParent;
  
  TYPE col_info IS RECORD (column_name user_tab_columns.column_name%type);

  TYPE val_info IS RECORD (column_value VARCHAR2(4000));

  TYPE mview_target IS RECORD (owner VARCHAR2(30), table_name VARCHAR2(30));

--  FUNCTION upsert( table_name IN VARCHAR2, cols IN VARCHAR2, col_values IN VARCHAR2) RETURN NUMBER;
  PROCEDURE savePrimaryColumns(sname in VARCHAR2) ;
  
  PROCEDURE saveChildRelationships(sname in VARCHAR2, ename in VARCHAR2) ;

  PROCEDURE saveParentRelationships(sname in VARCHAR2, ename in VARCHAR2) ;  
  
  FUNCTION getCols(sname IN varchar2, tab_name IN VARCHAR2) RETURN VARCHAR2;

  PROCEDURE saveInserts(sname IN varchar2, tab_name IN VARCHAR2, where_clause IN VARCHAR2, ename IN VARCHAR2,data_flag VARCHAR2, calling_tab_name VARCHAR2 default null, err_seqno IN VARCHAR2 default null);

  PROCEDURE processParents(sname IN varchar2, tab_name IN VARCHAR2, doc IN xmlType, ename IN VARCHAR2);

  PROCEDURE processChildren(sname IN varchar2, tab_name IN VARCHAR2, doc IN xmlType, ename IN VARCHAR2);

  PROCEDURE getData(sname IN varchar2, ename IN varchar2, tname IN VARCHAR2, w_cls IN VARCHAR2);

  PROCEDURE clean_keys(sname IN varchar2);

  PROCEDURE extractData(sname IN varchar2, tab_name IN VARCHAR2, where_clause IN VARCHAR2, ename IN VARCHAR2);

  PROCEDURE regenerateData(sname IN varchar2 default null, del_tmp_table in varchar2 default 'Y');

  function getMViewBaseTable(mview_nm IN varchar2, owner_nm IN varchar2) return varchar2;

  PROCEDURE processMviews(sname IN varchar2);
  
   PROCEDURE test_category;
   PROCEDURE test_product;
   PROCEDURE preOperations(sname in VARCHAR2);
   --preOpsFlag VARCHAR2(1) := 'N';
--  PROCEDURE dequeueData;
--  PROCEDURE kill;

END PKG_TESTDATA_GEN;
/
