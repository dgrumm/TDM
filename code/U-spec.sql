CREATE OR REPLACE PACKAGE U IS

  ------------
  --  OVERVIEW
  --  This package provides global procedures and functions and Variables

  ------------
  --  SPECIAL NOTES
  --  In order to compile this package, the SELECT privilege must be granted
  --  on the following dynamic views:
  --    v_$session, v_$mystat, v_$statname, v$sesstat, v$process

  -----------------------
  --  Revisions
  --   Name        Date       Comments
  --  ----------- ---------- ----------------------------------------------
  --#  sahoos      11/14/02   created
  --#  sahoos      11/25/02   User Defined Exceptions Added
  --#  sahoos      02/05/03   xmltype gobal variable Added


  -----------------------
  -- EXCEPTIONS

    SYSTEM_PARAMETER_NOT_FOUND               exception;
    pragma exception_init(SYSTEM_PARAMETER_NOT_FOUND,    -20001);

    LISTENER_ALREADY_RUNNING                 exception;
    pragma exception_init(LISTENER_ALREADY_RUNNING,      -20008);


  -----------------------
  -- GLOBAL VARIABLES

  -----------------------
  -- PUBLIC FUNCTIONS / PROCEDURES


  function get_sid(
              p_serial#                  out number)
             return number;
  --  Function to get the sid and it's serial number

  function get_sid
             return number;
  --  Function to get the sid concatenated with it's serial number

  function get_process_id
             return varchar2;
  --  Function to get the operating process id

  procedure say(
              p_text                  in     varchar2);
  --  Wrapper procedure for dbms_output.put_line

END U;
/
