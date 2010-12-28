CREATE OR REPLACE PACKAGE TRACE IS

  --$Header: /cvs/master/cjis/db/plsql/trace.pls,v 1.6.8.1 2007/09/27 19:03:01 comes Exp $

  ------------
  --  OVERVIEW
  --  This package provides tracing utility  using DBMS.PIPES
  --  and UTL_FILE technique. Messages will be sent over the pipes.
  --  based on the destination type the tracing message will be captured
  --  in files or in database table.

  -----------------------
  --  SPECIAL NOTES
  --  In order to compile this package, the SELECT privilege must be granted
  --  on the following dynamic views:
  --    v_$session, v_$mystat, v_$statname, v$sesstat, v$process
  --  In addition the EXECUTE privilege must be granted on DBMS_PIPE,UTL_FILE

  -----------------------
  --  Revisions
  --  Name         Date       Comments
  --  ------------ ---------- ----------------------------------------------
  --#  comes       11/14/02   created
  --#  comes       11/21/02   added procedure close_listener_process
  --#  comes       08/25/02   added statistics delta
  --#  vmandalika  03/06/08   added get_current_module_index, set_current_module_index,
  --#                         get_current_module_name and set_current_module_name routines
  --#  GLedford    12/29/2008 Performance enhancement

  -----------------------
  -- EXCEPTIONS

  -----------------------
  -- PUBLIC CONSTANTS

  DEFAULT_PIPE_NAME  CONSTANT varchar2(16) := 'DEBUG';
  FATAL              CONSTANT   PLS_INTEGER      := 0;
  WARNING            CONSTANT   PLS_INTEGER      := 1;
  INFO               CONSTANT   PLS_INTEGER      := 2;
  GREETING           CONSTANT   PLS_INTEGER      := 3;
  ERROR              CONSTANT   PLS_INTEGER      := 4;
  B4_LOOP            CONSTANT   PLS_INTEGER      := 5;
  IN_LOOP            CONSTANT   PLS_INTEGER      := 7;
  DEBUG              CONSTANT   PLS_INTEGER      := 9;
  -----------------------
  -- GLOBAL VARIABLES

  type sesstat_table is table of number(38) index by binary_integer;

  type trace_type is record (
         tracing                   boolean := FALSE,
         trace_id                  number  := 0,
         trace_flag                varchar2 ( 1 ),
         sid                       number  := 0,
         serial_number             number  := 0,
         process_id                v$process.spid%type,
         trace_level               number := 0,
         pipe_name                 varchar2(16) := DEFAULT_PIPE_NAME,
         trace_commit_interval     varchar2(16) := '1',
         trace_flush_interval      varchar2(16) := '10',
         application_name          varchar2(16) := 'YANTRA',
         program_name              varchar2(16) := 'ETL',
         destination_name          varchar2 ( 128 ),
         sesstat                   sesstat_table
         );

  u_trace                  trace_type;

  listener_id              utl_file.file_type;

  n_trace_commit_interval  varchar2(16);
  n_trace_flush_interval   varchar2(16);
  n_last_time              number;

  cursor c_stat(cp_sid in  number) is
    select statistic#, value
      from v$sesstat ss, v$session se
     where ss.sid = se.sid
       and se.sid||se.serial# = cp_sid;

  -----------------------
  -- PUBLIC FUNCTIONS / PROCEDURES

  FUNCTION get_current_module_index
    RETURN BINARY_INTEGER;

  PROCEDURE set_current_module_index (p_current_module_index IN BINARY_INTEGER);

  FUNCTION get_current_module_name
    RETURN VARCHAR2;

  PROCEDURE set_current_module_name (p_current_module_name IN VARCHAR2);

  FUNCTION get_last_debug_message
    RETURN VARCHAR2;

  procedure send_message (
               p_msg_type        in     number,
               p_message         in     varchar2 default null,
               p_pipe_name       in     varchar2 default DEFAULT_PIPE_NAME);

  --  Common Procedure to send message to the Listner.
  --  Pipe name is specified.

  procedure start_trace (
               p_application_name  in varchar2,
               p_program_name      in varchar2,
               p_trace_level       in number default 9,
               p_pipe_name         in varchar2 default DEFAULT_PIPE_NAME);
  --  Procedure to set the tracing ON by sending Start signal to the Listner
  --
  procedure stop_trace;
  --  Procedure to Stop tracing by sending Stop signal to the Listner.
  --
  procedure it (
               p_debug_msg       in     varchar2,
               p_trace_level     in     pls_integer default 9);
  --  Procedure to send the debugg message to be traced to the Listner.
  --
  procedure it(
               p_debug_stat      in     number,
               p_trace_level     in     pls_integer default 1);
  --  Overloading Procedure to send the statistics information to the Listner.
  --  This will be used specifically for tunning purpose.

  procedure listener(
               p_pipe_name       in     varchar2 default DEFAULT_PIPE_NAME,
               p_silent_mode     in     varchar2 default 'FALSE');
  --  Procedure to start the requested listener i.e. to open the requested pipe.
  --  This procedure also checks for the open listener. If the requested
  --   listener is already open this will give warning message.

  procedure stop_listener(
               p_pipe_name       in     varchar2 default DEFAULT_PIPE_NAME);
  --  Procedure to stop the requested listener.

  procedure close_listener_process(
               p_pipe_name       in     varchar2);
  --  Procedure to close all files, remove the pipes and client_info entry

/*
  procedure start_trace_statistics(
               n_trace_id        in     number,
           p_sid             in     number);
  procedure update_trace_statistics(
               n_trace_id        in     number,
           p_sid             in     number);
*/
  PROCEDURE log_error ( p_module_name IN VARCHAR2 );

END TRACE;
/
