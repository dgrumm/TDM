CREATE OR REPLACE PACKAGE BODY TRACE IS

/* $URL: https://svn-vip/cjis/COSE/cjis/branches/cjis2.6.4.8/db/plsql/trace.plb $
   $Rev: 5554 $
   $Author: comes $
   $Date: 2009-06-10 07:21:16 -0700 (Wed, 10 Jun 2009) $
   $Id$
*/
  --  $Header: /cvs/master/cjis/db/plsql/trace.plb,v 1.16.2.3 2007/11/20 00:49:45 comes Exp $
  --
  --  Procedure to start a new trace session.
  --    When the listener receives a start_trace request, it will get information
  --    for the program to trace from trace_info and enable the tracing.
  --    If there is a problem sending messages to the listener, the tracing will be
  --    automatically disabled in the session to avoid clogging the pipe and affect
  --    performance.
  --
  --    Additionally, the SQL*Trace event 10046 can be set.  Simply set the EVENT_NUMBER
  --    and the EVENT_LEVEL in the TRACE_INFO table.
  --
  -- JYang    10/04/05   commented out commit in start_trace() and add timeout 1 sec in send_message()
  -- scome    10/17/05   read status from programs table instead of trace_info.
  -- scome    11/22/05   removed the logging of "stop trace".
  -- JYang    01/10/06   Bug 64: Chg start_trace() to send trace_level for each session to the listener,
  --                             Chg listener() to print "close trace" info  base on trace_level.
  --                             commented out some loggings in listener().
  --                     Chg stop_trace() to send type 1 msg to listener only if u_trace.tracing = true
  -- scome    08/13/07   Cleared the trace file handler explicitely after sporadic INVALID OEPRATION
  --                     exception raised under Oracle 10gR2.
  -- scome    09/27/07   Added "last_message" sent
  -- mandalv  03/06/08   Added get_current_module_index, set_current_module_index,
  --                     get_current_module_name and set_current_module_name routines
  -- mandalv  07/17/08   In IT procedure, Changed pv_current_module_index to
  --                     LEAST(pv_current_module_index,4), so indentation doesn't
  --                     run into more than 3 levels (and doesnt't take up more 12 char)
  -- GLedford 12/29/2008 Performance enhancement
  -- mandalv  01/05/2009 NCC-214: Made changes related to writing trace info to a table

  --* Package Variables
  pv_current_module_index NUMBER := 1;
  pv_current_module_name  VARCHAR2(200) := '';
  pv_last_message         VARCHAR2(254) := NULL;

  TYPE trace_module_rec IS RECORD (
     module_name VARCHAR2(100),
     start_time  TIMESTAMP,
     end_time    TIMESTAMP
     --level       NUMBER
     --error_count NUMBER
     );

  TYPE trace_module_tab IS TABLE OF trace_module_rec
  INDEX BY BINARY_INTEGER;

  tmt trace_module_tab;

  --* vmandalika - 12/01/2006 - trace_file_tab keeps track of the trace_files opened by start_trace
  --* routine
  TYPE trace_file_rec IS RECORD (
     application_name VARCHAR2(50),
     program_name     VARCHAR2(50),
     start_time       TIMESTAMP,
     end_time         TIMESTAMP
     );

  TYPE trace_file_tab IS TABLE OF trace_file_rec
  INDEX BY BINARY_INTEGER;

  trace_file_list TRACE_FILE_TAB;

  FUNCTION get_current_module_index
    RETURN BINARY_INTEGER
  IS
  BEGIN
     RETURN pv_current_module_index;
  END get_current_module_index;

  PROCEDURE set_current_module_index (p_current_module_index IN BINARY_INTEGER)
  IS
  BEGIN
     pv_current_module_index := p_current_module_index;
  END set_current_module_index;

  FUNCTION get_current_module_name
    RETURN VARCHAR2
  IS
  BEGIN
     RETURN pv_current_module_name;
  END get_current_module_name;

  PROCEDURE set_current_module_name (p_current_module_name IN VARCHAR2)
  IS
  BEGIN
     pv_current_module_name := p_current_module_name;
  END set_current_module_name;

  FUNCTION get_last_debug_message
    RETURN VARCHAR2
    IS
    BEGIN
      RETURN pv_last_message;
    END get_last_debug_message;

  FUNCTION elapsed_time_formatted (
     p_start_timestamp IN TIMESTAMP,
     p_end_timestamp   IN TIMESTAMP
     )
    RETURN VARCHAR2
  IS

     v_elapsed_time           VARCHAR2(200);
     v_elapsed_time_formatted VARCHAR2(200);
     v_hr                     NUMBER;
     v_min                    NUMBER;
     v_sec                    NUMBER;


  BEGIN

     v_elapsed_time := SUBSTR(
                          p_end_timestamp - p_start_timestamp,
                          INSTR( p_end_timestamp - p_start_timestamp, ' ', 1 )+1
                          );

     v_hr  := TO_NUMBER ( SUBSTR ( v_elapsed_time, 1, INSTR ( v_elapsed_time,':', 1 ) -1 ) );
     v_min := TO_NUMBER ( SUBSTR ( v_elapsed_time, INSTR ( v_elapsed_time,':',1,1 ) +1, INSTR (v_elapsed_time,':',1,2 ) -INSTR ( v_elapsed_time,':',1,1)-1 ) );
     v_sec := TO_NUMBER ( SUBSTR ( v_elapsed_time, INSTR ( v_elapsed_time,':',1,2 ) +1 ) );

     v_sec := ( v_hr*3600 ) + ( v_min*60 ) + ( v_sec );
     v_elapsed_time_formatted := v_sec|| ' sec';

     RETURN v_elapsed_time_formatted;

  END elapsed_time_formatted;


  procedure start_trace(
              p_application_name in varchar2,
              p_program_name     in varchar2,
              p_trace_level      in number default 9,
              p_pipe_name        in varchar2 default DEFAULT_PIPE_NAME)
            is

       n_listeners       number;   --* SID of the running listener
       v_app_name        varchar2 ( 16 )  := 'TDM';
       v_trace_flag      varchar2 (  1 )  := 'N';
       v_file_index      number;

    begin

      IF ( pv_current_module_index IS NULL )
      THEN
         --* vmandalika - 11/29/2006 - Initializing current module variables
         pv_current_module_index := 1;
         pv_current_module_name  := '';
         tmt.DELETE;
      END IF;

      --* vmandalika - 12/01/2006 - If there are no OPEN trace files trace_file_list array, add the new AppName/PgmName
      --* trace file as the first element of the array. If there are already some open trace file names in the array,
      --* add this file name as the last element of the list.
      IF ( trace_file_list.COUNT = 0 )
      THEN
         v_file_index := 1;
      ELSE
         v_file_index := trace_file_list.LAST+1;
      END IF;

      trace_file_list ( v_file_index ).application_name := p_application_name;
      trace_file_list ( v_file_index ).program_name     := p_program_name;
      trace_file_list ( v_file_index ).start_time       := SYSTIMESTAMP;

      --* Only executes if the current session is not already in trace mode
      if ( not ( u_trace.tracing ) or
           ( u_trace.application_name <> nvl ( p_application_name, 'null' ) )
         )
      then

        n_last_time := dbms_utility.get_time();

        --u.say('start_trace called');

        u_trace.application_name  := p_application_name;
        u_trace.program_name      := p_program_name;
        u_trace.trace_level       := p_trace_level;
        u_trace.pipe_name         := p_pipe_name;

        -- Get the tracing information for this application/program
        --  The matching combination is not case sensitive
        --u.say('application_name='||p_application_name);
        u_trace.trace_flag  := 'Y';
        u_trace.trace_level := 9;

        if (u_trace.trace_flag = 'Y') then

          -- get the current sid and serial# from the v$session view
          u_trace.sid := u.get_sid(u_trace.serial_number);
          u_trace.process_id := u.get_process_id;

          -- Set an event if specified in the configuration
          /*  Not implemented yet.  Will need to add event_number and event_level
              columns to the programs table.
          if ( u_trace.event_number is not null and u_trace.event_number > 10000 ) then
            sys.dbms_system.set_ev(u_trace.sid, u_trace.serial_number,
                               u_trace.event_number, u_trace.event_level, '');
          end if;
          */

          u_trace.destination_name := p_program_name||'_'||
                                      to_char(sysdate,'MMDD')||
                                      '_'||u_trace.sid||'.trc';

          select trace_seq.nextval
            into u_trace.trace_id
            from dual;

          -- GLedford 12/29/2008 - don't execute query if already tracing to improve performance
          IF ( NOT u_trace.tracing )
          THEN

             --* Check if listener is running for this pipe
             select count(*)
               into n_listeners
               from v$session
              where client_info = u_trace.pipe_name;

             if ( n_listeners = 1 ) then
               u_trace.tracing := TRUE;
             end if;

          END IF;

          -- send the message to the listener that trace started for this session
          send_message(3);
          --u.say('msg type 3 sent');
        else
          u_trace.tracing := FALSE;
        end if;

      end if;

    exception
      when NO_DATA_FOUND then
        null;
      when OTHERS then
        u.say(sqlerrm ||' In start trace' );

    end; -- Start_trace

  procedure send_message (
              p_msg_type         in     number,
              p_message          in     varchar2 default null,
              p_pipe_name        in     varchar2 default DEFAULT_PIPE_NAME)
	    is

      n_return     number := 0;
      /* vmandalika - 01/05/2009 - NCC-214 */
      v_trace_message_no VARCHAR2(128);
      v_message_id       VARCHAR2(128);
      v_application_name VARCHAR2(128);
      v_program_name     VARCHAR2(128);

    begin

      --u.say('sending msg type '||p_msg_type||' '||p_message);

      -- Message Types
      --   0  => debug message
      --   1  => stop trace
      --   2  => kill listener
      --   3  => start trace

      dbms_pipe.pack_message(p_msg_type);
      dbms_pipe.pack_message(u_trace.sid);

      if p_msg_type = 0 then

        dbms_pipe.pack_message(p_message);
        dbms_pipe.pack_message(v_trace_message_no);
        dbms_pipe.pack_message(v_message_id);
        dbms_pipe.pack_message (u_trace.application_name);
        dbms_pipe.pack_message (u_trace.program_name);
        dbms_pipe.pack_message (u_trace.trace_level);

      end if;

      if p_msg_type = 3 then
        dbms_pipe.pack_message (u_trace.trace_id);
        dbms_pipe.pack_message (u_trace.destination_name);
        dbms_pipe.pack_message (u_trace.application_name);
        dbms_pipe.pack_message (u_trace.program_name);
        dbms_pipe.pack_message (u_trace.trace_level);
      end if;

      n_return := dbms_pipe.send_message(nvl(p_pipe_name,u_trace.pipe_name), 1);
      if n_return <> 0 then
        -- problem with the pipe, abort the trace!
        u.say('problem with the pipe, abort the trace!');
        u_trace.tracing := FALSE;
      end if;

      --u.say('message sent...');
    exception
      when others then
        u_trace.tracing := false;
    end;

  procedure stop_trace is

    begin

       IF u_trace.tracing THEN
         send_message(1);
         u_trace.tracing := false;
       END IF;

       --* vmandalika - 12/01/2006 - When STOP_TRACE() is called, we need to stop adding trace log entries to
       --* the most current AppName/PgmName trace file, and start writing log entries to the previous AppName/PgmName
       --* trace file.
       --* So, we are deleting the last AppName/PgmName added to trace_file_list array, and if the array
       --* still retains an OPEN AppName/PgmName trace file, tracing control is returned to that file.
       IF trace_file_list.COUNT > 0
       THEN

          trace_file_list.DELETE( trace_file_list.LAST );

          IF trace_file_list.COUNT > 0
          THEN
             trace.start_trace(
                trace_file_list( trace_file_list.LAST).application_name,
                trace_file_list( trace_file_list.LAST).program_name
                );
          END IF;

       END IF;

    exception
      when OTHERS then
         u.say(sqlerrm ||'In Stop trace procedure' );

    end; -- Stop_trace


  procedure it(
              p_debug_stat       in     number,
              p_trace_level      in     pls_integer default 1)
	    is

      n_value      v$sesstat.value%type;
      n_delta      number(38);
      v_message    varchar2 ( 256 );

    begin

      if u_trace.tracing then
        select a.value, b.name
          into n_value, v_message
          from v$mystat a,v$statname b
         where a.statistic# = b.statistic#
           and a.statistic# = to_number(p_debug_stat);

         begin
           n_delta := n_value - u_trace.sesstat(p_debug_stat);
           v_message := 'Stat: '||n_delta||'/'||n_value||' => '||v_message;
         exception
           when no_data_found then
              v_message := 'Stat: '||n_value||' => '||v_message;
         end;
         u_trace.sesstat(p_debug_stat) := n_value;

         if p_trace_level <= u_trace.trace_level then
            send_message(0, v_message);
	    --u.say('trace_level '||u_trace.trace_level);
	 end if;

      end if;

    exception
      when OTHERS then
        u.say(sqlerrm ||'In Debug Statistics' );

    end;

  procedure it (
              p_debug_msg        in     varchar2,
              p_trace_level      in     pls_integer default 9)
	    is
    begin
      pv_last_message := substr(p_debug_msg,1,254);
      if ( u_trace.tracing ) then
         if ( p_trace_level <= u_trace.trace_level ) then

            --* vmandalika - 11/29/2006 - Concatenated current module name with proper indentation to
            --* p_debug_msg

            if ( trim(pv_current_module_name) is not null )
            then
               send_message(
                 0,
                 SUBSTR( LPAD(' ', 4*(LEAST(pv_current_module_index,4)-1),  '.')||
                         '('||pv_current_module_name||'): '||
                         p_debug_msg, 1, 250
                         )
                 );
            else
               send_message(
                 0,
                 SUBSTR( LPAD(' ', 4*(LEAST(pv_current_module_index,4)-1),  '.')||
                         p_debug_msg, 1, 250
                         )
                 );
            end if;

	 end if;
      end if;

    exception
      when OTHERS then
        u.say('In Debug Msg'||'session_id:'||u_trace.sid||
             '--'||p_debug_msg||'--'||sqlerrm);

    end;

  procedure stop_listener(
              p_pipe_name        in      varchar2 default DEFAULT_PIPE_NAME)
	    is

    begin
       send_message(2, null, p_pipe_name);
    end;

-- LISTENER

  procedure listener (
              p_pipe_name        in      varchar2 default DEFAULT_PIPE_NAME,
              p_silent_mode      in      varchar2 default 'FALSE'
	      )
	    is

      n_count                  pls_integer := 0;
      n_file_record_count      number(38)  := 0; --record count for file
      n_table_record_count     pls_integer := 0; --record count for table
      n_msg_type               pls_integer := 0;
      n_return                 pls_integer;
      n_sid                    pls_integer;

      v_time                   varchar2(24);
      v_directory              varchar2(128);   --The file location
      v_app_name               varchar2(16)  := 'CHRONIX';
      v_message                varchar2(1024);
      v_dbg                    varchar2(80);
      v_filemode               varchar2(1) := 'A';

      b_exists boolean;
      n_file_length number;
      n_blocksize   number;
      v_tmp_handler      utl_file.file_type;

      LISTENER_ALREADY_RUNNING exception;

      pragma exception_init(LISTENER_ALREADY_RUNNING, -20008);

      file_id                  utl_file.file_type;

      type trace_table is table of trace_type index by binary_integer;
      trace_tab    trace_table;

      type file_table is table of utl_file.file_type index by binary_integer;

      /* vmandalika - 01/05/2009 - NCC-214 */
      file_tab     file_table;
      v_message_id       VARCHAR2(128);
      v_trace_message_no VARCHAR2(128);
      v_application_name VARCHAR2(128);
      v_program_name     VARCHAR2(128);
      n_trace_level      NUMBER;

      /*
      procedure debug(p_file_id in utl_file.file_type, p_text in varchar2, p_sid in number default null )
      is
        v_time    varchar2(25);
      begin
          v_time := substr(to_char(systimestamp,'MM/DD/YY HH24:MI:SS.FF'),1,21);
          utl_file.put_line(p_file_id, v_time||'--> '||p_sid||'-'||p_text);
      end;
      */

    begin

      u.say('starting listener...');
      -- check to see if no other listener is running
      select count(*) into n_count
        from v$session
       where client_info = upper(p_pipe_name);

      if n_count > 0 then
        --u.say('Listener already running!');
        raise u.LISTENER_ALREADY_RUNNING;
      else
        -- set the listner name
        n_return := dbms_pipe.create_pipe(p_pipe_name, 32768, FALSE);
        dbms_application_info.set_client_info(upper(p_pipe_name));
      end if;

      u_trace.trace_commit_interval := 1;
      u_trace.trace_flush_interval := 1;

      --* Start the listener trace
      v_directory := 'EXPORT';
      u.say('opening listener trace in '||v_directory);
      listener_id := utl_file.fopen(v_directory, 'listener_'||p_pipe_name||
                          '_'||to_char(sysdate,'YYYYMMDDHH24MI')||'.trc', 'W');
      -- start the listner in an infinite loop
      loop  <<main_loop>>

        begin

          v_dbg := 'start loop';
          n_return := dbms_pipe.receive_message(p_pipe_name);
          --u.say('received msg');
          --select to_char(sysdate,'DD-MM-YY HH24:MI:SS') into v_time from dual;
          v_time := substr(to_char(systimestamp,'MM/DD/YY HH24:MI:SS.FF'),1,21);

          if n_return = 0 then
            -- success
            --debug(listener_id, 'received message');
            dbms_pipe.unpack_message(n_msg_type);

            v_dbg := 'unpack n_msg_type';
            --debug(listener_id, 'unpacked msg_type='||n_msg_type);
            dbms_pipe.unpack_message(n_sid);
            v_dbg := 'unpack n_sid';
            --debug(listener_id, 'unpacked', n_sid);
            --u.say('type='||n_msg_type||' sid='||n_sid);

            if n_msg_type = 0 then

              /* vmandalika - 01/05/2009 - NCC-214 */
              -- normal debug message
              dbms_pipe.unpack_message(v_message);
              dbms_pipe.unpack_message(v_trace_message_no);
              dbms_pipe.unpack_message(v_message_id);
              dbms_pipe.unpack_message(v_application_name);
              dbms_pipe.unpack_message(v_program_name);
              dbms_pipe.unpack_message(n_trace_level);

              --debug(listener_id, 'message len='||length(v_message), n_sid);
              v_dbg := 'unpack '||length(v_message);
              --debug(listener_id, v_dbg, n_sid);
              --u.say('unpacked msg len='||length(v_message));

              begin
                  --debug(listener_id, 'writing file log'||n_sid);
                  utl_file.put_line(file_tab(n_sid), v_time||' ['||trace_tab(n_sid).program_name||'] - '||substr(v_message,1,250));
                  --debug(listener_id, 'write complete', n_sid);
                  v_dbg := 'utl_filed';
                  n_file_record_count := n_file_record_count+1 ;
                  --debug(listener_id, 'record '||n_file_record_count, n_sid);

              exception
                when NO_DATA_FOUND then
                  --debug(listener_id, 'NO DATA FOUND', n_sid);
                  -- ignore the message
		  --utl_file.put_line(listener_id,v_time||'no trc('||n_sid||'): '||substr(v_message,1,200));
                  null;
              end;

            elsif n_msg_type = 1 then
              -- stop tracing for current session
              -- utl_file.put_line(listener_id,v_time||' stop trace for '||n_sid);
              --debug(listener_id, 'STOP TRACING', n_sid);
              begin

                /* vmandalika - 01/05/2009 - NCC-214 */
                --VMANDALIKA - COMMENTED THIS OUT ON 02/19/2009

                  if utl_file.is_open(file_tab(n_sid)) then
                    IF trace_tab(n_sid).trace_level = 9 THEN
                      utl_file.put_line(file_tab(n_sid), v_time||' '||
                          'close trace file');
                      --debug(listener_id, 'file closed', n_sid);
                    END IF;
                    utl_file.fflush(file_tab(n_sid));
                    utl_file.fclose(file_tab(n_sid));
                    -- added the next line after experiencing sporadic invalid operations
                    -- under 10g.
                    file_tab(n_sid) := null;
                    --u.say('stopped trace--'|| 'after closing file');
                    IF trace_tab(n_sid).trace_level = 9 THEN
                       utl_file.put_line(listener_id,v_time||' file closed');
                    END IF;
                  end if;

                trace_tab.DELETE(n_sid);
                --debug(listener_id, 'session '||n_sid||' removed from trace_tab', n_sid);
                --utl_file.put_line(listener_id,v_time||
                --                  ' removed '||n_sid||' from trace_tab');
                commit;

              exception
                when no_data_found then
                  --debug(listener_id, 'NO DATA FOUND for message type 1', n_sid);
                  -- ignore the message
                  --utl_file.put_line(listener_id,v_time||
                  --                  ' no data found here...');
                  null;

              end;

            elsif n_msg_type = 2 then
               --  kill the listner process
              utl_file.put_line(listener_id,v_time||
                                ' kill message received. shuting down listener!');
              while trace_tab.count > 0 loop
                n_sid := trace_tab.last;
                IF trace_tab(n_sid).trace_level = 9 THEN
                   utl_file.put_line(listener_id,v_time||
                                  'closing sid# '||n_sid);
                END IF;
                  if utl_file.is_open(file_tab(n_sid)) then
                    IF trace_tab(n_sid).trace_level = 9 THEN
                      utl_file.put_line(file_tab(n_sid), v_time||' '||
                        'listener closing trace file');
                    END IF;
                    trace_tab.delete(n_sid);
                    utl_file.put_line(listener_id,v_time||' close ok!');
                  end if;
              end loop;

              close_listener_process(p_pipe_name);
              commit;
              exit;  -- Exit from the loop

            elsif n_msg_type = 3 then
              --debug(listener_id, 'start tracing session', n_sid);
              --debug(listener_id, 'already tracing '||trace_tab.count||' sessions and '||file_tab.count||' files open');
              -- start tracing for current session
              v_dbg := 'received msg_type 3';
              trace_tab(n_sid).sid := n_sid;
              v_dbg := 'register sid '||n_sid||' for trace';
              --debug(listener_id, v_dbg, n_sid);
              dbms_pipe.unpack_message(trace_tab(n_sid).trace_id);
              dbms_pipe.unpack_message(trace_tab(n_sid).destination_name);
              dbms_pipe.unpack_message(trace_tab(n_sid).application_name);
              dbms_pipe.unpack_message(trace_tab(n_sid).program_name);
              dbms_pipe.unpack_message(trace_tab(n_sid).trace_level);
              v_dbg := 'unpacked message successful';
              --debug(listener_id, v_dbg, n_sid);
              /*
              begin
                select event_number, event_level
                  into trace_tab(n_sid).event_number,
                       trace_tab(n_sid).event_level
                  from trace_info
                 where program_name = trace_tab(n_sid).program_name
                   and application_name = trace_tab(n_sid).application_name;
                 v_dbg := 'found trace info for '||trace_tab(n_sid)||'.'||trace_tab(n_sid).program_name;
                 --debug(listener_id, v_dbg, n_sid);
                 if trace_tab(n_sid).event_number is not null and
                    trace_tab(n_sid).event_level is not null then
                      v_dbg := 'find session '||n_sid;
                      --debug(listener_id, v_dbg, n_sid);
                      select serial#
                        into trace_tab(n_sid).serial_number
                        from v$session
                       where sid = n_sid;
                      sys.dbms_system.set_ev(n_sid, trace_tab(n_sid).serial_number,
                                         trace_tab(n_sid).event_number, trace_tab(n_sid).event_level,'');
                      --debug(listener_id, 'event set', n_sid);
                 end if;
              exception
                when NO_DATA_FOUND then
                  --debug(listener_id, 'uh oh - NO DATA FOUND...', n_sid);
                  null;
                  u.say('no data found after '||v_dbg);
              end;
              */
                v_dbg := 'tracing into file for '||n_sid;
                --debug(listener_id, v_dbg, n_sid);
                v_directory := 'EXPORT';
                --u.say('opening file '||n_sid||' in '||v_directory);

                utl_file.put_line(listener_id,v_time||' opening '||
                                  v_directory||'/'||trace_tab(n_sid).destination_name);

                utl_file.fgetattr(v_directory,trace_tab(n_sid).destination_name,b_exists,n_file_length,n_blocksize);
                if (b_exists) then
                  v_filemode := 'A';
                  --debug(listener_id, 'file exists, len='||n_file_length||', blocksize='||n_blocksize);
                else
                  v_filemode := 'W';
                  --debug(listener_id, 'file not found');
                end if;

                --debug(listener_id, 'check if file_tab('||n_sid||') has any handler');

                begin
                  v_tmp_handler := file_tab(n_sid);
                  --debug(listener_id, 'humm... handler still there.  close the file');
                  utl_file.fclose(v_tmp_handler);
                  file_tab(n_sid) := null;
                exception
                  when no_data_found then
                    null;
                    --debug(listener_id, 'NO HANDLER found');
                end;

                /*
                if (utl_file.is_open(file_tab(n_sid))) then
                  debug(listener_id, 'file is already open');
                else
                  debug(listener_id, 'file is closed');
                end if;
                */
		v_dbg := 'about to open file '||trace_tab(n_sid).destination_name;
                --debug(listener_id, v_dbg, n_sid);
                file_tab(n_sid) := utl_file.fopen(v_directory,
                                         trace_tab(n_sid).destination_name, v_filemode);
                if ( trace_tab(n_sid).trace_level = 9 ) then
                  utl_file.put_line(file_tab(n_sid), v_time||' start tracing');
                end if;

            end if;  -- n_msg_type

          end if;  -- return

        exception
          when UTL_FILE.INVALID_PATH then
            u.say(n_sid||' Problem in STOP TRACE ' || 'Invalid Path');
            --debug(listener_id, 'INVALID_PATH');

	  when UTL_FILE.INVALID_MODE then
            u.say(n_sid||' Problem in STOP TRACE ' || 'Invalid Mode');
            --debug(listener_id, 'INVALID_MODE');

	  when UTL_FILE.INVALID_FILEHANDLE then
            u.say(n_sid||' Problem in STOP TRACE ' || 'Invalid FILEHANDLE');
            --debug(listener_id, 'INVALID_FILEHANDLE');

	  when UTL_FILE.INVALID_OPERATION then
            u.say(n_sid||' Problem in STOP TRACE ' || 'Invalid OPERATION');
            --debug(listener_id, 'INVALID_OPERATION');

	  when UTL_FILE.READ_ERROR  then
            u.say(n_sid||' Problem in STOP TRACE ' || ' READ_ERROR ');
            --debug(listener_id, 'READ_ERROR');

	  when UTL_FILE.WRITE_ERROR  then
            u.say(n_sid||' Problem in STOP TRACE ' || ' WRITE_ERROR');
            --debug(listener_id, 'WRITE_ERROR');

          when UTL_FILE.INTERNAL_ERROR  then
            u.say(n_sid||' Problem in STOP TRACE ' || 'INTERNAL_ERROR ');
            --debug(listener_id, 'INTERNAL_ERROR');
        end;
          --u.say('trace_commit_interval before commit '||
	  --                u_trace.trace_commit_interval );
          --u.say('table record count before commit '||n_table_record_count );

          if mod(n_table_record_count,u_trace.trace_commit_interval) = 0 then
	     commit;
             n_table_record_count := 0;
          end if;

          if n_file_record_count >= u_trace.trace_flush_interval then
             utl_file.fflush(file_tab(n_sid));
             n_file_record_count := 0;
	  end if;

          --debug.log(listener_id,'flushing...');
          utl_file.fflush(listener_id);

      end loop;

    exception
      when LISTENER_ALREADY_RUNNING  then
        --raise_application_error(-20008 ,'Listener ' ||upper(p_pipe_name)
	--                         ||'  '|| 'already started');
        if p_silent_mode = 'FALSE' then
            u.say('Listener already running!');
        end if;
      when OTHERS then
        close_listener_process(p_pipe_name);
        u.say(v_dbg);
        u.say('listener: ' ||sqlerrm);
    end;

  procedure close_listener_process(
               p_pipe_name       in     varchar2) is

      n_return       pls_integer;

    begin
      utl_file.fclose_all; -- Close all Open files
      n_return := dbms_pipe.remove_pipe(p_pipe_name);
      dbms_application_info.set_client_info(NULL);
    end;

/*
  procedure start_trace_statistics(
              n_trace_id         in     number,
	      p_sid              in     number)
	    is

    begin
      for r_stat in c_stat(p_sid) loop
        insert into trace_statistics(
	                             trace_id,
				     statistic_number,
				     value
				     )
                              values (
			             n_trace_id,
				     r_stat.statistic#,
				     r_stat.value);
      end loop;

    end;
*/
/*
  procedure update_trace_statistics(
              n_trace_id         in     number,
	      p_sid              in     number)
	    is

    begin
      --utl_file.put_line(listener_id,'update trace_statistics');
      for r_stat in c_stat(p_sid) loop
        update trace_statistics
           set value = r_stat.value - value
         where trace_id = n_trace_id
           and statistic_number = r_stat.statistic#;
      end loop;
      --utl_file.put_line(listener_id,'updated trace_statistics');

    end;
*/

  PROCEDURE log_error ( p_module_name IN VARCHAR2 )
  IS
     v_index NUMBER;
  BEGIN

     trace.it('Oracle Error. SQLCode = '||SQLCODE||'; SQL Error Msg = '|| SUBSTR(SQLERRM,1,200) );

  END log_error;

END TRACE;
/


