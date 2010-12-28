CREATE OR REPLACE PACKAGE BODY U AS

  function get_sid(
              p_serial#             out number)
             return number
	   is

      n_sid    number;

    begin
      select se.sid, se.serial#
        into n_sid, p_serial#
        from v$session se,
             v$mystat  ms
       where se.sid = ms.sid
         and ms.statistic# = 0;

      return n_sid;

    end;

  function get_sid
             return number
	   is

      n_sid    number;

    begin
      select se.sid||se.serial#
        into n_sid
        from v$session se,
             v$mystat  ms
       where se.sid = ms.sid
         and ms.statistic# = 0;
      return n_sid;

    end;

  function get_process_id
             return varchar2
	   is

      v_process_id    v$process.spid%type;

    begin

      select p.spid
        into v_process_id
        from v$process p, v$session s, v$mystat m
       where s.paddr = p.addr
         and s.sid = m.sid
         and m.statistic# = 0;

       return v_process_id;

      end;

  procedure say(
             p_text              in     varchar2)
	    is

    begin
      dbms_output.put_line(p_text);
    end;

END U;
/
