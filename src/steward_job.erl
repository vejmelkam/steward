

-module(steward_job).
-author("Martin Vejmelka <vejmelkam@gmail.com>").
-export([execute/2,check_missing_args/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


read_submit_file(Path) ->
  case steward_utils:file_read_ints(Path,2) of
    [QueueId,UnixTS] ->
      {QueueId,steward_utils:unix_to_datetime(UnixTS)};
    {error,_} ->
      invalid
  end.


begin_monitoring(InDir,TaskId,QueueId,PidTimeoutMS,TimeoutMS,LogF) ->
  PidPath = steward_utils:make_proc_file_path(InDir,TaskId,".pid"),
  Pid = steward_utils:start_monitoring(InDir,TaskId,QueueId,TimeoutMS,LogF),
  spawn(fun () -> Pid ! {pid_msg, steward_utils:wait_for_file(PidPath,PidTimeoutMS,500)} end),
  {running,Pid,QueueId}.


tvar_string_to_atom("%%numnodes%%") -> num_nodes;
tvar_string_to_atom("%%ppn%%") -> proc_per_node;
tvar_string_to_atom("%%workdir%%") -> in_dir;
tvar_string_to_atom("%%taskid%%") -> task_id;
tvar_string_to_atom("%%execpath%%") -> cmd;
tvar_string_to_atom("%%walltimehrs%%") -> wall_time_hrs;
tvar_string_to_atom("%%walltimemins%%") -> wall_time_mins;
tvar_string_to_atom("%%np%%") -> np.

extract_unique_tvars(S) ->
  {ok,MP} = re:compile("(%%[^%]+%%)"),
  case re:run(S, MP, [global, {capture, all, list}]) of
    nomatch -> [];
    {match, Captures} ->
      lists:usort(lists:map(fun ([_, N]) -> N end, Captures))
  end.


render_to_string(V) when is_list(V) -> V;
render_to_string(V) -> io_lib:format("~p", [V]).


replace_tvar(TVar,Templ,Args) ->
  N = tvar_string_to_atom(TVar),
  ValStr = render_to_string(proplists:get_value(N, Args)),
  re:replace(Templ, TVar, ValStr, [global, {return, list}]).


build_job_script(Args0, Profile) ->

  Templ = proplists:get_value(job_script_template, Profile),

  % these are guaranteed to be present
  NumNodes = proplists:get_value(num_nodes, Args0),
  ProcPerNode = proplists:get_value(proc_per_node, Args0),

  % ensure that numprocs is present and correctly computed for the given nodes/ppn
  NP = NumNodes * ProcPerNode,
  Args = [{np,NP}|Args0],

  % retrieve all unique variable requests from the template
  Vars = extract_unique_tvars(Templ),

  % for each variable, replace its occurrence with the associated value
  lists:foldl(fun (TVar, Acc) -> replace_tvar(TVar, Acc, Args) end, Templ, Vars).


-spec submit_job([atom()|tuple()],[atom()|tuple()]) -> integer().
submit_job(Args,Profile) ->
  SubmitTempl = proplists:get_value(submit_command_template, Profile),
  Command = re:replace(SubmitTempl,"%%scriptpath%%", proplists:get_value(script_path, Args), [global, {return, list}]),
  QidLine = lists:flatten(os:cmd(lists:flatten(Command))),
  case re:run(QidLine, "([0-9]+)", [{capture,first,list}]) of
    {match, [QidStr]} ->
      list_to_integer(QidStr);
    error ->
      -1
   end.


-spec execute([atom()|tuple()],fun()) -> {success,integer()}|{running,pos_integer(),integer()}.
execute(Args,LogF) ->
  case check_missing_args(Args) of
    [] ->
      ok;
    Missing ->
      throw({missing_arguments, Missing})
  end,
  InDir = proplists:get_value(in_dir, Args),
  TaskId = proplists:get_value(task_id, Args),
  PidTimeoutS = proplists:get_value(pid_timeout_s, Args),
  TimeoutS = proplists:get_value(job_timeout_s, Args),
  ProfileName = proplists:get_value(hpc_profile, Args),
  {ok,Profile} = file:consult("etc/steward-profiles/" ++ ProfileName),
  [PidPath,ExitCodePath,SubmitPath] = steward_utils:make_proc_names(InDir,TaskId,[".pid",".exitcode",".submit"]),
  case steward_utils:read_exitcode_file(ExitCodePath) of
    {ExitCode,ExitTime} ->
      LogF(info, "~s: process already completed on ~w, exit code was ~p, returning immediately",
           [TaskId,ExitTime,ExitCode]),
      {success, ExitCode};
    invalid ->
      case steward_utils:read_pid_file(PidPath) of
        {OsPid,StartTime} ->
          {QueueId,SubmitTime} = read_submit_file(SubmitPath),
          RemTimeoutS = TimeoutS - steward_utils:seconds_elapsed_from(StartTime),
          LogF(info, "~s: already submitted on ~w and started on ~w, os pid is ~p, remaining execution time ~p s",
                    [TaskId,SubmitTime,StartTime,OsPid,RemTimeoutS]),
          begin_monitoring(InDir,TaskId,QueueId,PidTimeoutS*1000,RemTimeoutS*1000,LogF);
        invalid ->
          case read_submit_file(SubmitPath) of
            {QueueId,SubmitTime} ->
              RemPidTimeoutS = PidTimeoutS - steward_utils:seconds_elapsed_from(SubmitTime),
              LogF(info, "~s: already submitted ~w, waiting ~p s more for job start", [TaskId,SubmitTime,RemPidTimeoutS]),
              begin_monitoring(InDir,TaskId,QueueId,RemPidTimeoutS*1000,TimeoutS*1000,LogF);
            invalid ->
              JS = build_job_script(Args,Profile),
              ScriptPath = steward_utils:make_proc_file_path(InDir,TaskId,".sh"),
              steward_utils:write_run_script(ScriptPath,JS),
              QueueId = submit_job([{script_path,ScriptPath}|Args],Profile),
              file:write_file(SubmitPath,io_lib:format("~p~n~p~n", [QueueId,steward_utils:unix_timestamp()])),
              begin_monitoring(InDir,TaskId,QueueId,PidTimeoutS*1000,TimeoutS*1000,LogF)
          end
      end
  end.


-spec check_missing_args([atom()|tuple()]) -> [atom()].
check_missing_args(Args) ->
  M0 = lists:filter(fun (K) -> not proplists:is_defined(K,Args) end,
                    [task_id,in_dir,pid_timeout_s,job_timeout_s,hpc_profile,
                     wall_time_hrs,wall_time_mins,proc_per_node,num_nodes]),
  % exception: if the profile is null (direct execution of mpiexec) then wall_time_hrs is not needed
  case proplists:get_value(hpc_profile,Args) of
    null ->
      lists:delete(wall_time_hrs,M0);
    _ ->
      M0
  end.


-ifdef(TEST).

execute_null_success_test() ->
  file:delete("sleeper.submit"),
  file:delete("sleeper.pid"),
  file:delete("sleeper.exitcode"),
  file:write_file("sleeper_file.sh", "#!/usr/bin/env bash\nsleep 1\nexit 0"),
  file:change_mode("sleeper_file.sh",448),   % 448=700 octal
  {running,Pid,-1} = execute([{task_id,"sleeper"},{cmd,"./sleeper_file.sh"},{in_dir,"."},
                              {num_nodes,1},{proc_per_node,1},{pid_timeout_s,2},{job_timeout_s,3},
                              {hpc_profile,null},{mpi_path,"mpiexec"}],
                             fun(I,T,A) -> io:format("~p " ++ T ++ "\n", [I|A]) end),
  receive
    {proc_started,Pid,_} ->
      ok;
    A ->
      io:format("received incorrect message (instead of proc_started) ~p~n",[A]),
      ?assert(false)
  after 3000 ->
      ?assert(false)
  end,
  receive
    {proc_terminated,Pid,{success,0}} ->
      ok;
    B ->
      io:format("received incorrect message (instead of proc_terminated): ~p~n", [B]),
      ?assert(false)
  after 3000 ->
    ?assert(false)
  end,
  file:delete("sleeper.submit"),
  file:delete("sleeper.pid"),
  file:delete("sleeper.sh"),
  file:delete("sleeper_file.sh"),
  file:delete("sleeper.exitcode").

execute_null_pid_timeout_test() ->
  file:delete("sleeper1.submit"),
  file:delete("sleeper1.pid"),
  file:delete("sleeper1.exitcode"),
  file:write_file("sleeper_file1.sh", "#!/usr/bin/env bash\nsleep 1\nexit 0"),
  file:change_mode("sleeper_file1.sh",448),   % 448=700 octal
  {running,Pid,-1} = execute([{task_id,"sleeper1"},{cmd,"sleep 5\n./sleeper_file1.sh"},{in_dir,"."},
                              {num_nodes,1},{proc_per_node,1},{pid_timeout_s,2},{job_timeout_s,2},
                              {hpc_profile,null},{mpi_path,"mpiexec"},{wall_time_hrs,1}],
                             fun(I,T,A) -> io:format("~p " ++ T ++ "\n", [I|A]) end),
  receive
    {proc_terminated,Pid,{failure, acquire_pid_timeout}} ->
      ok;
    B ->
      io:format("received incorrect message (instead of proc_terminated): ~p~n",[B]),
      ?assert(false)
  after 4000 ->
    ?assert(false) 
  end,
  file:delete("sleeper1.submit"),
  file:delete("sleeper1.pid"),
  file:delete("sleeper1.sh"),
  file:delete("sleeper_file1.sh"),
  file:delete("sleeper1.exitcode").


execute_null_pid_exitcode_failure_test() ->
  file:delete("sleeper2.submit"),
  file:delete("sleeper2.pid"),
  file:delete("sleeper2.exitcode"),
  file:write_file("sleeper_file2.sh", "#!/usr/bin/env bash\nsleep 1\nexit 5"),
  file:change_mode("sleeper_file2.sh",448),   % 448=700 octal
  {running,Pid,-1} = execute([{task_id,"sleeper2"},{cmd,"./sleeper_file2.sh"},{in_dir,"."},
                              {num_nodes,1},{proc_per_node,1},{pid_timeout_s,2},{job_timeout_s,3},
                              {hpc_profile,null},{mpi_path,"mpiexec"}],
                             fun(I,T,A) -> io:format("~p " ++ T ++ "\n", [I|A]) end),
  receive
    {proc_started,Pid,_} ->
      ok;
    A ->
      io:format("~p~n",[A]),
      ?assert(false)
  after 3000 ->
    ?assert(false)
  end,
  receive
    {proc_terminated,Pid,{success,5}} ->
      ok;
    B ->
      io:format("recieved incorrect message: ~p~n", [B]),
      ?assert(false)
  after 3000 ->
    ?assert(false) 
  end,
  file:delete("sleeper2.submit"),
  file:delete("sleeper2.pid"),
  file:delete("sleeper2.sh"),
  file:delete("sleeper_file2.sh"),
  file:delete("sleeper2.exitcode").


execute_null_pid_exec_timeout_test() ->
  file:delete("sleeper3.submit"),
  file:delete("sleeper3.pid"),
  file:delete("sleeper3.exitcode"),
  file:write_file("sleeper_file3.sh", "#!/usr/bin/env bash\nsleep 5\nexit 0"),
  file:change_mode("sleeper_file3.sh",448),   % 448=700 octal
  {running,Pid,-1} = execute([{task_id,"sleeper3"},{cmd,"./sleeper_file3.sh"},{in_dir,"."},
                              {num_nodes,1},{proc_per_node,1},{pid_timeout_s,2},{job_timeout_s,2},
                              {hpc_profile,null},{mpi_path,"mpiexec"},{wall_time_hrs,1}],
                             fun(I,T,A) -> io:format("~p " ++ T ++ "\n", [I|A]) end),
  receive
    {proc_started,Pid,_} ->
      ok;
    A ->
      io:format("received incorrect message instead of proc_started: ~p~n",[A]),
      ?assert(false)
  after 3000 ->
    ?assert(false)
  end,
  receive
    {proc_terminated,Pid,{failure,timeout}} ->
      ok;
    B ->
      io:format("received incorrect message instead of proc_terminated: ~p~n", [B]),
      ?assert(false)
  after 3000 ->
    ?assert(false) 
  end,
  file:delete("sleeper3.submit"),
  file:delete("sleeper3.pid"),
  file:delete("sleeper3.sh"),
  file:delete("sleeper_file3.sh"),
  file:delete("sleeper3.exitcode").

-endif.

