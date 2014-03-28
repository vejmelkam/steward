

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


build_job_script(Args,null) ->
  NumNodes = proplists:get_value(num_nodes, Args),
  ProcPerNode = proplists:get_value(proc_per_node, Args),
  InDir = proplists:get_value(in_dir, Args),
  TaskId = proplists:get_value(task_id, Args),
  Cmd = proplists:get_value(cmd, Args),
  MpiPath = proplists:get_value(mpi_path, Args),
  MpiExtraParams = proplists:get_value(mpi_extra_params, Args, []),
  PreLines = proplists:get_value(jobscript_pre_mpiexec, Args, []),
  PostLines = proplists:get_value(jobscript_post_mpiexec, Args, []),
  NP = NumNodes*ProcPerNode,
  lists:flatten([
    "#!/usr/bin/env bash\n",
    "cd ",InDir,"\n",
    "TID=",TaskId,"\n",
    PreLines,
    MpiPath, " ", MpiExtraParams, " -np ", integer_to_list(NP), " ", Cmd," &\n",
    PostLines,
    "PID=$!\n",
    "echo $PID `date +%s` > $TID.pid\n",
    "wait $PID\n",
    "echo $? `date +%s` > $TID.exitcode\n"]);


build_job_script(Args,pbs) ->
  NumNodes = proplists:get_value(num_nodes, Args),
  ProcPerNode = proplists:get_value(proc_per_node, Args),
  InDir = proplists:get_value(in_dir, Args),
  TaskId = proplists:get_value(task_id, Args),
  Cmd = proplists:get_value(cmd, Args),
  MpiPath = proplists:get_value(mpi_path, Args),
  MpiExtraParams = proplists:get_value(mpi_extra_params, Args, []),
  WallTimeHrs = proplists:get_value(wall_time_hrs, Args),
  PreLines = proplists:get_value(jobscript_pre_mpiexec, Args, []),
  PostLines = proplists:get_value(jobscript_post_mpiexec, Args, []),
  NP = NumNodes * ProcPerNode,
  lists:flatten([
    "#!/usr/bin/env bash\n",
    io_lib:format("#PBS -l nodes=~p:ppn=~p\n", [NumNodes,ProcPerNode]),
    io_lib:format("#PBS -l walltime=~p:00:00\n", [WallTimeHrs]),
    "#PBS -N ", TaskId, "\n",
    "TID=",TaskId,"\n",
    "cd ",InDir,"\n",
    PreLines,
    MpiPath, " ", MpiExtraParams, " -np ", integer_to_list(NP), " ", Cmd, " &\n",
    PostLines,
    "PID=$!\n",
    "echo $PID `date +%s` > $TID.pid\n",
    "wait $PID\n",
    "echo $? `date +%s` > $TID.exitcode\n"]);

build_job_script(Args,sge) ->
  NumNodes = proplists:get_value(num_nodes, Args),
  ProcPerNode = proplists:get_value(proc_per_node, Args),
  InDir = proplists:get_value(in_dir, Args),
  TaskId = proplists:get_value(task_id, Args),
  Cmd = proplists:get_value(cmd, Args),
  MpiPath = proplists:get_value(mpi_path, Args),
  MpiExtraParams = proplists:get_value(mpi_extra_params, Args, []),
  WallTimeHrs = proplists:get_value(wall_time_hrs, Args),
  PreLines = proplists:get_value(jobscript_pre_mpiexec, Args, []),
  PostLines = proplists:get_value(jobscript_post_mpiexec, Args, []),
  NP = NumNodes*ProcPerNode,
  lists:flatten([
     "#$ -S /bin/bash\n",
     "#$ -N ",TaskId,"\n",
     "#$ -wd ",InDir,"\n",
     "#$ -l h_rt=",integer_to_list(WallTimeHrs),":00:00\n",
     "#$ -pe mpi ",integer_to_list(NP),"\n",
     "TID=",TaskId,"\n",
     "cp $PE_HOSTFILE pe_hostfile\n",
     "awk '{for (i=1;i<=$2;i++) print $1}' pe_hostfile > machinefile\n",
     PreLines,
     MpiPath," ",MpiExtraParams," -np ",integer_to_list(NP)," -machinefile machinefile ",Cmd," &\n",
     PostLines,
     "PID=$!\n",
     "echo $PID `date +%s` > $TID.pid\n",
     "wait $PID\n",
     "echo $? `date +%s` > $TID.exitcode\n" ]).


strip_qid(QidLine,_Backend) ->
  case re:run(QidLine, "([0-9]+)", [{capture,first,list}]) of
    {match, [QidStr]} ->
      list_to_integer(QidStr);
    error ->
      -1
   end.


submit_job(InDir,TaskId,pbs) ->
  QidLine = lists:flatten(os:cmd(lists:flatten(["cd ",InDir," && qsub ",TaskId,".sh"]))),
  strip_qid(QidLine,pbs);
submit_job(InDir,TaskId,sge) ->
  QidLine = lists:flatten(os:cmd(lists:flatten(["cd ",InDir," && qsub ",TaskId,".sh"]))),
  strip_qid(QidLine,sge);
submit_job(InDir,TaskId,null) ->
  % simulates submission by delaying the execution of the task script by 2 seconds
  timer:apply_after(1000,os,cmd,[filename:join([InDir,TaskId++".sh"])]),
  -1.


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
  Backend = proplists:get_value(hpc_backend, Args),
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
              JS = build_job_script(Args,Backend),
              steward_utils:write_run_script(steward_utils:make_proc_file_path(InDir,TaskId,".sh"),JS),
              QueueId = submit_job(InDir,TaskId,Backend),
              file:write_file(SubmitPath,io_lib:format("~p~n~p~n", [QueueId,steward_utils:unix_timestamp()])),
              begin_monitoring(InDir,TaskId,QueueId,PidTimeoutS*1000,TimeoutS*1000,LogF)
          end
      end
  end.


-spec check_missing_args([atom()|tuple()]) -> [atom()].
check_missing_args(Args) ->
  M0 = lists:filter(fun (K) -> not proplists:is_defined(K,Args) end,
                    [task_id,in_dir,pid_timeout_s,job_timeout_s,hpc_backend,
                     mpi_path,wall_time_hrs,proc_per_node,num_nodes]),
  % exception: if the backend is null (direct execution of mpiexec) then wall_time_hrs is not needed
  case proplists:get_value(hpc_backend,Args) of
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
                              {hpc_backend,null},{mpi_path,"mpiexec"}],
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
                              {hpc_backend,null},{mpi_path,"mpiexec"},{wall_time_hrs,1}],
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
                              {hpc_backend,null},{mpi_path,"mpiexec"}],
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
                              {hpc_backend,null},{mpi_path,"mpiexec"},{wall_time_hrs,1}],
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

