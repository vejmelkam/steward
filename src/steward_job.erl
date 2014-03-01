

-module(steward_job).
-author("Martin Vejmelka <vejmelkam@gmail.com>").
-export([execute/10]).

read_submit_file(Path) ->
  case steward_utils:file_read_ints(Path,2) of
    [QueueId,UnixTS] ->
      {QueueId,steward_utils:unix_to_datetime(UnixTS)};
    {error,_} ->
      invalid
  end.


begin_monitoring(InDir,TaskId,QueueId,PidTimeoutMS,TimeoutMS,LogF) ->
  PidPath = steward_utils:make_proc_file_path(InDir,TaskId,".pid"),
  Pid = steward_utils:begin_monitoring(InDir,TaskId,QueueId,TimeoutMS,LogF),
  spawn(fun () -> Pid ! {pid_msg, steward_utils:wait_for_file(PidPath,PidTimeoutMS,500)} end),
  {running,Pid,QueueId}.


build_job_script(TaskId,InDir,Cmd,NumNodes,ProcPerNode,WallTimeHrs,pbs) ->
  lists:flatten([
    "#!/usr/bin/env bash\n",
    io_lib:format("#PBS -l nodes=~p:ppn=~p\n", [NumNodes,ProcPerNode]),
    io_lib:format("#PBS -l walltime=~p:00:00\n", [WallTimeHrs]),
    "#PBS -N i", TaskId, "\n",
    "TID=",Cmd,"\n",
    "cd ",InDir,"\n",
    io_lib:format("mpirun -np ~p ~s &\n", [NumNodes*ProcPerNode,Cmd]),
    "PID=$!\n",
    "echo $PID `date +%s` > $TID.pid\n",
    "wait $PID\n",
    "echo $? `date +%s` > $TID.exitcode\n"]);

build_job_script(TaskId,InDir,Cmd,NumNodes,ProcPerNode,WallTimeHrs,oge) ->
  NP = NumNodes*ProcPerNode,
  lists:flatten([
     "#!/usr/bin/env bash\n",
     "#$ -N ",TaskId,"\n",
     "#$ -wd ",InDir,"\n",
     "#$ -l h_rt=",integer_to_list(WallTimeHrs),":00:00\n",
     "#$ -pe mpi ",integer_to_list(NP),"\n",
     "TID=",TaskId,"\n",
     "cp $PE_HOSTFILE pe_hostfile\n"
     "awk '{for (i=1;i<=$2;i++) print $1}' pe_hostfile > machinefile\n"
     "mpirun --mca plm_rsh_disable_qrsh 1 -np ",integer_to_list(NP)," -machinefile machinefile ",Cmd," &\n",
     "PID=$!\n",
     "echo $PID `date +%s` > $TID.pid\n",
     "wait $PID\n",
     "echo $? `date +%s` > $TID.exitcode\n" ]).


strip_qid(QidLine) ->
  case re:run(QidLine, "([0-9]+)", [{capture,first,list}]) of
    {match, [QidStr]} ->
      list_to_integer(QidStr);
    error ->
      -1
   end.


write_run_script(Path,Content) ->
  file:write_file(Path,Content),
  file:change_mode(Path, 448).   % 448_10 == 700 in octal


execute(TaskId,Cmd,InDir,NumNodes,ProcPerNode,WallTimeHrs,PidTimeoutS,TimeoutS,Backend,LogF) ->
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
              JS = build_job_script(TaskId,InDir,Cmd,NumNodes,ProcPerNode,WallTimeHrs,Backend),
              write_run_script(steward_utils:make_proc_file_path(InDir,TaskId,".sh"),JS),
              QidLine = lists:flatten(os:cmd(lists:flatten(["cd ",InDir," && qsub ",TaskId,".sh"]))),
              QueueId = strip_qid(QidLine),
              file:write_file(SubmitPath,io_lib:format("~p~n~p~n", [QueueId,steward_utils:unix_timestamp()])),
              begin_monitoring(InDir,TaskId,QueueId,PidTimeoutS*1000,TimeoutS*1000,LogF)
          end
      end
  end.


