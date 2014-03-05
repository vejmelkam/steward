

-module(steward_process).
-author("Martin Vejmelka <vejmelkam@gmail.com>").
-export([execute/6]).

-define(PID_TIMEOUT_MS, 5000).


begin_monitoring(InDir,TaskId,TimeoutMS,LogF) ->
  PidPath = steward_utils:make_proc_file_path(InDir,TaskId,".pid"),
  Pid = steward_utils:start_monitoring(InDir,TaskId,undefined,TimeoutMS,LogF),
  spawn(fun() -> Pid ! {pid_msg, steward_utils:wait_for_file(PidPath,?PID_TIMEOUT_MS,500)} end),
  {running, Pid}.

make_bash_run_script(TaskId,Cmd,OutSpec) ->
  lists:flatten(
    ["#!/usr/bin/env bash\n",
     "CMD=",Cmd,"\n",
     "TID=",TaskId,"\n",
     "PIDFILE=\"$TID.pid\"\n",
     "EXITFILE=\"$TID.exitcode\"\n",
     lists:map(fun ({_,Path}) -> ["touch ",Path,"\n"] end, OutSpec),
     Cmd, lists:map(fun ({FD,Path}) -> io_lib:format(" ~p>> ~s", [FD,Path]) end, OutSpec), "&\n",
     "PID=$!\n",
     "echo $PID `date +%s` > $PIDFILE\n",
     "wait $PID\n",
     "echo $? `date +%s` > $EXITFILE\n"]).


execute(TaskId,Cmd,OutSpec,InDir,TimeoutS,LogF) ->
  [PidPath,ExitCodePath] = steward_utils:make_proc_names(InDir,TaskId,[".pid",".exitcode"]),
  case steward_utils:read_exitcode_file(ExitCodePath) of
    {ExitCode,ExitTime} ->
      LogF(info, "~s: exit code ~p found, process exited on ~w, returning immediately.", [TaskId,ExitCode,ExitTime]),
      {success, ExitCode};
    invalid ->
      LogF(info, "~s: exit code not found, looking for pid file ~s", [TaskId,PidPath]),
      case steward_utils:read_pid_file(PidPath) of
        {OsPid,StartTime} ->
          RemTimeoutS = TimeoutS - steward_utils:seconds_elapsed_from(StartTime),
          LogF(info, "~s: found pid file with pid ~p, process started on ~w, waiting for another ~p s.",
               [TaskId,OsPid,StartTime,RemTimeoutS]),
          begin_monitoring(InDir,TaskId,RemTimeoutS,LogF);
        invalid ->
          LogF(info, "~s: no pid file found, process ~p is being started now at ~w", [TaskId,Cmd,calendar:local_time()]),
          FQN = steward_utils:make_proc_file_path(InDir,TaskId,".sh"),
          steward_utils:write_run_script(FQN,make_bash_run_script(TaskId,Cmd,OutSpec)),
          open_port({spawn,FQN},[{cd,InDir},out]),
          begin_monitoring(InDir,TaskId,TimeoutS*1000,LogF)
      end
  end.


