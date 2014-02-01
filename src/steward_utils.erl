

-module(steward_utils).
-author("Martin Vejmelka <vejmelkam@gmail.com>").
-export([file_read_ints_robust/2,start_monitoring/5,wait_for_completion/1,
         make_std_output_spec/2,make_proc_file_path/3,remove_execution_files/2,
         make_proc_names/3,read_exitcode_file/1,read_pid_file/1,wait_for_file/3,
         seconds_elapsed_from/1,unix_to_datetime/1]).


seconds_between(From,To) ->
  FromS = calendar:datetime_to_gregorian_seconds(From),
  ToS = calendar:datetime_to_gregorian_seconds(To),
  ToS - FromS.

seconds_elapsed_from(From) ->
  seconds_between(From, calendar:local_time()).

unix_to_datetime(Unix) ->
  GMT = calendar:gregorian_seconds_to_datetime(Unix + 719528 * 86400),
  calendar:universal_time_to_local_time(GMT).


wait_for_file(Path,TimeoutMS,WaitMS) ->
  case filelib:is_regular(Path) of
    true ->
      {success, Path};
    false ->
      if
        TimeoutMS >= WaitMS ->
          timer:sleep(WaitMS),
          wait_for_file(Path,TimeoutMS - WaitMS,WaitMS);
        true ->
          {failure, timeout}
      end
  end.



file_read_ints(File,Count) ->
  case file:read_file(File) of
    {ok, D} ->
      T = string:tokens(binary_to_list(D), " \n"),
      case length(T) of
        Count ->
          lists:map(fun (S) -> list_to_integer(string:strip(S)) end, T);
        WrongCount ->
          {error, {int_count, WrongCount}}
        end;
    {error, R} ->
      {error, R}
  end.



retry_at_most(N,PauseMS,FailFun,Func) ->
  R = Func(),
  case FailFun(R) of
    true ->
      case N of
        0 ->
          R;
        NonZero ->
          timer:sleep(PauseMS),
          retry_at_most(NonZero - 1,PauseMS,FailFun,Func)
      end;
    false ->
      R
  end.


file_read_ints_robust(File,Count) ->
  FailCheck = fun({error, _}) -> true;
    (_) -> false end,
  retry_at_most(4, 250, FailCheck, fun () -> file_read_ints(File,Count) end).


read_pid_file(Path) ->
  case file_read_ints(Path,2) of
    {error, _} ->
      invalid;
    [Pid,UnixTs] ->
      {Pid, unix_to_datetime(UnixTs)}
  end.


read_exitcode_file(Path) ->
  case file_read_ints(Path,2) of
    {error, _} ->
      invalid;
    [Pid,UnixTs] ->
      {Pid, unix_to_datetime(UnixTs)}
  end.


make_proc_file_path(InDir,TaskId,Suffix) ->
  filename:join(InDir, TaskId ++ Suffix).

make_std_output_spec(InDir,TaskId) ->
  [{1, make_proc_file_path(InDir, TaskId, ".stdout")},
   {2, make_proc_file_path(InDir, TaskId, ".stderr")}].

make_proc_names(InDir,TaskId,Suffixes) ->
  lists:map(fun(S) -> make_proc_file_path(InDir,TaskId,S) end, Suffixes).

remove_execution_files(InDir,TaskId) ->
  lists:map(fun file:delete/1, make_proc_names(InDir, TaskId, [".pid", ".exitcode", ".submit"])).


process_kill(TaskId,OsPid,QueueId,LogF) ->
  case QueueId of
    undefined ->
      % it's an immediate process, not batched
      os:cmd(io_lib:format("kill ~p", [OsPid])),
      LogF(warn, "~s: monitor-process acting on kill request, kill sent to os process ~p",
          [TaskId, OsPid]);
    _ValidId ->
      % it's a batch job
      os:cmd(io_lib:format("qdel ~p", [QueueId])),
      LogF(warn, "~s: monitor-process acting on kill request, qdel ~p executed", [TaskId,QueueId])
  end.


monitor_process(MasterPid,InDir,TaskId,OsPid,QueueId,TimeoutMS,LogF) ->
  try
    receive
      {kill, Reason} ->
        process_kill(TaskId,OsPid,QueueId,LogF),
        remove_execution_files(InDir,TaskId),
        {killed, Reason};
      {exit_msg, {success, ExitCodePath}} ->
        {ExitCode,ExitTime} = read_exitcode_file(ExitCodePath),
        LogF(info, "~s: exited with code ~p on ~w", [TaskId,ExitCode,ExitTime]),
        {success, ExitCode};
      {exit_msg, {failure, Reason}} ->
        process_kill(TaskId,OsPid,QueueId,LogF),
        LogF(error,"~s: failed with reason ~p", [TaskId,Reason]),
        {failure, Reason};
      {pid_msg, {failure, Reason}} ->
        LogF(error, "~s: pid not acquired with reason ~w", [TaskId,Reason]),
        process_kill(TaskId,OsPid,QueueId,LogF),
        remove_execution_files(InDir,TaskId),
        {failure, acquire_pid_timeout};
      {pid_msg, {success, PidPath}} ->
        {RealOsPid,StartTime} = read_pid_file(PidPath),
        LogF(info, "~s: received pid ~p, execution started on ~w", [TaskId, RealOsPid,StartTime]),
        MasterPid ! {proc_started, self(), StartTime},
        RemainingTimeMS = TimeoutMS - seconds_elapsed_from(StartTime) * 1000,
        S = self(),
        spawn(fun() -> S ! {exit_msg, wait_for_file(make_proc_file_path(InDir,TaskId,".exitcode"),
                  RemainingTimeMS, 500)} end),
        monitor_process(MasterPid,InDir,TaskId,RealOsPid,QueueId,TimeoutMS,LogF);
      Other ->
        error_logger:error_msg("stewutils:monitor_process received message [~p] which it should not have", [Other]),
        monitor_process(MasterPid,InDir,TaskId,OsPid,QueueId,TimeoutMS,LogF)
    end
  catch
    X ->
      error_logger:error_msg("stewutils:monitor_process caught exception ~p", [X]),
      monitor_process(MasterPid,InDir,TaskId,OsPid,QueueId,TimeoutMS,LogF)
  end.


start_monitoring(InDir,TaskId,QueueId,TimeoutMS,LogF) ->
  S = self(),
  spawn(fun() -> S ! {proc_terminated, self(), monitor_process(S,InDir,TaskId,undefined,QueueId,TimeoutMS,LogF)} end).


% A reference implementation of the wait method that handles monitor_process messages
% All implementations must handle the following messages:
% {proc_started, Pid, StartedWhen}, {proc_terminated, Pid, Result}.
% wait_for_completion also forwards {kill, Reason} to the monitor so the external process can be killed
% by sending a message to the invoking process.
%
wait_for_completion(MonPid) ->
  receive
    {proc_started,MonPid,_} ->
      wait_for_completion(MonPid);
    {proc_terminated,MonPid,Result} ->
      Result;
    % the kill message is expected to come from another process that needs to stop the computation
    {kill, Reason} ->
      MonPid ! {kill, Reason},
      wait_for_completion(MonPid)
  end.


