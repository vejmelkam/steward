steward
=======

Steward implements a state machine approach to executing external processes or batch jobs (via either PBS or SGE).  The state of the processes is tracked via a set of files store on the filesystem.  The idea is for the state of the executed processes or batch jobs to survive outages, crashes or restarts of the coordinating system.  This is useful for tracking long-running processes.

Steward is useful if jobs are run on a shared filesystem, i.e. it has access to the run directories or as a component of a workflow system where the state of a complex chain of computations must be tracked.


### Concept

Each process/job has a TaskId which identifies it for the steward library.  The steward library then creates during the lifetime of the process/job a set of files which encode the current state of the job.  If the program running the steward library is restarted at any time and the processes/jobs are re-executed, steward will detect whether the job is already submitted/running/terminated.

### Processes

The state machine of a process executed immediately (not using a batch system) is

    Not started yet -> running -> terminated.

When the process is started a _TaskId.pid_ file is generated and store in the run directory containing two integers, the OS pid and a unix timestamp of when the process was started.  When the process terminates a _TaskId.exitcode_ file is generated containing the exit code of the process and the time of termination.

The way the persistence works is if a steward\_process:execute function is called and finds the exitcode, it returns immediately with the exit code read from the file.  If there is no exit code file but there is a pid file, then the process must still be running and the library computes from the start time in the file how much more time is left for the process until termination and begins tracking it.  If there is not even a pid file then the process is restarted.

Note: this does not handle unexpected deaths of the process after the pid file was written.  Currently if this happens, steward will wait until it hits the timeout and then returns with a {failure, timeout} message.  This a TODO item but not trivial as the process may run on another machine.


## Example:

    steward\_process:execute("list", "ls", [{1, "list.stdout"},{2, "list.stderr"}], ".", 5000, fun(L,T,A) -> ok end).

This runs the command "ls" in the current directory by first writing a "list.sh" file which contains the appropriate commands to route standard output/error streams and creates the ''list.pid'' file upon start.  When "ls" exits, a ''list.exitcode'' file is created, which will contain the exit code (0 on success) and the unix timestamp of the termination time.  The last argument is a logging function which receives arguments logging level (one of info,warn,error), the message format and arguments (in the sense of io\_lib:format).


### Batch jobs

Steward is able to submit and track jobs in a PBS (Portable Batch System) or SGE (Sun Grid Engine) batch system.
Batch jobs have an extra state

    Not submitted yet -> Not started yet (waiting in queue) -> running -> terminated.

When a job is submitted a _TaskId.submit_ file is created with the job id in the batch system and the system waits until the scheduler starts the job, then a _TaskId.pid_ file is written.  When the job completes a _TaskId.exitcode_ file is written.

## Example

    steward\_job:execute("wrf", "/opt/wrf-3.4/WRFV3/main/wrf.exe", "/home/workspace",12,16,6,fun(L,T,A) -> ok end).

This will queue a wrf job that will ask for 12 nodes, 16 cpus per node and 6 hrs of walltime.
If the system using steward is restarted and this function is called again while the job is either queued or running, it will enter the appropriate state and continue waiting for termination.

### TODO
  * check whether a process with the OS pid is running if a pid file is found, solve this for remote executions
  * add explicit queue parameter to batch jobs


