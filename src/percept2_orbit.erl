-module(percept2_orbit). 

-compile(export_all).

-define(cmd, filename:join([code:root_dir(), "bin","erl"])).
 
%%command to run the the sd-orbit benchmark and trace message sending.
run_orbit_with_trace(N) ->
    Nodes=[list_to_atom("node"++integer_to_list(I)++"@127.0.0.1")
           ||I<-lists:seq(1,N)],
    start_nodes(N),
    percept2_dist:start(Nodes, {init_bench, main, [Nodes]}, new, [procs,send], []),
    teardown(N).
  
analyze_orbit_data(N) ->
    Files=[list_to_atom("node"++integer_to_list(I)++"@127.0.0.1-ttb")
           ||I<-lists:seq(1,N)],
    percept2:analyze(Files).

start_nodes(N) ->
    [begin
         Cmd = ?cmd++"  -name node"++integer_to_list(I)++"@127.0.0.1"++
                " -setcookie \"secret\" -detached -pa ebin",
         io:format("starting node:\n~p\n",[Cmd]),
         os:cmd(Cmd)
     end
     ||I<-lists:seq(1, N)].

teardown(N) -> 
    F=fun(I) ->
              Node=list_to_atom("node"++integer_to_list(I)++"@127.0.0.1"),
              rpc:call(Node, erlang, halt, [])
      end,
    lists:foreach(fun(I) -> F(I) end, lists:seq(1, N)).

    
%% percept2_orbit:percept2_orbit:run_orbit_with_trace(15).
%% percept2_orbit:analyze_orbit_data(15).



%% How to use: 
%% N: number of nodes.
%% Please modify the functions run_orbit_with_trace and 
%% run_oribit_with_trace if the nodes are named in 
%% a different way.

%% To profile:
%% In an Erlang node, run the command:
%% percept2_multi_node_trace:run_orbit_with_trace(N).

%% after profile.
%% In the same Erlang node:
%% 1) go to the directory which contains the trace data.
%% 2)run the command:
%%   percept2_multi_node_trace:analyze_orbit_data(N).

%% To see the profiling data:
%% 1)in the Erlang node, run the command:  percept2:start_webserver(8080).
%% 2)goto page localhost:8080.
%% 3)goto the 'processes' page to see the send/receive data.

