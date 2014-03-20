%% orbit-int benchmarks (for RELEASE)
%%

-module(init_bench).

-include("./orbit.hrl").

-compile(export_all).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Note: Some macros are defined in orbit.hrl, so %%
%% please change the values as needed.            %%
%%                                                %%
%%              How to run                        %%
%% To run this example with N nodes.              %%
%% cd orit_int                                    %%
%% ./compile                                      %%
%% ./start.sh                                     %%
%% in the erlang shell, type:                     %%
%% init_bench:test(N).                            %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
test(N) when is_integer(N) -> 
    Nodes=[list_to_atom(make_node_name(I))||I<-lists:seq(1,N)],
    teardown(N),
    start_nodes(N),
    timer:sleep(1000),
    init_bench(Nodes),
    timer:sleep(1000),
    %%the teardown operation returns ** exception exit: noconnection; 
    %%don't know why,but should not matter.
    teardown(N). 

   
   

init_bench(Nodes) ->
    G=fun bench:g12345/1, 
    N= 100000, %%100000 calculates Orbit for 0..N
    P= 40, %% Naumber of worker processes on each node
    Start = now(),
    case ?SDOrbit of 
        true -> %% run SD Orbit.
            G_size=5, %% Number of nodes in each s_group
            {NumberOfGroups, Group_size} = calc_num_of_groups(Nodes, G_size),
            case grouping:create_s_groups(Nodes, NumberOfGroups) of 
                {ok, Groups} ->
                    try 
                        io:format("s_groups:~p\n", [Groups]),
                        WorkerNodes = lists:append(
                                        [WorkerNodes||{GroupName, _GatewayNode, WorkerNodes}<-
                                                          Groups, GroupName/=master_group]),
                        Res=bench:dist(G, N, P, WorkerNodes),
                        LapsedUs = timer:now_diff(now(), Start),
                        io:format("N:~p  ---- Num process: ~p  --- Num Nodes: ~p "
                                  "---- Group size: ~p \n",
                                  [N, P, length(Nodes), Group_size]),
                        io:format("Elapsed time in total (microseconds): ~p \n",[LapsedUs]),
                        R=delete_s_groups(Groups),
                        Res
                    catch 
                        E1:E2 -> io:format("Error:~p\n", [{E1, E2}])
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        false -> %%run DOrbit.
            bench:dist(G,N,P,Nodes),
            LapsedUs = timer:now_diff(now(), Start),
            io:format("N:~p  ---- Num process: ~p  --- Num Nodes: ~p \n",
                      [N, P, length(Nodes)]),
            io:format("Elapsed time in total (microseconds): ~p \n",[LapsedUs]) 
    end.

calc_num_of_groups(Nodes, G_size) ->
    NumGroups=length(Nodes) div G_size,
    if
        NumGroups > 0 ->
            NumberOfGroups=NumGroups,
            Group_size=G_size;
        true ->
            %% when number of nodes is less than group size
            NumberOfGroups=1,
            Group_size=length(Nodes)
    end,
    {NumberOfGroups, Group_size}. 

delete_s_groups(Groups) ->
    [rpc:call(GatewayNode, s_group, delete_s_group, [GroupName])
     ||{GroupName, GatewayNode, _GroupNodes}
           <- Groups].
                           
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%                                                %%
%%       utility functions                        %%
%%                                                %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_nodes(N) ->
    io:format("starting nodes...\n"),
    [begin
         Cmd = ?cmd++"  -name "++make_node_name(I)++
                " -setcookie \"secret\" -detached -pa ebin",
         os:cmd(Cmd)
     end
     ||I<-lists:seq(1, N)].

teardown(N) -> 
    F=fun(I) ->
              Node=list_to_atom(make_node_name(I)),
              rpc:call(Node, erlang, halt, [])
      end,
    lists:foreach(fun(I) -> F(I) end, lists:seq(1, N)).

make_node_name(I) -> "node" ++ integer_to_list(I) 
                         ++ "@127.0.0.1".
