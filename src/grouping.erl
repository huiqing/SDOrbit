%% creates dynamically s_gropus
%%
%% Author: Amir Ghaffari <Amir.Ghaffari@glasgow.ac.uk>
%% Modified by H.Li <H.Li@kent.ac.uk>

-module(grouping).

-export([create_s_groups/2,
        s_group_spawn_link/4, gateway_loop/1,
        s_group_send/2, 
        new_s_group/3]).

-include("./orbit.hrl").
-compile(export_all).

%% makes s_groups by dividing all the nodes into a number of s_groups
create_s_groups(Nodes, NumGroup) 
  when NumGroup > 0, is_list(Nodes) ->
    GroupSize=length(Nodes) div NumGroup,
    case GroupSize of 
        0 ->
            throw({error, group_size_is_zero});
        _->
            Node=node(),
            {ok, Conf}=create_s_group_conf(Nodes, GroupSize, 1, []),
            GatewayNodes =element(2,lists:unzip3(Conf)),
            Groups=[create_a_s_group(G)||G<-Conf],
            RouteMap=
                [{Worker, GatewayNode}
                 ||{_GName, GatewayNode, WorkNodes}<-
                       Groups,
                   Worker<-WorkNodes],
            {ok, master_group,GroupNodes}=
                create_master_group(Node, GatewayNodes, RouteMap),
            {ok, [{master_group, Node,GroupNodes--[Node]}|Groups]}
    end.



create_s_group_conf([], _, _Couter, Acc) ->
    Acc;
create_s_group_conf(Nodes=[G|_], GroupSize, Counter, Acc)
  when length(Nodes)=<GroupSize ->
    GroupName=list_to_atom("s_group_"++ 
                               integer_to_list(Counter)),
    {ok,[{GroupName, G, Nodes}|Acc]};
create_s_group_conf(Nodes=[G|_], GroupSize, Counter, Acc) ->
    {GroupNodes, Remain} = lists:split(GroupSize, Nodes),
    GroupName=list_to_atom("s_group_"++ 
                               integer_to_list(Counter)),
    create_s_group_conf(Remain, GroupSize, Counter+1,
                        [{GroupName, G, GroupNodes}|Acc]).


create_master_group(Node, GatewayNodes, RouteMap) ->
    GroupNodes=[Node|GatewayNodes],
    {ok, master_group, GroupNodes1}=
        new_s_group(master_group, GroupNodes, Node),
    application:set_env(kernel, route_map, RouteMap),
    [init_gateway(GNode,?NumGateways, RouteMap)
     ||GNode<-GatewayNodes],
    {ok, master_group, GroupNodes1}.
    

%% creates a s_group on gateway node and includes all Workers in it
create_a_s_group({GroupName, GatewayNode, GroupNodes})->
    {ok, GroupName, Nodes}=rpc:call(GatewayNode, ?MODULE, new_s_group, 
                                    [GroupName, GroupNodes, GatewayNode]),
    {GroupName, GatewayNode, Nodes--[GatewayNode]}.

       
s_group_spawn_link(Node, Mod, Fun, Args) ->
    Info=s_group:info(),
    {_, OwnGroupNodes} = lists:keyfind(own_group_nodes, 1, Info),
    case lists:member(Node, OwnGroupNodes) of 
        true ->
            spawn_link(Node, Mod, Fun, Args);
        false ->
            case find_gateway_node(Node) of 
                {ok, GNode} ->
                    rpc:call(GNode,?MODULE, s_group_spawn_link, 
                             [Node, Mod, Fun, Args]);
                none ->
                    spawn_link(Node, Mod, Fun, Args)
            end
    end.
          
s_group_send(Pid, Msg) ->
    ?dbg("s_group_send:~p\n", [{Pid, Msg}]),
    DestNode = node(Pid),
    ?dbg("DestNode:~p\n", [DestNode]),
    Info=s_group:info(),
    {_, OwnGroupNodes} = lists:keyfind(own_group_nodes, 1, Info),
    ?dbg("OwnGroupNodes:~p\n", [OwnGroupNodes]),
    case lists:member(DestNode, OwnGroupNodes) of 
        true ->
            ?dbg("Same Group\n",[]),
            Pid ! Msg;
        false ->
            case find_gateway_node(DestNode) of 
                {ok, GNode} ->
                    I = integer_to_list(random:uniform(?NumGateways)),
                    {list_to_atom("gateway"++I), GNode} !
                        {Pid, first, Msg};
                none ->
                    io:format("could not find gateway node\n"),
                    Pid ! Msg
            end
    end.

find_gateway_node(DestNode) ->
    Info=s_group:info(),
    case application:get_env(kernel,route_map) of 
        undefined ->
            case application:get_env(kernel, gateway) of 
               {ok, Gateway} ->
                    {ok, Gateway};
                undefined ->
                    io:format("could not find gateway node\n"),
                    none
            end;
        {ok, RouteMap} ->
            case lists:keyfind(DestNode, 1, RouteMap) of 
                {DestNode, GNode} ->
                    {ok, GNode};
                none ->
                    io:format("could not find gateway node\n"),
                    none
            end
    end.

init_gateway(Node, NumGatewayWorkers, RouteMap)->
    rpc:call(Node, application, set_env, [kernel, route_map, RouteMap]),
    [spawn_link(Node, 
                fun() ->
                        N=list_to_atom(atom_to_list(gateway)
                                       ++integer_to_list(I)),
                        register(N, self()),
                        gateway_loop(RouteMap) 
                end)
     ||I<-lists:seq(1,NumGatewayWorkers)].
   
gateway_loop(RouteMap)-> 
    receive
        {DestPid, first, Msg} ->
            DestNode=node(DestPid),
            case  lists:keyfind(DestNode, 1, RouteMap) of 
                {DestNode, GatewayNode}->
                    case GatewayNode == node() of 
                        true ->
                            DestPid ! Msg;
                        false ->
                            I = integer_to_list(random:uniform(?NumGateways)),
                            GatewayProc=list_to_atom("gateway"++ I),
                            {GatewayProc, GatewayNode}!{DestPid, second, Msg}
                    end;
                false -> DestPid ! Msg
            end,
            gateway_loop(RouteMap);
        {DestPid, second, Msg} ->
            DestPid ! Msg,
            gateway_loop(RouteMap);
        {From, known_nodes} ->
            Info = s_group:info(),
            {_, OwnGroupNodes} = lists:keyfind(own_group_nodes, 1, Info),
            {registered_name, Name} = process_info(self(), registered_name),
            From ! {{Name, node()}, OwnGroupNodes},
            gateway_loop(RouteMap);
        stop->
            ok;
        _Others ->
            ?dbg("UNEXPECTED MESSAGES:~p\n", [_Others]),
            gateway_loop(RouteMap)
    end.

%% A temporary workaround.
%% GatewayNode belongs to GroupNodes.
new_s_group(GroupName, GroupNodes, GatewayNode) -> 
    {ok, GroupName, Nodes}=s_group:new_s_group(GroupName, GroupNodes),
    [rpc:call(Node, application, set_env, [kernel, gateway, GatewayNode])
     ||Node<- Nodes],
    {ok, GroupName, Nodes}.
        
