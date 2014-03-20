
%% Some macro definitions.
 
-define(SDOrbit, true).
-define(cmd, "c:/erl5.9.1/bin/erl").
-define(NumGateways, 120).

%%-define(debug, 1).
-ifdef(debug).
-define(dbg(F, A), io:format(F, A)).
-else.
-define(dbg(F, A), ok).
-endif.

