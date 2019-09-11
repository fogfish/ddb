%%
%%   Copyright 2019 Dmitry Kolesnikov, All Rights Reserved
%%
%%   Licensed under the Apache License, Version 2.0 (the "License");
%%   you may not use this file except in compliance with the License.
%%   You may obtain a copy of the License at
%%
%%       http://www.apache.org/licenses/LICENSE-2.0
%%
%%   Unless required by applicable law or agreed to in writing, software
%%   distributed under the License is distributed on an "AS IS" BASIS,
%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%   See the License for the specific language governing permissions and
%%   limitations under the License.
%%
-module(ddb_codec).

-export([
   encode/2
,  encode_key/2
,  encode_val/2
,  decode/2
]).

%%
%%
-define(id,       <<"id">>).
-define(prefix,   <<"prefix">>).
-define(suffix,   <<"suffix">>).

%%
%%
-spec encode(_, fun((_) -> map())) -> datum:either([{_, _}]).

encode(Val, Encode) ->
   case Encode(Val) of
      #{?id := {iri, _, _}} = Pairs ->
         {ok, encode_pairs(maps:to_list(Pairs))};
      #{?id := Key} = Pairs when is_binary(Key) ->
         {ok, encode_pairs(maps:to_list(Pairs))};
      #{?id := Id} ->
         {error, {badarg, id, Id}};
      _ ->
         {error, {badarg, id, undefined}}
   end.

%%
%%
-spec encode_key(_, fun((_) -> map())) -> datum:either([{_, _}]).

encode_key(Val, Encode) ->
   case Encode(Val) of
      #{?id := {iri, _, _} = Key} ->
         {ok, encode_pairs([{?id, Key}])};
      #{?id := Key} when is_binary(Key) ->
         {ok, encode_pairs([{?id, Key}])};
      #{?id := Id} ->
         {error, {badarg, id, Id}};
      _ ->
         {error, {badarg, id, undefined}}
   end.

%%
%%
-spec encode_val(_, fun((_) -> map())) -> datum:either([{_, _}]).

encode_val(Val, Encode) ->
   {ok, encode_pairs(maps:to_list( maps:without([?id], Encode(Val)) ))}.

%%
%%
encode_pairs([{?id, {iri, Prefix, undefined}} | Tail]) ->
   [
      {?prefix, encode_value(Prefix)}
   |  encode_pairs(Tail)
   ];

encode_pairs([{?id, {iri, Prefix, Suffix}} | Tail]) ->
   [
      {?prefix, encode_value(Prefix)}
   ,  {?suffix, encode_value(Suffix)}
   |  encode_pairs(Tail)
   ];

encode_pairs([{?id, Prefix} | Tail]) when is_binary(Prefix) ->
   [
      {?id, encode_value(Prefix)}
   |  encode_pairs(Tail)
   ];

encode_pairs([{Key, Value} | Tail]) ->
   [
      {Key, encode_value(Value)}
   |  encode_pairs(Tail)
   ];

encode_pairs([]) ->
   [].

%%
%%
encode_value(Val)
 when is_list(Val) ->
   {l, [encode_value(X) || X <- Val]};

encode_value(Val) 
 when is_binary(Val) ->
   {s, Val};

encode_value(Val)
 when is_integer(Val) ->
   {n, Val};

encode_value(Val)
 when is_float(Val) ->
   {n, Val};

encode_value(Val)
 when is_boolean(Val) ->
   {bool, Val};

encode_value(Val)
 when is_map(Val) ->
   {m, [{K, encode_value(V)} || {K, V} <- maps:to_list(Val)]};

encode_value({iri, Prefix, Suffix}) ->
   {s, <<$/, Prefix/binary, $/, Suffix/binary>>}.

%%
%%
-spec decode([{_, _}], fun((map()) -> _)) -> datum:either(_).

decode([], _) ->
   {error, not_found};

decode(ValueDDB, Decode) ->
   case maps:from_list(ValueDDB) of
      #{?prefix := Prefix, ?suffix := Suffix} = Pairs ->
         Value = decode_pairs(maps:without([?prefix, ?suffix], Pairs)),
         {ok, Decode(Value#{?id => {iri, Prefix, Suffix}})};
      #{?id := Prefix} = Pairs ->
         Value = decode_pairs(maps:without([?id], Pairs)),
         {ok, Decode(Value#{?id => Prefix})};
      Pairs ->
         {error, {badarg, ddb, Pairs}}
   end.


decode_pairs(Pairs) ->
   maps:map(fun(_, X) -> decode_value(X) end, Pairs).

decode_value(<<$/, Identity/binary>>) ->
   Prefix = filename:dirname(Identity),
   Suffix  = filename:basename(Identity),
   {iri, Prefix, Suffix};

decode_value([{_, _}| _] = X) ->
   maps:from_list(X);
decode_value(X) ->
   X.