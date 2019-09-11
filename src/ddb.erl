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
%% @doc
%%   aws dynamodb
-module(ddb).
-behaviour(pipe).

-compile({parse_transform, category}).
-include_lib("erlcloud/include/erlcloud_aws.hrl").
-include_lib("erlcloud/include/erlcloud_ddb2.hrl").

-export([
   start_link/4
,  init/1
,  free/2
,  handle/3
]).
-export([
   put/1
,  get/1
,  remove/1
,  update/1
,  match/1
,  match/2
]).

%%
%% data types
-type entity() :: tuple().
-type codec()  :: _.

-record(ddb, {
   uri    = undefined :: _  %% ddb access uri
,  bucket = undefined :: _  %% ddb bucket
,  encode = undefined :: _
,  decode = undefined :: _
}).

-define(TIMEOUT, 20000).

%%%------------------------------------------------------------------
%%%
%%% factory
%%%
%%%------------------------------------------------------------------

-spec start_link(atom(), uri:uri(), codec(), codec()) -> datum:either(pid()).

start_link(Key, Uri, Encode, Decode)
 when is_binary(Uri) orelse is_list(Uri) ->
   pipe:start_link({local, typeof(Key)}, ?MODULE, [uri:new(Uri), Encode, Decode], []).

init([Uri, Encode, Decode]) ->
   {ok, handle,
      #ddb{
         uri    = Uri,
         bucket = hd(uri:segments(Uri)),
         encode = Encode,
         decode = Decode
      }
   }.

free(_, _) ->
   ok.

%%%------------------------------------------------------------------
%%%
%%% api
%%%
%%%------------------------------------------------------------------

%%
%% Creates a new entity, or replaces an old entity with a new value.
-spec put(entity()) -> datum:either(entity()).

put(Value) ->
   pipe:call(typeof(Value), {put, Value}, ?TIMEOUT).

%%
%% Get entity
-spec get(entity()) -> datum:either(entity()).

get(Key) ->
   pipe:call(typeof(Key), {get, Key}, ?TIMEOUT).

%%
%% Remove entity
-spec remove(entity()) -> datum:either(entity()).

remove(Key) ->
   pipe:call(typeof(Key), {remove, Key}, ?TIMEOUT).

%%
%% Partial update (patch an entity)
-spec update(entity()) -> datum:either(entity()).

update(Value) ->
   pipe:call(typeof(Value), {update, Value}, ?TIMEOUT).

%%
%% 
-spec match(entity()) -> datum:either({[entity()], _}).

match(Pattern) ->
   match(Pattern, #{}).

-spec match(entity(), _) -> datum:either({[entity()], _}).

match(Pattern, Opts) ->
   pipe:call(typeof(Pattern), {match, Pattern, Opts}, ?TIMEOUT).

%%%------------------------------------------------------------------
%%%
%%% state machine
%%%
%%%------------------------------------------------------------------

handle({put, Value}, _, #ddb{uri = Uri, bucket = Bucket, encode = Encode} = State) ->
   {reply, 
      [either ||
         Config <- aws_auto_config(Uri),
         ddb_codec:encode(Value, Encode),
         erlcloud_ddb2:put_item(Bucket, _, [], Config),
         cats:unit(Value)
      ],
      State
   };

handle({get, Key}, _, #ddb{uri = Uri, bucket = Bucket, encode = Encode, decode = Decode} = State) ->
   {reply,
      [either ||
         Config <- aws_auto_config(Uri),
         ddb_codec:encode(Key, Encode),
         erlcloud_ddb2:get_item(Bucket, _, [], Config),
         ddb_codec:decode(_, Decode)
      ],
      State
   };

handle({remove, Key}, _, #ddb{uri = Uri, bucket = Bucket, encode = Encode} = State) ->
   {reply,
      [either ||
         Config <- aws_auto_config(Uri),
         ddb_codec:encode(Key, Encode),
         erlcloud_ddb2:delete_item(Bucket, _, [], Config),
         cats:unit(Key)
      ],
      State
   };

handle({update, Value}, _, #ddb{uri = Uri, bucket = Bucket, encode = Encode} = State) ->
   {reply,
      [either ||
         Config <- aws_auto_config(Uri),
         Key <- ddb_codec:encode_key(Value, Encode),
         ddb_codec:encode_val(Value, Encode),
         erlcloud_ddb2:update_item(Bucket, Key, _, [], Config),
         cats:unit(Value)
      ],
      State
   };


handle({match, Pattern, Opts}, _, #ddb{uri = Uri, bucket = Bucket, encode = Encode, decode = Decode} = State) ->
   {reply,
      [either ||
         Config <- aws_auto_config(Uri),
         ddb_codec:encode(Pattern, Encode),
         #ddb2_q{
            items = Items,
            last_evaluated_key = Seq
         } <- erlcloud_ddb2:q(Bucket, _, match_opts(Opts), Config),
         cats:unit({
            lists:map(
               fun(X) -> erlang:element(2, ddb_codec:decode(X, Decode)) end,
               Items
            ),
            match_seq(Seq)
         })
      ],
      State
   }.

match_seq(undefined) ->
   undefined;
match_seq(Seq) ->
   base64url:encode(erlang:term_to_binary(Seq)).

match_opts(Opts) ->
   lists:flatten([
      {out, record}
   ,  match_opts_seq(Opts)
   ,  match_opts_len(Opts)
   ,  match_opts_ind(Opts)
   ,  match_opts_sort(Opts)
   ]).

match_opts_seq(#{seq := undefined}) ->
   [];
match_opts_seq(#{seq := Seq}) ->
   [{exclusive_start_key, erlang:binary_to_term(base64url:decode(Seq))}];
match_opts_seq(_) ->
   [].

match_opts_len(#{len := undefined}) ->
   [];
match_opts_len(#{len := Len}) ->
   [{limit, Len}];
match_opts_len(_) ->
   [].

match_opts_ind(#{ind := undefined}) ->
   [];
match_opts_ind(#{ind := Ind}) ->
   [{index_name, Ind}];
match_opts_ind(_) ->
   [].

match_opts_sort(#{sort := forward}) ->
   [{scan_index_forward, true}];
match_opts_sort(#{sort := reverse}) ->
   [{scan_index_forward, false}];
match_opts_sort(_) ->
   [].

%%
%% patch aws config for custom endpoint
aws_auto_config(Uri) ->
   [either ||
      erlcloud_aws:auto_config(),
      cats:unit(config_ddb_endpoint(Uri, _))
   ].

%% Note: erlcloud_aws:auto_config() might retrun {ok, undefined}
config_ddb_endpoint(Uri, Config) ->
   Schema = case uri:schema(Uri) of [_, X] -> X; X -> X end,
   Config#aws_config{
      ddb_scheme = typecast:c(Schema) ++ "://",
      ddb_host   = typecast:c(uri:host(Uri)),
      ddb_port   = uri:port(Uri)
   }.

%%
typeof(Tuple) -> 
   erlang:element(1, Tuple).