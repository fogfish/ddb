-module(ddb_SUITE).

-export([all/0]).
-export([
   ddb_put/1
,  ddb_put_invalid_identity/1
,  ddb_get/1
,  ddb_remove/1
,  ddb_update/1
,  ddb_match/1
]).

-compile({parse_transform, category}).
-compile({parse_transform, generic}).
-include_lib("erlcloud/include/erlcloud_aws.hrl").
-include_lib("erlcloud/include/erlcloud_ddb2.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
   [Test || {Test, NAry} <- ?MODULE:module_info(exports), 
      Test =/= module_info,
      Test =/= init_per_suite,
      Test =/= end_per_suite,
      NAry =:= 1
   ].

%%
%%
-record(test, {id, a, b, c, d, e}).
-define(ddb,  uri:new("ddb://dynamodb.eu-west-1.amazonaws.com:443/dev-styx-1-test")).

%%
%%
-define(entity,
   #test{
      id = {iri, <<"test">>, <<"1">>}
   ,  a  = 1
   ,  b  = 1.0
   ,  c  = <<"0123456789">>
   ,  d  = true
   ,  e  = [1, 1.0, <<"10">>, true]
   }
).

-define(key,
   #test{
      id = {iri, <<"test">>, <<"1">>}
   }
).

ddb_put(_) ->
   mock_init_put(
      [
         {<<"prefix">>, {s, <<"test">>}}
      ,  {<<"suffix">>, {s, <<"1">>}}
      ,  {<<"a">>,      {n, 1}}
      ,  {<<"b">>,      {n, 1.0}}
      ,  {<<"c">>,      {s, <<"0123456789">>}}
      ,  {<<"d">>,      {bool, true}}
      ,  {<<"e">>,      {l, [{n, 1}, {n, 1.0}, {s, <<"10">>}, {bool, true}]}}
      ]
   ),

   {ok, _} = spawn(),
   {ok, ?entity} = ddb:put(?entity),

   mock_free().


ddb_put_invalid_identity(_) ->
   {ok, _} = spawn(),
   {error, {badarg, id, undefined}} = ddb:put(#test{}).


ddb_get(_) ->
   mock_init_get(
      [
         {<<"prefix">>, <<"test">>}
      ,  {<<"suffix">>, <<"1">>}
      ,  {<<"a">>,      1}
      ,  {<<"b">>,      1.0}
      ,  {<<"c">>,      <<"0123456789">>}
      ,  {<<"d">>,      true}
      ,  {<<"e">>,      [1, 1.0, <<"10">>, true]}
      ]
   ),

   {ok, _} = spawn(),
   {ok, ?entity} = ddb:get(?key),

   mock_free().

ddb_remove(_) ->
   mock_init_remove(
      [
         {<<"prefix">>, {s, <<"test">>}}
      ,  {<<"suffix">>, {s, <<"1">>}}
      ]
   ),

   {ok, _} = spawn(),
   {ok, ?key} = ddb:remove(?key),

   mock_free().

ddb_update(_) ->
   mock_init_update(
      [
         {<<"prefix">>, {s, <<"test">>}}
      ,  {<<"suffix">>, {s, <<"1">>}}
      ],
      [
         {<<"a">>,      {n, 1}}
      ,  {<<"b">>,      {n, 1.0}}
      ,  {<<"c">>,      {s, <<"0123456789">>}}
      ,  {<<"d">>,      {bool, true}}
      ,  {<<"e">>,      {l, [{n, 1}, {n, 1.0}, {s, <<"10">>}, {bool, true}]}}
      ]
   ),

   {ok, _} = spawn(),
   {ok, ?entity} = ddb:update(?entity),

   mock_free().

ddb_match(_) ->
   mock_init_match([
      [
         {<<"prefix">>, <<"test">>}
      ,  {<<"suffix">>, <<"1">>}
      ,  {<<"a">>,      1}
      ,  {<<"b">>,      1.0}
      ,  {<<"c">>,      <<"0123456789">>}
      ,  {<<"d">>,      true}
      ,  {<<"e">>,      [1, 1.0, <<"10">>, true]}
      ]
   ]),

   {ok, _} = spawn(),
   {ok, {[?entity], undefined}} = ddb:match(?key),

   mock_free().

%%
%%
spawn() ->
   ddb:start_link(test, ?ddb, labelled:encode(#test{}), labelled:decode(#test{})).

mock_init_put(Expect) ->
   mock_init(),
   meck:expect(erlcloud_ddb2, put_item, 
      fun(<<"dev-styx-1-test">>, X, _, #aws_config{}) -> 
         X = lists:keysort(1, Expect),
         ok
      end
   ).

mock_init_get(Expect) ->
   mock_init(),
   meck:expect(erlcloud_ddb2, get_item,
      fun(<<"dev-styx-1-test">>, _, _, #aws_config{}) -> 
         {ok, Expect}
      end
   ).

mock_init_remove(Expect) ->
   mock_init(),
   meck:expect(erlcloud_ddb2, delete_item,
      fun(<<"dev-styx-1-test">>, X, _, #aws_config{}) ->
         X = lists:keysort(1, Expect)
      end
   ).

mock_init_update(Key, Expect) ->
   mock_init(),
   meck:expect(erlcloud_ddb2, update_item, 
      fun(<<"dev-styx-1-test">>, K, X, _, #aws_config{}) ->
         K = lists:keysort(1, Key),
         X = lists:keysort(1, Expect),
         ok
      end
   ).

mock_init_match(Expect) ->
   mock_init(),
   meck:expect(erlcloud_ddb2, q, 
      fun(<<"dev-styx-1-test">>, _, _, #aws_config{}) ->
         {ok, #ddb2_q{items = Expect}}
      end
   ).

mock_init() ->
   meck:new(erlcloud_aws, [passthrough, unstick]),
   meck:new(erlcloud_ddb2, [passthrough, unstick]),
   meck:expect(erlcloud_aws, auto_config, fun() -> {ok, #aws_config{}} end).

mock_free() ->
   meck:unload(erlcloud_ddb2),
   meck:unload(erlcloud_aws).
