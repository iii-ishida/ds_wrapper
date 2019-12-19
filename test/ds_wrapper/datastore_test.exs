defmodule DsWrapper.DatastoreTest do
  use ExUnit.Case, async: true
  import Mox

  alias DsWrapper.Datastore
  alias DsWrapper.GoogleApiProjectsMock

  alias GoogleApi.Datastore.V1.Model.{
    Entity,
    EntityResult,
    Key,
    LookupRequest,
    LookupResponse,
    PathElement,
    QueryResultBatch,
    ReadOptions,
    RunQueryRequest,
    RunQueryResponse,
    Value
  }

  @conn %DsWrapper.Connection{connection: %Tesla.Client{}, project_id: "test"}

  @kind "SomeKind"
  @key_name "some-id"
  @key %Key{path: [%PathElement{kind: @kind, name: @key_name}]}
  @property_name "some_property"
  @property_value "some value"
  @entity %Entity{key: @key, properties: %{@property_name => %Value{stringValue: @property_value}}}

  test "run_query/1" do
    query = Datastore.query("SomeKind")

    GoogleApiProjectsMock
    |> expect(:datastore_projects_run_query, fn _, _, [body: body] ->
      assert body == %RunQueryRequest{query: query, readOptions: %ReadOptions{}}

      {:ok,
       %RunQueryResponse{
         batch: %QueryResultBatch{endCursor: "end-cursor", entityResults: [%EntityResult{cursor: "end-cursor", entity: @entity}]},
         query: query
       }}
    end)

    entities = [DsWrapper.Entity.to_map(@entity)]
    assert Datastore.run_query(@conn, query) == {:ok, %{cursor: "end-cursor", entities: entities}}
  end

  describe "find/2" do
    test "when found" do
      GoogleApiProjectsMock
      |> expect(:datastore_projects_lookup, fn _, _, [body: body] ->
        assert body == %LookupRequest{keys: [@key], readOptions: %ReadOptions{}}

        {:ok, %LookupResponse{found: [@entity]}}
      end)

      assert Datastore.find(@conn, @key) == {:ok, DsWrapper.Entity.to_map(@entity)}
    end

    test "when not found" do
      GoogleApiProjectsMock
      |> expect(:datastore_projects_lookup, fn _, _, _ ->
        {:ok, %LookupResponse{missing: [%EntityResult{entity: @entity}]}}
      end)

      assert Datastore.find(@conn, @key) == {:ok, nil}
    end
  end

  describe "find_all/2" do
    test "when found" do
      another_key = %Key{path: [%PathElement{kind: @kind, name: "another-name"}]}
      another_entity = %Entity{key: another_key, properties: %{@property_name => %Value{stringValue: "another value"}}}
      keys = [@key, another_key]
      entities = [@entity, another_entity]

      GoogleApiProjectsMock
      |> expect(:datastore_projects_lookup, fn _, _, [body: body] ->
        assert body == %LookupRequest{keys: keys, readOptions: %ReadOptions{}}

        {:ok, %LookupResponse{found: entities}}
      end)

      assert Datastore.find_all(@conn, keys) == {:ok, %{found: Enum.map(entities, &DsWrapper.Entity.to_map/1), missing: nil, deferred: nil}}
    end

    test "when not found" do
      another_key = %Key{path: [%PathElement{kind: @kind, name: "another-name"}]}
      another_entity = %Entity{key: another_key, properties: %{@property_name => %Value{stringValue: "another value"}}}
      keys = [@key, another_key]

      GoogleApiProjectsMock
      |> expect(:datastore_projects_lookup, fn _, _, _ ->
        {:ok, %LookupResponse{missing: [%EntityResult{entity: @entity}, %EntityResult{entity: another_entity}]}}
      end)

      assert Datastore.find_all(@conn, keys) == {:ok, %{found: nil, missing: keys, deferred: nil}}
    end
  end
end
