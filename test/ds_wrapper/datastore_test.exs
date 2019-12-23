defmodule DsWrapper.DatastoreTest do
  use ExUnit.Case, async: true
  import Mox

  alias DsWrapper.Datastore
  alias DsWrapper.GoogleApiProjectsMock

  alias GoogleApi.Datastore.V1.Model.{
    CommitRequest,
    CommitResponse,
    Entity,
    EntityResult,
    Key,
    LookupRequest,
    LookupResponse,
    Mutation,
    MutationResult,
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

  describe "run_query/1" do
    test "call datastore_projects_run_query" do
      query = Datastore.query("SomeKind")

      GoogleApiProjectsMock
      |> expect(:datastore_projects_run_query, fn _, _, [body: body] ->
        assert body == %RunQueryRequest{query: query, readOptions: %ReadOptions{}}

        {:ok, %RunQueryResponse{batch: %QueryResultBatch{}}}
      end)

      Datastore.run_query(@conn, query)
    end

    test "return a cursor and entities" do
      query = Datastore.query("SomeKind")

      GoogleApiProjectsMock
      |> expect(:datastore_projects_run_query, fn _, _, _ ->
        {:ok,
         %RunQueryResponse{
           batch: %QueryResultBatch{endCursor: "end-cursor", entityResults: [%EntityResult{cursor: "end-cursor", entity: @entity}]},
           query: query
         }}
      end)

      entities = [DsWrapper.Entity.to_map(@entity)]
      assert Datastore.run_query(@conn, query) == {:ok, %{cursor: "end-cursor", entities: entities}}
    end
  end

  describe "find/2" do
    test "call datastore_projects_lookup" do
      GoogleApiProjectsMock
      |> expect(:datastore_projects_lookup, fn _, _, [body: body] ->
        assert body == %LookupRequest{keys: [@key], readOptions: %ReadOptions{}}

        {:ok, %LookupResponse{}}
      end)

      Datastore.find(@conn, @key)
    end

    test "when found" do
      GoogleApiProjectsMock
      |> expect(:datastore_projects_lookup, fn _, _, _ ->
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
    test "call datastore_projects_lookup" do
      another_key = %Key{path: [%PathElement{kind: @kind, name: "another-name"}]}
      keys = [@key, another_key]

      GoogleApiProjectsMock
      |> expect(:datastore_projects_lookup, fn _, _, [body: body] ->
        assert body == %LookupRequest{keys: keys, readOptions: %ReadOptions{}}

        {:ok, %LookupResponse{}}
      end)

      Datastore.find_all(@conn, keys)
    end

    test "when found" do
      another_key = %Key{path: [%PathElement{kind: @kind, name: "another-name"}]}
      another_entity = %Entity{key: another_key, properties: %{@property_name => %Value{stringValue: "another value"}}}
      keys = [@key, another_key]
      entities = [@entity, another_entity]

      GoogleApiProjectsMock
      |> expect(:datastore_projects_lookup, fn _, _, _ ->
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

  describe "insert/2" do
    test "call datastore_projects_commit" do
      GoogleApiProjectsMock
      |> expect(:datastore_projects_commit, fn _, _, [body: body] ->
        assert body == %CommitRequest{
                 mode: "NON_TRANSACTIONAL",
                 mutations: [%Mutation{insert: @entity}]
               }

        {:ok, %CommitResponse{mutationResults: []}}
      end)

      Datastore.insert(@conn, @entity)
    end

    test "returns keys for entities" do
      another_key = %Key{path: [%PathElement{kind: @kind, id: "1234"}]}
      another_entity = %Entity{key: %Key{path: [%PathElement{kind: @kind}]}, properties: %{@property_name => %Value{stringValue: "another value"}}}

      GoogleApiProjectsMock
      |> expect(:datastore_projects_commit, fn _, _, _ ->
        {:ok, %CommitResponse{mutationResults: [%MutationResult{key: nil}, %MutationResult{key: another_key}]}}
      end)

      assert Datastore.insert(@conn, [@entity, another_entity]) == {:ok, [@key, another_key]}
    end
  end

  describe "upsert/2" do
    test "call datastore_projects_commit" do
      GoogleApiProjectsMock
      |> expect(:datastore_projects_commit, fn _, _, [body: body] ->
        assert body == %CommitRequest{
                 mode: "NON_TRANSACTIONAL",
                 mutations: [%Mutation{upsert: @entity}]
               }

        {:ok, %CommitResponse{mutationResults: []}}
      end)

      Datastore.upsert(@conn, @entity)
    end

    test "returns keys for entities" do
      another_key = %Key{path: [%PathElement{kind: @kind, id: "1234"}]}
      another_entity = %Entity{key: %Key{path: [%PathElement{kind: @kind}]}, properties: %{@property_name => %Value{stringValue: "another value"}}}

      GoogleApiProjectsMock
      |> expect(:datastore_projects_commit, fn _, _, _ ->
        {:ok, %CommitResponse{mutationResults: [%MutationResult{key: nil}, %MutationResult{key: another_key}]}}
      end)

      assert Datastore.upsert(@conn, [@entity, another_entity]) == {:ok, [@key, another_key]}
    end
  end

  describe "update/2" do
    test "call datastore_projects_commit" do
      GoogleApiProjectsMock
      |> expect(:datastore_projects_commit, fn _, _, [body: body] ->
        assert body == %CommitRequest{
                 mode: "NON_TRANSACTIONAL",
                 mutations: [%Mutation{update: @entity}]
               }

        {:ok, %CommitResponse{mutationResults: []}}
      end)

      Datastore.update(@conn, @entity)
    end

    test "returns keys for entities" do
      another_key = %Key{path: [%PathElement{kind: @kind, id: "1234"}]}
      another_entity = %Entity{key: %Key{path: [%PathElement{kind: @kind}]}, properties: %{@property_name => %Value{stringValue: "another value"}}}

      GoogleApiProjectsMock
      |> expect(:datastore_projects_commit, fn _, _, _ ->
        {:ok, %CommitResponse{mutationResults: [%MutationResult{key: nil}, %MutationResult{key: another_key}]}}
      end)

      assert Datastore.update(@conn, [@entity, another_entity]) == {:ok, [@key, another_key]}
    end
  end

  describe "delete/2" do
    test "call datastore_projects_commit" do
      GoogleApiProjectsMock
      |> expect(:datastore_projects_commit, fn _, _, [body: body] ->
        assert body == %CommitRequest{
                 mode: "NON_TRANSACTIONAL",
                 mutations: [%Mutation{delete: @key}]
               }

        {:ok, %CommitResponse{mutationResults: []}}
      end)

      Datastore.delete(@conn, @key)
    end

    test "returns :ok" do
      GoogleApiProjectsMock
      |> expect(:datastore_projects_commit, fn _, _, _ ->
        {:ok, %CommitResponse{mutationResults: [%MutationResult{key: nil}]}}
      end)

      assert Datastore.delete(@conn, @key) == :ok
    end
  end
end
