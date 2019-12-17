defmodule DsWrapper.DatastoreTest do
  use ExUnit.Case, async: true
  import Mox

  alias DsWrapper.GoogleApiProjectsMock
  alias DsWrapper.Datastore

  alias GoogleApi.Datastore.V1.Model.{
    Entity,
    EntityResult,
    Key,
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
    assert {:ok, %{cursor: "end-cursor", entities: entities}} == Datastore.run_query(@conn, query)
  end
end
