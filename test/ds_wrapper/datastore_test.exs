defmodule DsWrapper.DatastoreTest do
  use ExUnit.Case, async: true
  import Mox

  setup :verify_on_exit!

  alias DsWrapper.Datastore
  alias DsWrapper.GoogleApiProjectsMock

  alias GoogleApi.Datastore.V1.Model.{
    BeginTransactionRequest,
    BeginTransactionResponse,
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
    ReadOnly,
    ReadOptions,
    ReadWrite,
    RollbackRequest,
    RollbackResponse,
    RunQueryRequest,
    RunQueryResponse,
    TransactionOptions,
    Value
  }

  @conn %DsWrapper.Connection{connection: %Tesla.Client{}, project_id: "test"}

  @kind "SomeKind"
  @key_name "some-id"
  @key %Key{path: [%PathElement{kind: @kind, name: @key_name}]}
  @property_name "some_property"
  @property_value "some value"
  @entity %Entity{key: @key, properties: %{@property_name => %Value{stringValue: @property_value}}}

  setup do
    {:ok, pid} = DsWrapper.MutationStore.start_link()
    %{tx_conn: %DsWrapper.Connection{@conn | transaction_id: "transaction-id", mutation_store_pid: pid}}
  end

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

    test "call datastore_projects_run_query with transaction", %{tx_conn: tx_conn} do
      query = Datastore.query("SomeKind")

      GoogleApiProjectsMock
      |> expect(:datastore_projects_run_query, fn _, _, [body: body] ->
        assert body == %RunQueryRequest{query: query, readOptions: %ReadOptions{transaction: tx_conn.transaction_id}}

        {:ok, %RunQueryResponse{batch: %QueryResultBatch{}}}
      end)

      Datastore.run_query(tx_conn, query)
    end

    test "returns a cursor and entities" do
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

  describe "run_query!/1" do
    test "returns the result when run_query/1 returns {:ok, result}" do
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
      assert Datastore.run_query!(@conn, query) == %{cursor: "end-cursor", entities: entities}
    end

    test "raises an exception when run_query/1 returns {:error, reason}" do
      query = Datastore.query("SomeKind")

      GoogleApiProjectsMock
      |> expect(:datastore_projects_run_query, fn _, _, _ ->
        {:error, "some error"}
      end)

      assert_raise RuntimeError, fn -> Datastore.run_query!(@conn, query) end
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

    test "call datastore_projects_lookup with transaction", %{tx_conn: tx_conn} do
      GoogleApiProjectsMock
      |> expect(:datastore_projects_lookup, fn _, _, [body: body] ->
        assert body == %LookupRequest{keys: [@key], readOptions: %ReadOptions{transaction: tx_conn.transaction_id}}

        {:ok, %LookupResponse{}}
      end)

      Datastore.find(tx_conn, @key)
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

  describe "find!/2" do
    test "returns the result when find/2 returns {:ok, result}" do
      GoogleApiProjectsMock
      |> expect(:datastore_projects_lookup, fn _, _, _ ->
        {:ok, %LookupResponse{found: [@entity]}}
      end)

      assert Datastore.find!(@conn, @key) == DsWrapper.Entity.to_map(@entity)
    end

    test "raises an exception when find/2 returns {:error, reason}" do
      GoogleApiProjectsMock
      |> expect(:datastore_projects_lookup, fn _, _, _ ->
        {:error, "some error"}
      end)

      assert_raise RuntimeError, fn -> Datastore.find!(@conn, @key) end
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

    test "call datastore_projects_lookup with transaction", %{tx_conn: tx_conn} do
      another_key = %Key{path: [%PathElement{kind: @kind, name: "another-name"}]}
      keys = [@key, another_key]

      GoogleApiProjectsMock
      |> expect(:datastore_projects_lookup, fn _, _, [body: body] ->
        assert body == %LookupRequest{keys: keys, readOptions: %ReadOptions{transaction: tx_conn.transaction_id}}

        {:ok, %LookupResponse{}}
      end)

      Datastore.find_all(tx_conn, keys)
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

  describe "find_all!/2" do
    test "returns the result when find_all/2 returns {:ok, result}" do
      another_key = %Key{path: [%PathElement{kind: @kind, name: "another-name"}]}
      another_entity = %Entity{key: another_key, properties: %{@property_name => %Value{stringValue: "another value"}}}
      keys = [@key, another_key]
      entities = [@entity, another_entity]

      GoogleApiProjectsMock
      |> expect(:datastore_projects_lookup, fn _, _, _ ->
        {:ok, %LookupResponse{found: entities}}
      end)

      assert Datastore.find_all!(@conn, keys) == %{found: Enum.map(entities, &DsWrapper.Entity.to_map/1), missing: nil, deferred: nil}
    end

    test "raises an exception when find_all/2 returns {:error, reason}" do
      GoogleApiProjectsMock
      |> expect(:datastore_projects_lookup, fn _, _, _ ->
        {:error, "some error"}
      end)

      assert_raise RuntimeError, fn -> Datastore.find_all!(@conn, [@key]) end
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

    # for transaction

    test "don't call datastore_projects_commit when transaction mode", %{tx_conn: tx_conn} do
      GoogleApiProjectsMock
      |> expect(:datastore_projects_commit, 0, fn _, _, _ -> nil end)

      Datastore.insert(tx_conn, @entity)
    end

    test "returns {:ok, nil} when transaction mode", %{tx_conn: tx_conn} do
      assert Datastore.insert(tx_conn, @entity) == {:ok, nil}
    end

    test "add mutations to MutationStore when transaction mode", %{tx_conn: tx_conn} do
      another_entity = %Entity{key: %Key{path: [%PathElement{kind: @kind}]}, properties: %{@property_name => %Value{stringValue: "another value"}}}

      Datastore.insert(tx_conn, [@entity, another_entity])
      assert DsWrapper.MutationStore.get_all(tx_conn.mutation_store_pid) == [%Mutation{insert: @entity}, %Mutation{insert: another_entity}]
    end
  end

  describe "insert!/2" do
    test "returns the result when insert/2 returns {:ok, result}" do
      another_key = %Key{path: [%PathElement{kind: @kind, id: "1234"}]}
      another_entity = %Entity{key: %Key{path: [%PathElement{kind: @kind}]}, properties: %{@property_name => %Value{stringValue: "another value"}}}

      GoogleApiProjectsMock
      |> expect(:datastore_projects_commit, fn _, _, _ ->
        {:ok, %CommitResponse{mutationResults: [%MutationResult{key: nil}, %MutationResult{key: another_key}]}}
      end)

      assert Datastore.insert!(@conn, [@entity, another_entity]) == [@key, another_key]
    end

    test "raises an exception when insert/2 returns {:error, reason}" do
      GoogleApiProjectsMock
      |> expect(:datastore_projects_commit, fn _, _, _ ->
        {:error, "some error"}
      end)

      assert_raise RuntimeError, fn -> Datastore.insert!(@conn, [@entity]) end
    end

    test "returns nil when transaction mode", %{tx_conn: tx_conn} do
      another_entity = %Entity{key: %Key{path: [%PathElement{kind: @kind}]}, properties: %{@property_name => %Value{stringValue: "another value"}}}

      assert Datastore.insert!(tx_conn, [@entity, another_entity]) == nil
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

    # for transaction

    test "don't call datastore_projects_commit when transaction mode", %{tx_conn: tx_conn} do
      GoogleApiProjectsMock
      |> expect(:datastore_projects_commit, 0, fn _, _, _ -> nil end)

      Datastore.upsert(tx_conn, @entity)
    end

    test "returns {:ok, nil} when transaction mode", %{tx_conn: tx_conn} do
      assert Datastore.upsert(tx_conn, @entity) == {:ok, nil}
    end

    test "add mutations to MutationStore when transaction mode", %{tx_conn: tx_conn} do
      another_entity = %Entity{key: %Key{path: [%PathElement{kind: @kind}]}, properties: %{@property_name => %Value{stringValue: "another value"}}}

      Datastore.upsert(tx_conn, [@entity, another_entity])
      assert DsWrapper.MutationStore.get_all(tx_conn.mutation_store_pid) == [%Mutation{upsert: @entity}, %Mutation{upsert: another_entity}]
    end
  end

  describe "upsert!/2" do
    test "returns the result when upsert/2 returns {:ok, result}" do
      another_key = %Key{path: [%PathElement{kind: @kind, id: "1234"}]}
      another_entity = %Entity{key: %Key{path: [%PathElement{kind: @kind}]}, properties: %{@property_name => %Value{stringValue: "another value"}}}

      GoogleApiProjectsMock
      |> expect(:datastore_projects_commit, fn _, _, _ ->
        {:ok, %CommitResponse{mutationResults: [%MutationResult{key: nil}, %MutationResult{key: another_key}]}}
      end)

      assert Datastore.upsert!(@conn, [@entity, another_entity]) == [@key, another_key]
    end

    test "raises an exception when upsert/2 returns {:error, reason}" do
      GoogleApiProjectsMock
      |> expect(:datastore_projects_commit, fn _, _, _ ->
        {:error, "some error"}
      end)

      assert_raise RuntimeError, fn -> Datastore.upsert!(@conn, [@entity]) end
    end

    test "returns nil when transaction mode", %{tx_conn: tx_conn} do
      another_entity = %Entity{key: %Key{path: [%PathElement{kind: @kind}]}, properties: %{@property_name => %Value{stringValue: "another value"}}}

      assert Datastore.upsert!(tx_conn, [@entity, another_entity]) == nil
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

    # for transaction

    test "don't call datastore_projects_commit when transaction mode", %{tx_conn: tx_conn} do
      GoogleApiProjectsMock
      |> expect(:datastore_projects_commit, 0, fn _, _, _ -> nil end)

      Datastore.update(tx_conn, @entity)
    end

    test "returns {:ok, nil} when transaction mode", %{tx_conn: tx_conn} do
      assert Datastore.update(tx_conn, @entity) == {:ok, nil}
    end

    test "add mutations to MutationStore when transaction mode", %{tx_conn: tx_conn} do
      another_entity = %Entity{key: %Key{path: [%PathElement{kind: @kind}]}, properties: %{@property_name => %Value{stringValue: "another value"}}}

      Datastore.update(tx_conn, [@entity, another_entity])
      assert DsWrapper.MutationStore.get_all(tx_conn.mutation_store_pid) == [%Mutation{update: @entity}, %Mutation{update: another_entity}]
    end
  end

  describe "update!/2" do
    test "returns the result when update/2 returns {:ok, result}" do
      another_key = %Key{path: [%PathElement{kind: @kind, id: "1234"}]}
      another_entity = %Entity{key: %Key{path: [%PathElement{kind: @kind}]}, properties: %{@property_name => %Value{stringValue: "another value"}}}

      GoogleApiProjectsMock
      |> expect(:datastore_projects_commit, fn _, _, _ ->
        {:ok, %CommitResponse{mutationResults: [%MutationResult{key: nil}, %MutationResult{key: another_key}]}}
      end)

      assert Datastore.update!(@conn, [@entity, another_entity]) == [@key, another_key]
    end

    test "raises an exception when update/2 returns {:error, reason}" do
      GoogleApiProjectsMock
      |> expect(:datastore_projects_commit, fn _, _, _ ->
        {:error, "some error"}
      end)

      assert_raise RuntimeError, fn -> Datastore.update!(@conn, [@entity]) end
    end

    test "returns nil when transaction mode", %{tx_conn: tx_conn} do
      another_entity = %Entity{key: %Key{path: [%PathElement{kind: @kind}]}, properties: %{@property_name => %Value{stringValue: "another value"}}}

      assert Datastore.update!(tx_conn, [@entity, another_entity]) == nil
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

    # for transaction

    test "don't call datastore_projects_commit when transaction mode", %{tx_conn: tx_conn} do
      GoogleApiProjectsMock
      |> expect(:datastore_projects_commit, 0, fn _, _, _ -> nil end)

      Datastore.delete(tx_conn, @entity)
    end

    test "returns :ok when transaction mode", %{tx_conn: tx_conn} do
      assert Datastore.delete(tx_conn, @entity) == :ok
    end

    test "add mutations to MutationStore when transaction mode", %{tx_conn: tx_conn} do
      Datastore.delete(tx_conn, @key)
      assert DsWrapper.MutationStore.get_all(tx_conn.mutation_store_pid) == [%Mutation{delete: @key}]
    end
  end

  describe "delete!/2" do
    test "returns the result when delete/2 returns {:ok, result}" do
      GoogleApiProjectsMock
      |> expect(:datastore_projects_commit, fn _, _, _ ->
        {:ok, %CommitResponse{mutationResults: [%MutationResult{key: nil}]}}
      end)

      assert Datastore.delete!(@conn, @key) == :ok
    end

    test "raises an exception when delete/2 returns {:error, reason}" do
      GoogleApiProjectsMock
      |> expect(:datastore_projects_commit, fn _, _, _ ->
        {:error, "some error"}
      end)

      assert_raise RuntimeError, fn -> Datastore.delete!(@conn, [@entity]) end
    end

    test "returns :ok when transaction mode", %{tx_conn: tx_conn} do
      another_entity = %Entity{key: %Key{path: [%PathElement{kind: @kind}]}, properties: %{@property_name => %Value{stringValue: "another value"}}}

      assert Datastore.delete!(tx_conn, [@entity, another_entity]) == :ok
    end
  end

  describe "transaction/1" do
    test "call datastore_projects_begin_transaction" do
      GoogleApiProjectsMock
      |> expect(:datastore_projects_begin_transaction, fn _, _, [body: body] ->
        assert body == %BeginTransactionRequest{transactionOptions: %TransactionOptions{readWrite: %ReadWrite{}}}
      end)

      Datastore.transaction(@conn)
    end

    test "call datastore_projects_begin_transaction with previousTransaction option when pass previous_transaction" do
      GoogleApiProjectsMock
      |> expect(:datastore_projects_begin_transaction, fn _, _, [body: body] ->
        assert body == %BeginTransactionRequest{transactionOptions: %TransactionOptions{readWrite: %ReadWrite{previousTransaction: "prev-transaction-id"}}}
      end)

      Datastore.transaction(@conn, previous_transaction: "prev-transaction-id")
    end

    test "call datastore_projects_begin_transaction with readOnly option when pass :read_only" do
      GoogleApiProjectsMock
      |> expect(:datastore_projects_begin_transaction, fn _, _, [body: body] ->
        assert body == %BeginTransactionRequest{transactionOptions: %TransactionOptions{readOnly: %ReadOnly{}}}
      end)

      Datastore.transaction(@conn, read_only: true)
    end

    test "returns connection with transaction_id and mutation_store_pid" do
      GoogleApiProjectsMock
      |> expect(:datastore_projects_begin_transaction, fn _, _, _ ->
        {:ok, %BeginTransactionResponse{transaction: "transaction-id"}}
      end)

      assert {:ok, %DsWrapper.Connection{connection: %Tesla.Client{}, project_id: "test", transaction_id: transaction_id, mutation_store_pid: pid}} = Datastore.transaction(@conn)

      assert transaction_id == "transaction-id"
      assert pid != nil
    end
  end

  describe "run_in_transaction/1" do
    test "call datastore_projects_begin_transaction" do
      GoogleApiProjectsMock
      |> stub(:datastore_projects_commit, fn _, _, _ -> {:ok, nil} end)
      |> expect(:datastore_projects_begin_transaction, fn _, _, [body: body] ->
        assert body == %BeginTransactionRequest{transactionOptions: %TransactionOptions{readWrite: %ReadWrite{}}}
      end)

      Datastore.run_in_transaction(@conn, fn _ -> nil end)
    end

    test "call datastore_projects_commit when no exception occured" do
      another_entity = %Entity{key: %Key{path: [%PathElement{kind: @kind}]}, properties: %{@property_name => %Value{stringValue: "another value"}}}

      GoogleApiProjectsMock
      |> stub(:datastore_projects_begin_transaction, fn _, _, _ -> {:ok, %BeginTransactionResponse{transaction: "transaction-id"}} end)
      |> expect(:datastore_projects_commit, fn _, _, [body: body] ->
        assert body == %CommitRequest{
                 mode: "TRANSACTIONAL",
                 transaction: "transaction-id",
                 mutations: [%Mutation{insert: @entity}, %Mutation{update: another_entity}]
               }

        {:ok, %CommitResponse{mutationResults: []}}
      end)

      Datastore.run_in_transaction(@conn, fn tx_conn ->
        Datastore.insert(tx_conn, @entity)
        Datastore.update(tx_conn, another_entity)
      end)
    end

    test "call datastore_projects_rollback when exception occured" do
      GoogleApiProjectsMock
      |> stub(:datastore_projects_begin_transaction, fn _, _, _ -> {:ok, %BeginTransactionResponse{transaction: "transaction-id"}} end)
      |> expect(:datastore_projects_rollback, fn _, _, [body: body] ->
        assert body == %RollbackRequest{transaction: "transaction-id"}

        {:ok, %RollbackResponse{}}
      end)

      Datastore.run_in_transaction(@conn, fn _ ->
        raise "error"
      end)
    end

    test "returns {:ok, result} when no exception occured" do
      GoogleApiProjectsMock
      |> stub(:datastore_projects_begin_transaction, fn _, _, _ -> {:ok, %BeginTransactionResponse{transaction: "transaction-id"}} end)
      |> expect(:datastore_projects_commit, fn _, _, _ ->
        {:ok, %CommitResponse{mutationResults: []}}
      end)

      assert Datastore.run_in_transaction(@conn, fn tx_conn ->
               Datastore.insert(tx_conn, @entity)
               "result"
             end) == {:ok, "result"}
    end

    test "returns {:error, reason} when exception occured" do
      GoogleApiProjectsMock
      |> stub(:datastore_projects_begin_transaction, fn _, _, _ -> {:ok, %BeginTransactionResponse{transaction: "transaction-id"}} end)
      |> expect(:datastore_projects_rollback, fn _, _, _ ->
        {:ok, %RollbackResponse{}}
      end)

      assert Datastore.run_in_transaction(@conn, fn _ ->
               raise RuntimeError, "reason"
             end) == {:error, %RuntimeError{message: "reason"}}
    end
  end

  describe "commit/1" do
    test "call datastore_projects_commit", %{tx_conn: tx_conn} do
      another_entity = %Entity{key: %Key{path: [%PathElement{kind: @kind}]}, properties: %{@property_name => %Value{stringValue: "another value"}}}

      GoogleApiProjectsMock
      |> expect(:datastore_projects_commit, fn _, _, [body: body] ->
        assert body == %CommitRequest{
                 mode: "TRANSACTIONAL",
                 transaction: tx_conn.transaction_id,
                 mutations: [%Mutation{insert: @entity}, %Mutation{update: another_entity}]
               }

        {:ok, %CommitResponse{mutationResults: []}}
      end)

      Datastore.insert(tx_conn, @entity)
      Datastore.update(tx_conn, another_entity)
      Datastore.commit(tx_conn)
    end

    test "stop MutationStore", %{tx_conn: tx_conn} do
      GoogleApiProjectsMock |> expect(:datastore_projects_commit, fn _, _, _ -> {:ok, %CommitResponse{mutationResults: []}} end)
      Datastore.commit(tx_conn)

      refute Process.alive?(tx_conn.mutation_store_pid)
    end
  end

  describe "rollback/1" do
    test "call datastore_projects_rollback", %{tx_conn: tx_conn} do
      GoogleApiProjectsMock
      |> expect(:datastore_projects_rollback, fn _, _, [body: body] ->
        assert body == %RollbackRequest{transaction: tx_conn.transaction_id}

        {:ok, %RollbackResponse{}}
      end)

      Datastore.rollback(tx_conn)
    end

    test "stop MutationStore", %{tx_conn: tx_conn} do
      GoogleApiProjectsMock |> expect(:datastore_projects_rollback, fn _, _, _ -> {:ok, %RollbackResponse{}} end)
      Datastore.rollback(tx_conn)

      refute Process.alive?(tx_conn.mutation_store_pid)
    end
  end
end
