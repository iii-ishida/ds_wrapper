defmodule DsWrapper.MutationTest do
  use ExUnit.Case, async: true

  alias GoogleApi.Datastore.V1.Model.{Entity, Key, Mutation, PathElement, Value}

  @key_1 %Key{path: [%PathElement{kind: "SomeKind", name: "key-01"}]}
  @key_2 %Key{path: [%PathElement{kind: "SomeKind", name: "key-02"}]}
  @entity_1 %Entity{key: @key_1, properties: %{"some_property" => %Value{stringValue: "some value 1"}}}
  @entity_2 %Entity{key: @key_2, properties: %{"some_property" => %Value{stringValue: "some value 2"}}}

  describe "for_insert/1" do
    test "an entity" do
      assert DsWrapper.Mutation.for_insert(@entity_1) == [%Mutation{insert: @entity_1}]
    end

    test "entities" do
      assert DsWrapper.Mutation.for_insert([@entity_1, @entity_2]) == [%Mutation{insert: @entity_1}, %Mutation{insert: @entity_2}]
    end
  end

  describe "for_upsert/1" do
    test "an entity" do
      assert DsWrapper.Mutation.for_upsert(@entity_1) == [%Mutation{upsert: @entity_1}]
    end

    test "entities" do
      assert DsWrapper.Mutation.for_upsert([@entity_1, @entity_2]) == [%Mutation{upsert: @entity_1}, %Mutation{upsert: @entity_2}]
    end
  end

  describe "for_update/1" do
    test "an entity" do
      assert DsWrapper.Mutation.for_update(@entity_1) == [%Mutation{update: @entity_1}]
    end

    test "entities" do
      assert DsWrapper.Mutation.for_update([@entity_1, @entity_2]) == [%Mutation{update: @entity_1}, %Mutation{update: @entity_2}]
    end
  end

  describe "for_delete/1" do
    test "a key" do
      assert DsWrapper.Mutation.for_delete(@key_1) == [%Mutation{delete: @key_1}]
    end

    test "keys" do
      assert DsWrapper.Mutation.for_delete([@key_1, @key_2]) == [%Mutation{delete: @key_1}, %Mutation{delete: @key_2}]
    end
  end
end
