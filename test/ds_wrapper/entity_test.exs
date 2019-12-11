defmodule DsWrapper.EntityTest do
  use ExUnit.Case, async: true

  alias GoogleApi.Datastore.V1.Model.{Entity, EntityResult, Key, PathElement, Value}

  @key %Key{path: [%PathElement{kind: "SomeKind", name: "some-name"}]}

  describe "new/3" do
    test "without exclude_from_indexes" do
      assert DsWrapper.Entity.new(@key, %{"some_property" => "some value"}) == %Entity{
               key: @key,
               properties: %{"some_property" => %Value{stringValue: "some value", excludeFromIndexes: false}}
             }
    end

    test "with exclude_from_indexes" do
      properties = %{"some_property_01" => "some value 01", "some_property_02" => "some value 02"}

      assert DsWrapper.Entity.new(@key, properties, ["some_property_02"]) == %Entity{
               key: @key,
               properties: %{
                 "some_property_01" => %Value{stringValue: "some value 01", excludeFromIndexes: false},
                 "some_property_02" => %Value{stringValue: "some value 02", excludeFromIndexes: true}
               }
             }
    end
  end

  describe "to_map/1" do
    test "with an nil" do
      assert DsWrapper.Entity.to_map(nil) == nil
    end

    test "with an EntityResult" do
      entity = %Entity{
        key: @key,
        properties: %{"some_property" => %Value{stringValue: "some value"}}
      }

      assert DsWrapper.Entity.to_map(%EntityResult{entity: entity}) == %{"some_property" => "some value"}
    end

    test "with an Entity" do
      entity = %Entity{
        key: @key,
        properties: %{"some_property" => %Value{stringValue: "some value"}}
      }

      assert DsWrapper.Entity.to_map(entity) == %{"some_property" => "some value"}
    end
  end
end
