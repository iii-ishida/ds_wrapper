defmodule DsWrapper.ValueTest do
  use ExUnit.Case, async: true
  doctest DsWrapper.Value

  alias GoogleApi.Datastore.V1.Model.{ArrayValue, Entity, Key, LatLng, PathElement, Value}

  describe "from_native/2" do
    test "integerValue" do
      assert DsWrapper.Value.from_native(123) == %Value{integerValue: "123"}
      assert DsWrapper.Value.from_native(123, true) == %Value{integerValue: "123", excludeFromIndexes: true}
    end

    test "doubleValue" do
      assert DsWrapper.Value.from_native(12.3) == %Value{doubleValue: 12.3}
      assert DsWrapper.Value.from_native(12.3, true) == %Value{doubleValue: 12.3, excludeFromIndexes: true}
    end

    test "stringValue" do
      assert DsWrapper.Value.from_native("some value") == %Value{stringValue: "some value"}
      assert DsWrapper.Value.from_native("some value", true) == %Value{stringValue: "some value", excludeFromIndexes: true}
    end

    test "nullValue" do
      assert DsWrapper.Value.from_native(nil) == %Value{nullValue: "NULL_VALUE"}
      assert DsWrapper.Value.from_native(nil, true) == %Value{nullValue: "NULL_VALUE", excludeFromIndexes: true}
    end

    test "geoPointValue" do
      assert DsWrapper.Value.from_native(%{latitude: 123, longitude: 456}) == %Value{geoPointValue: %LatLng{latitude: 123, longitude: 456}}
      assert DsWrapper.Value.from_native(%{latitude: 123, longitude: 456}, true) == %Value{geoPointValue: %LatLng{latitude: 123, longitude: 456}, excludeFromIndexes: true}
    end

    test "keyValue" do
      key = %Key{path: [%PathElement{kind: "SomeKind", name: "some-name"}]}

      assert DsWrapper.Value.from_native(key) == %Value{keyValue: key}
      assert DsWrapper.Value.from_native(key, true) == %Value{keyValue: key, excludeFromIndexes: true}
    end

    test "entityValue" do
      entity = %Entity{
        key: %Key{path: [%PathElement{kind: "SomeKind", name: "some-name"}]},
        properties: %{"some_property" => %Value{stringValue: "some value"}}
      }

      assert DsWrapper.Value.from_native(entity) == %Value{entityValue: entity}
      assert DsWrapper.Value.from_native(entity, true) == %Value{entityValue: entity, excludeFromIndexes: true}
    end

    test "arrayValue" do
      assert DsWrapper.Value.from_native([123, "some string"]) == %Value{arrayValue: %ArrayValue{values: [%Value{integerValue: "123"}, %Value{stringValue: "some string"}]}}

      assert DsWrapper.Value.from_native([123, "some string"], true) == %Value{
               arrayValue: %ArrayValue{values: [%Value{integerValue: "123"}, %Value{stringValue: "some string"}]},
               excludeFromIndexes: true
             }
    end
  end

  describe "to_native/1" do
    test "integerValue" do
      assert DsWrapper.Value.to_native(%Value{integerValue: "123"}) == 123
    end

    test "doubleValue" do
      assert DsWrapper.Value.to_native(%Value{doubleValue: 12.3}) == 12.3
    end

    test "stringValue" do
      assert DsWrapper.Value.to_native(%Value{stringValue: "some value"}) == "some value"
    end

    test "nullValue" do
      assert DsWrapper.Value.to_native(%Value{}) == nil
    end

    test "geoPointValue" do
      assert DsWrapper.Value.to_native(%Value{geoPointValue: %LatLng{latitude: 123, longitude: 456}}) == %{latitude: 123, longitude: 456}
    end

    test "keyValue" do
      key = %Key{path: [%PathElement{kind: "SomeKind", name: "some-name"}]}

      assert DsWrapper.Value.to_native(%Value{keyValue: key}) == key
    end

    test "entityValue" do
      entity = %Entity{
        key: %Key{path: [%PathElement{kind: "SomeKind", name: "some-name"}]},
        properties: %{"some_property" => %Value{stringValue: "some value"}}
      }

      assert DsWrapper.Value.to_native(%Value{entityValue: entity}) == %{"some_property" => "some value"}
    end

    test "arrayValue" do
      assert DsWrapper.Value.to_native(%Value{arrayValue: %ArrayValue{values: [%Value{integerValue: "123"}, %Value{stringValue: "some string"}]}}) == [123, "some string"]
    end
  end
end
