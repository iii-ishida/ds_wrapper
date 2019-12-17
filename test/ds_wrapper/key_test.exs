defmodule DsWrapper.KeyTest do
  use ExUnit.Case, async: true
  doctest DsWrapper.Key

  alias GoogleApi.Datastore.V1.Model.{Key, PathElement}

  describe "new/3" do
    test "without parent or name" do
      assert DsWrapper.Key.new("SomeKind") == %Key{path: [%PathElement{kind: "SomeKind"}]}
    end

    test "without parent, with name" do
      assert DsWrapper.Key.new("SomeKind", "some-name") == %Key{path: [%PathElement{kind: "SomeKind", name: "some-name"}]}
    end

    test "with parent and name" do
      parent = DsWrapper.Key.new("ParentKind", "parent-name")

      assert DsWrapper.Key.new("SomeKind", "some-name", parent) == %Key{
               path: [
                 %PathElement{kind: "ParentKind", name: "parent-name"},
                 %PathElement{kind: "SomeKind", name: "some-name"}
               ]
             }
    end
  end
end
