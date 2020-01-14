defmodule DsWrapper.QueryTest do
  use ExUnit.Case, async: true

  alias GoogleApi.Datastore.V1.Model.{
    CompositeFilter,
    Filter,
    KindExpression,
    Projection,
    PropertyFilter,
    PropertyOrder,
    PropertyReference,
    Query,
    Value
  }

  @query DsWrapper.Query.new_query("SomeKind")

  test "new_query/1" do
    assert DsWrapper.Query.new_query("SomeKind") == %Query{
             kind: %KindExpression{name: "SomeKind"}
           }
  end

  describe "where/3" do
    test "single" do
      assert DsWrapper.Query.where(@query, "some_property", "=", "some value") == %Query{
               kind: %KindExpression{name: "SomeKind"},
               filter: %Filter{
                 compositeFilter: %CompositeFilter{
                   filters: [
                     %Filter{
                       propertyFilter: %PropertyFilter{
                         property: %PropertyReference{name: "some_property"},
                         op: "EQUAL",
                         value: %Value{stringValue: "some value"}
                       }
                     }
                   ],
                   op: "AND"
                 }
               }
             }
    end

    test "multiple" do
      query =
        @query
        |> DsWrapper.Query.where("some_property_01", "=", "some value 01")
        |> DsWrapper.Query.where("some_property_02", "=", "some value 02")

      assert query == %Query{
               kind: %KindExpression{name: "SomeKind"},
               filter: %Filter{
                 compositeFilter: %CompositeFilter{
                   filters: [
                     %Filter{
                       propertyFilter: %PropertyFilter{
                         property: %PropertyReference{name: "some_property_01"},
                         op: "EQUAL",
                         value: %Value{stringValue: "some value 01"}
                       }
                     },
                     %Filter{
                       propertyFilter: %PropertyFilter{
                         property: %PropertyReference{name: "some_property_02"},
                         op: "EQUAL",
                         value: %Value{stringValue: "some value 02"}
                       }
                     }
                   ],
                   op: "AND"
                 }
               }
             }
    end

    test "operator is =" do
      assert_equals_property_filter(DsWrapper.Query.where(@query, "some_property", "=", "some value"), %PropertyFilter{
        property: %PropertyReference{name: "some_property"},
        op: "EQUAL",
        value: %Value{stringValue: "some value"}
      })
    end

    test "operator is >" do
      assert_equals_property_filter(DsWrapper.Query.where(@query, "some_property", ">", "some value"), %PropertyFilter{
        property: %PropertyReference{name: "some_property"},
        op: "GREATER_THAN",
        value: %Value{stringValue: "some value"}
      })
    end

    test "operator is >=" do
      assert_equals_property_filter(DsWrapper.Query.where(@query, "some_property", ">=", "some value"), %PropertyFilter{
        property: %PropertyReference{name: "some_property"},
        op: "GREATER_THAN_OR_EQUAL",
        value: %Value{stringValue: "some value"}
      })
    end

    test "operator is <" do
      assert_equals_property_filter(DsWrapper.Query.where(@query, "some_property", "<", "some value"), %PropertyFilter{
        property: %PropertyReference{name: "some_property"},
        op: "LESS_THAN",
        value: %Value{stringValue: "some value"}
      })
    end

    test "operator is <=" do
      assert_equals_property_filter(DsWrapper.Query.where(@query, "some_property", "<=", "some value"), %PropertyFilter{
        property: %PropertyReference{name: "some_property"},
        op: "LESS_THAN_OR_EQUAL",
        value: %Value{stringValue: "some value"}
      })
    end
  end

  test "select/2" do
    assert DsWrapper.Query.select(@query, ["some_property_01", "some_property_02"]) == %Query{
             kind: %KindExpression{name: "SomeKind"},
             projection: [
               %Projection{property: %PropertyReference{name: "some_property_01"}},
               %Projection{property: %PropertyReference{name: "some_property_02"}}
             ]
           }
  end

  test "group_by/2" do
    assert DsWrapper.Query.group_by(@query, ["some_property_01", "some_property_02"]) == %Query{
             kind: %KindExpression{name: "SomeKind"},
             distinctOn: [
               %PropertyReference{name: "some_property_01"},
               %PropertyReference{name: "some_property_02"}
             ]
           }
  end

  describe "order/3" do
    test "single" do
      assert DsWrapper.Query.order(@query, "some_property") == %Query{
               kind: %KindExpression{name: "SomeKind"},
               order: [%PropertyOrder{property: %PropertyReference{name: "some_property"}, direction: "ASCENDING"}]
             }
    end

    test "multiple" do
      assert DsWrapper.Query.order(@query, "some_property_01") |> DsWrapper.Query.order("some_property_02", :desc) == %Query{
               kind: %KindExpression{name: "SomeKind"},
               order: [
                 %PropertyOrder{property: %PropertyReference{name: "some_property_01"}, direction: "ASCENDING"},
                 %PropertyOrder{property: %PropertyReference{name: "some_property_02"}, direction: "DESCENDING"}
               ]
             }
    end

    test "direction is :desc" do
      assert DsWrapper.Query.order(@query, "some_property", :desc) == %Query{
               kind: %KindExpression{name: "SomeKind"},
               order: [%PropertyOrder{property: %PropertyReference{name: "some_property"}, direction: "DESCENDING"}]
             }
    end

    test "direction is not :desc" do
      assert DsWrapper.Query.order(@query, "some_property", :asc) == %Query{
               kind: %KindExpression{name: "SomeKind"},
               order: [%PropertyOrder{property: %PropertyReference{name: "some_property"}, direction: "ASCENDING"}]
             }
    end
  end

  test "limit/2" do
    assert DsWrapper.Query.limit(@query, 100) == %Query{
             kind: %KindExpression{name: "SomeKind"},
             limit: 100
           }
  end

  test "start/2" do
    assert DsWrapper.Query.start(@query, "some-cursor") == %Query{
             kind: %KindExpression{name: "SomeKind"},
             startCursor: "some-cursor"
           }
  end

  defp assert_equals_property_filter(query, property_filter) do
    filter = query.filter.compositeFilter.filters |> List.first()
    assert filter.propertyFilter == property_filter
  end
end
