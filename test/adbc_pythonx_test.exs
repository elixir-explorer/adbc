defmodule Adbc.PythonxTest do
  use ExUnit.Case, async: true
  alias Adbc.Result

  defp eval!(code) do
    {py_table, %{}} = Pythonx.eval(code, %{})
    {:ok, result} = Result.from_py(py_table)
    result
  end

  describe "from_py" do
    test "with invalid object" do
      assert_raise ArgumentError, fn ->
        {py_list, %{}} = Pythonx.eval("[]", %{})
        Result.from_py!(py_list)
      end
    end

    test "with simple columns" do
      result =
        eval!("""
        import pyarrow
        n_legs = pyarrow.array([2, 4, 5, 100])
        animals = pyarrow.array(["Flamingo", "Horse", "Brittle stars", "Centipede"])
        names = ["n_legs", "animals"]
        pyarrow.Table.from_arrays([n_legs, animals], names=names)
        """)

      assert %{
               "n_legs" => [2, 4, 5, 100],
               "animals" => ["Flamingo", "Horse", "Brittle stars", "Centipede"]
             } == Result.to_map(result)
    end
  end

  describe "null" do
    for count <- [0, 1, 3] do
      test "materializes #{count} null values" do
        result =
          eval!("""
          import pyarrow
          batch = pyarrow.record_batch([pyarrow.nulls(#{unquote(count)})], names=["nulls"])
          pyarrow.RecordBatchReader.from_batches(batch.schema, [batch])
          """)

        assert [[%Adbc.Column{field: %Adbc.Field{type: nil}} = column]] = result.data
        assert Adbc.Column.to_list(column) == List.duplicate(nil, unquote(count))
        refute Adbc.Column.has_validity?(column)
        assert Result.to_map(result) == %{"nulls" => List.duplicate(nil, unquote(count))}
      end
    end

    test "materializes sliced null arrays without counting the offset" do
      result =
        eval!("""
        import pyarrow
        data = pyarrow.nulls(8).slice(5, 3)
        assert data.offset == 5
        pyarrow.Table.from_arrays([data], names=["nulls"])
        """)

      assert Result.to_map(result) == %{"nulls" => [nil, nil, nil]}
    end

    test "materializes nested null fields and preserves struct validity" do
      result =
        eval!("""
        import pyarrow
        inner_type = pyarrow.struct([("value", pyarrow.null())])
        data = pyarrow.array([
          {"inner": {"value": None}},
          None,
          {"inner": None},
          {"inner": {"value": None}}
        ], type=pyarrow.struct([("inner", inner_type)]))
        pyarrow.Table.from_arrays([data], names=["structs"])
        """)

      assert Result.to_map(result) == %{
               "structs" => [
                 %{"inner" => %{"value" => nil}},
                 nil,
                 %{"inner" => nil},
                 %{"inner" => %{"value" => nil}}
               ]
             }
    end

    test "materializes lists of null values" do
      result =
        eval!("""
        import pyarrow
        data = pyarrow.array([[None, None], [], None, [None]], type=pyarrow.list_(pyarrow.null()))
        pyarrow.Table.from_arrays([data], names=["lists"])
        """)

      assert Result.to_map(result) == %{"lists" => [[nil, nil], [], nil, [nil]]}
    end

    for offset <- [-1, 2] do
      test "rejects out-of-range null child offset #{offset}" do
        {data, %{}} =
          Pythonx.eval(
            """
            import pyarrow
            data = pyarrow.UnionArray.from_dense(
              pyarrow.array([0], type=pyarrow.int8()),
              pyarrow.array([#{unquote(offset)}], type=pyarrow.int32()),
              [pyarrow.nulls(1)], field_names=["nulls"])
            pyarrow.Table.from_arrays([data], names=["unions"])
            """,
            %{}
          )

        assert {:error, error} = Result.from_py(data)
        assert Exception.message(error) =~ "invalid offset/count for ArrowArray (null)"
      end
    end

    for count <- [0, 1, 3] do
      test "round-trips #{count} null values and nested children through parameter binding" do
        result =
          eval!("""
          import pyarrow
          nulls = pyarrow.nulls(#{unquote(count)})
          structs = pyarrow.array([{"value": None}] * #{unquote(count)}, type=pyarrow.struct([("value", pyarrow.null())]))
          lists = pyarrow.array([[None, None]] * #{unquote(count)}, type=pyarrow.list_(pyarrow.null()))
          batch = pyarrow.record_batch([nulls, structs, lists], names=["nulls", "structs", "lists"])
          pyarrow.RecordBatchReader.from_batches(batch.schema, [batch])
          """)

        db = start_supervised!({Adbc.Database, driver: :duckdb})
        conn = start_supervised!({Adbc.Connection, database: db})
        assert [columns] = result.data
        assert {:ok, _} = Adbc.Connection.bulk_insert(conn, columns, table: "nulls")
        map = conn |> Adbc.Connection.query!("SELECT * FROM nulls") |> Result.to_map()
        assert Map.get(map, "nulls", []) == List.duplicate(nil, unquote(count))
        assert Map.get(map, "structs", []) == List.duplicate(%{"value" => nil}, unquote(count))
        assert Map.get(map, "lists", []) == List.duplicate([nil, nil], unquote(count))

        if unquote(count) == 3 do
          [column | _] = columns

          assert {:error, error} =
                   Adbc.Connection.bulk_insert(conn, [%{column | data: -1}],
                     table: "invalid_nulls"
                   )

          assert Exception.message(error) =~ "invalid count for ArrowArray (null)"
        end
      end
    end

    test "round-trips a raw null IPC stream with Python and preserves field metadata" do
      {ipc, %{}} =
        Pythonx.eval(
          """
          import pyarrow
          schema = pyarrow.schema([pyarrow.field("nulls", pyarrow.null(), metadata={b"key": b"value"})])
          batch = pyarrow.record_batch([pyarrow.nulls(3)], schema=schema)
          sink = pyarrow.BufferOutputStream()
          with pyarrow.ipc.new_stream(sink, schema) as writer:
              writer.write_batch(batch)
          sink.getvalue().to_pybytes()
          """,
          %{}
        )

      ipc = Pythonx.decode(ipc)
      stream = Adbc.StreamResult.from_ipc_stream!(ipc)
      encoded = Adbc.StreamResult.to_ipc_stream(stream)
      result = Result.from_ipc_stream!(encoded)
      assert [[column]] = result.data
      assert column.field == %Adbc.Field{name: "nulls", type: nil, metadata: %{"key" => "value"}}
      assert Adbc.Column.to_list(column) == [nil, nil, nil]

      {valid, %{}} =
        Pythonx.eval(
          """
          import pyarrow
          table = pyarrow.ipc.open_stream(encoded).read_all()
          table.to_pydict() == {"nulls": [None, None, None]} and table.schema.field(0).nullable and table.schema.field(0).metadata == {b"key": b"value"}
          """,
          %{"encoded" => encoded}
        )

      assert Pythonx.decode(valid)
    end
  end

  describe "string_view and binary_view" do
    test "materializes utf8_view and binary_view columns" do
      result =
        eval!("""
        import pyarrow
        strings = pyarrow.array(["hi", None, "Brittle stars", "Centipede"], type=pyarrow.string_view())
        blobs = pyarrow.array([b"ab", b"cd", None, b"a longer blob!!"], type=pyarrow.binary_view())
        pyarrow.Table.from_arrays([strings, blobs], names=["strings", "blobs"])
        """)

      assert %Adbc.Result{
               data: [
                 [
                   %Adbc.Column{field: %{name: "strings", type: :string_view}},
                   %Adbc.Column{field: %{name: "blobs", type: :binary_view}}
                 ]
               ]
             } = result

      assert %{
               "strings" => ["hi", nil, "Brittle stars", "Centipede"],
               "blobs" => ["ab", "cd", nil, "a longer blob!!"]
             } == Result.to_map(result)
    end
  end

  describe "fixed_size_binary" do
    test "materializes fixed_size_binary column" do
      result =
        eval!("""
        import pyarrow
        data = pyarrow.array([b"ab", b"cd", None, b"ef"], type=pyarrow.binary(2))
        pyarrow.Table.from_arrays([data], names=["fsb"])
        """)

      assert %Adbc.Result{
               data: [
                 [
                   %Adbc.Column{
                     field: %Adbc.Field{name: "fsb", type: {:fixed_size_binary, 2}},
                     data: %Adbc.BufferData{}
                   } = col
                 ]
               ]
             } = result

      assert Adbc.Column.to_list(col) == ["ab", "cd", nil, "ef"]
    end
  end

  describe "list" do
    test "materializes list column" do
      result =
        eval!("""
        import pyarrow
        data = pyarrow.array([[1, 2, 3], [4, 5], None, [6]], type=pyarrow.list_(pyarrow.int32()))
        pyarrow.Table.from_arrays([data], names=["lists"])
        """)

      assert %Adbc.Result{
               data: [
                 [
                   %Adbc.Column{
                     field: %Adbc.Field{
                       name: "lists",
                       type: {:list, %Adbc.Field{name: "item", type: :s32}}
                     }
                   } = col
                 ]
               ]
             } = result

      assert Adbc.Column.to_list(col) == [[1, 2, 3], [4, 5], nil, [6]]
    end

    test "materializes large list column" do
      result =
        eval!("""
        import pyarrow
        data = pyarrow.array([[1, 2], None, [3, 4, 5]], type=pyarrow.large_list(pyarrow.int32()))
        pyarrow.Table.from_arrays([data], names=["lists"])
        """)

      assert %Adbc.Result{
               data: [
                 [
                   %Adbc.Column{
                     field: %Adbc.Field{
                       name: "lists",
                       type: {:large_list, %Adbc.Field{name: "item", type: :s32}}
                     }
                   } = col
                 ]
               ]
             } = result

      assert Adbc.Column.to_list(col) == [[1, 2], nil, [3, 4, 5]]
    end

    test "materializes fixed size list column" do
      result =
        eval!("""
        import pyarrow
        data = pyarrow.array([[1, 2, 3], [4, 5, 6], None, [7, 8, 9]], type=pyarrow.list_(pyarrow.int32(), 3))
        pyarrow.Table.from_arrays([data], names=["lists"])
        """)

      assert %Adbc.Result{
               data: [
                 [
                   %Adbc.Column{
                     field: %Adbc.Field{
                       name: "lists",
                       type: {:fixed_size_list, %Adbc.Field{name: "item", type: :s32}, 3}
                     }
                   } = col
                 ]
               ]
             } = result

      assert Adbc.Column.to_list(col) == [[1, 2, 3], [4, 5, 6], nil, [7, 8, 9]]
    end
  end

  describe "dictionary" do
    test "materializes dictionary column" do
      result =
        eval!("""
        import pyarrow
        indices = pyarrow.array([0, 1, 0, 2, 1])
        dictionary = pyarrow.array(["foo", "bar", "baz"])
        data = pyarrow.DictionaryArray.from_arrays(indices, dictionary)
        pyarrow.Table.from_arrays([data], names=["dict"])
        """)

      assert %Adbc.Result{
               data: [
                 [
                   %Adbc.Column{
                     field: %Adbc.Field{
                       name: "dict",
                       type:
                         {:dictionary, %Adbc.Field{type: key_type}, %Adbc.Field{type: :string}}
                     }
                   }
                 ]
               ]
             } = result

      assert key_type in [:s8, :s16, :s32, :s64]
      assert Adbc.Column.to_list(hd(hd(result.data))) == ["foo", "bar", "foo", "baz", "bar"]
    end

    test "materializes dictionary with nulls" do
      result =
        eval!("""
        import pyarrow
        indices = pyarrow.array([0, None, 1, 2, None])
        dictionary = pyarrow.array(["foo", "bar", "baz"])
        data = pyarrow.DictionaryArray.from_arrays(indices, dictionary)
        pyarrow.Table.from_arrays([data], names=["dict"])
        """)

      assert Adbc.Column.to_list(hd(hd(result.data))) == ["foo", nil, "bar", "baz", nil]
    end

    test "materializes dictionary with struct values" do
      result =
        eval!("""
        import pyarrow
        struct_type = pyarrow.struct([
          pyarrow.field("x", pyarrow.int32()),
          pyarrow.field("y", pyarrow.utf8())
        ])
        dictionary = pyarrow.array([
          {"x": 1, "y": "a"},
          {"x": 2, "y": "b"},
          {"x": 3, "y": "c"}
        ], type=struct_type)
        indices = pyarrow.array([0, 2, 1, 0])
        data = pyarrow.DictionaryArray.from_arrays(indices, dictionary)
        pyarrow.Table.from_arrays([data], names=["dict"])
        """)

      assert Adbc.Column.to_list(hd(hd(result.data))) == [
               %{"x" => 1, "y" => "a"},
               %{"x" => 3, "y" => "c"},
               %{"x" => 2, "y" => "b"},
               %{"x" => 1, "y" => "a"}
             ]
    end
  end

  describe "struct" do
    test "materializes struct column" do
      result =
        eval!("""
        import pyarrow
        data = pyarrow.array(
          [{"x": 1, "y": "a"}, {"x": 2, "y": "b"}, None],
          type=pyarrow.struct([("x", pyarrow.int32()), ("y", pyarrow.string())])
        )
        pyarrow.Table.from_arrays([data], names=["structs"])
        """)

      assert %Adbc.Result{
               data: [
                 [
                   %Adbc.Column{
                     field: %Adbc.Field{
                       name: "structs",
                       type:
                         {:struct,
                          [
                            %Adbc.Field{name: "x", type: :s32},
                            %Adbc.Field{name: "y", type: :string}
                          ]}
                     }
                   }
                 ]
               ]
             } = result

      assert Adbc.Column.to_list(hd(hd(result.data))) == [
               %{"x" => 1, "y" => "a"},
               %{"x" => 2, "y" => "b"},
               nil
             ]
    end
  end

  describe "run_end_encoded" do
    test "materializes run-end encoded column" do
      result =
        eval!("""
        import pyarrow
        data = pyarrow.RunEndEncodedArray.from_arrays(
          pyarrow.array([3, 5, 7], type=pyarrow.int32()),
          pyarrow.array(["a", "b", "c"])
        )
        pyarrow.Table.from_arrays([data], names=["ree"])
        """)

      assert %Adbc.Result{
               data: [
                 [
                   %Adbc.Column{
                     field: %Adbc.Field{
                       name: "ree",
                       type:
                         {:run_end_encoded, %Adbc.Field{name: "run_ends", type: :s32},
                          %Adbc.Field{name: "values", type: :string}}
                     },
                     data: %Adbc.RunEndEncodedData{
                       offset: 0,
                       length: 7,
                       values: _,
                       run_ends: %Adbc.BufferData{}
                     }
                   }
                 ]
               ]
             } = result

      assert Adbc.Column.to_list(hd(hd(result.data))) == ["a", "a", "a", "b", "b", "c", "c"]
    end

    test "materializes run-end encoded column with dictionary values" do
      result =
        eval!("""
        import pyarrow
        dict_type = pyarrow.dictionary(pyarrow.int32(), pyarrow.utf8())
        data = pyarrow.RunEndEncodedArray.from_arrays(
          pyarrow.array([3, 5, 7], type=pyarrow.int32()),
          pyarrow.array(["foo", "bar", "baz"], type=dict_type)
        )
        pyarrow.Table.from_arrays([data], names=["ree"])
        """)

      assert Adbc.Column.to_list(hd(hd(result.data))) ==
               ["foo", "foo", "foo", "bar", "bar", "baz", "baz"]
    end
  end

  describe "map" do
    test "materializes map column" do
      result =
        eval!("""
        import pyarrow
        data = pyarrow.array(
          [[("x", 1), ("y", 2)], [("z", 3)], None, [("a", 4), ("b", 5)]],
          type=pyarrow.map_(pyarrow.string(), pyarrow.int32())
        )
        pyarrow.Table.from_arrays([data], names=["maps"])
        """)

      assert %Adbc.Result{
               data: [
                 [
                   %Adbc.Column{
                     field: %Adbc.Field{
                       name: "maps",
                       type:
                         {:map, %Adbc.Field{name: "key", type: :string},
                          %Adbc.Field{name: "value", type: :s32}}
                     },
                     data: %Adbc.MapData{}
                   } = col
                 ]
               ]
             } = result

      assert Adbc.Column.to_list(col) == [
               %{"x" => 1, "y" => 2},
               %{"z" => 3},
               nil,
               %{"a" => 4, "b" => 5}
             ]
    end

    test "materializes map column with integer keys" do
      result =
        eval!("""
        import pyarrow
        data = pyarrow.array(
          [[(1, "a"), (2, "b")], [(3, "c")], None],
          type=pyarrow.map_(pyarrow.int32(), pyarrow.string())
        )
        pyarrow.Table.from_arrays([data], names=["maps"])
        """)

      assert Adbc.Column.to_list(hd(hd(result.data))) == [
               %{1 => "a", 2 => "b"},
               %{3 => "c"},
               nil
             ]
    end

    test "materializes map column with nulls in values" do
      result =
        eval!("""
        import pyarrow
        data = pyarrow.array(
          [[("x", 1), ("y", None)], None, [("z", 3)]],
          type=pyarrow.map_(pyarrow.string(), pyarrow.int32())
        )
        pyarrow.Table.from_arrays([data], names=["maps"])
        """)

      assert Adbc.Column.to_list(hd(hd(result.data))) == [
               %{"x" => 1, "y" => nil},
               nil,
               %{"z" => 3}
             ]
    end
  end

  describe "list_view" do
    test "materializes list_view column" do
      result =
        eval!("""
        import pyarrow
        values = pyarrow.array([10, 20, 30, 40, 50], type=pyarrow.int32())
        offsets = pyarrow.array([0, 2, 1], type=pyarrow.int32())
        sizes = pyarrow.array([2, 3, 2], type=pyarrow.int32())
        lv_type = pyarrow.list_view(pyarrow.field("item", pyarrow.int32(), nullable=False))
        data = pyarrow.ListViewArray.from_arrays(offsets, sizes, values, type=lv_type)
        pyarrow.Table.from_arrays([data], names=["lv"])
        """)

      assert %Adbc.Result{
               data: [
                 [
                   %Adbc.Column{
                     field: %Adbc.Field{
                       name: "lv",
                       type: {:list_view, %Adbc.Field{name: "item", type: :s32}}
                     },
                     data: %Adbc.ListViewData{
                       values: %Adbc.BufferData{},
                       offsets: _,
                       sizes: _,
                       validity: _,
                       bit_offset: _
                     }
                   }
                 ]
               ]
             } = result

      assert Adbc.Column.to_list(hd(hd(result.data))) == [[10, 20], [30, 40, 50], [20, 30]]
    end

    test "materializes list_view column with dictionary inner type" do
      result =
        eval!("""
        import pyarrow
        dict_type = pyarrow.dictionary(pyarrow.int32(), pyarrow.utf8())
        values = pyarrow.array(["foo", "bar", "baz", "foo", "bar"], type=dict_type)
        offsets = pyarrow.array([0, 2, 1], type=pyarrow.int32())
        sizes = pyarrow.array([2, 3, 2], type=pyarrow.int32())
        lv_type = pyarrow.list_view(pyarrow.field("item", dict_type, nullable=False))
        data = pyarrow.ListViewArray.from_arrays(offsets, sizes, values, type=lv_type)
        pyarrow.Table.from_arrays([data], names=["lv"])
        """)

      assert Adbc.Column.to_list(hd(hd(result.data))) ==
               [["foo", "bar"], ["baz", "foo", "bar"], ["bar", "baz"]]
    end
  end
end
