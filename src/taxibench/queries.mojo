"""The eight benchmark queries, spelled in iceberg.mojo's filter DSL.

These are the same eight the Python side runs; only the spelling of the
predicate differs, because PyIceberg parses a SQL-ish string and iceberg.mojo
parses a JSON S-expression. Keeping the pair in step is not left to trust: the
harness diffs the two implementations' answers, so a predicate that drifted
would show up as a mismatched count rather than as a quietly faster query.

`select()` names only the columns a query needs. Iceberg resolves projection by
field id, so this is what stops each query paying for all nineteen.
"""

comptime K_COUNT = 0
"""Count the rows that survive the filter."""
comptime K_SUM = 1
"""Sum one double column over the surviving rows."""
comptime K_RATIO = 2
"""Sum `tip_amount` and `fare_amount`, and report the ratio."""
comptime K_ZONE_COUNT = 3
"""Count rows per pickup zone; report the top ten."""
comptime K_ZONE_SUM = 4
"""Sum `total_amount` per pickup zone; report the top ten."""


struct Query(Copyable, Movable):
    """One benchmark query: what to scan, and how to fold it."""

    var name: String
    var title: String
    var filter: String
    var columns: List[String]
    var kind: Int
    var sum_column: String
    var notes: String

    def __init__(
        out self,
        var name: String,
        var title: String,
        var filter: String,
        var columns: List[String],
        kind: Int,
        var sum_column: String,
        var notes: String,
    ):
        self.name = name^
        self.title = title^
        self.filter = filter^
        self.columns = columns^
        self.kind = kind
        self.sum_column = sum_column^
        self.notes = notes^

    def __init__(out self, *, copy: Self):
        self.name = copy.name.copy()
        self.title = copy.title.copy()
        self.filter = copy.filter.copy()
        self.columns = copy.columns.copy()
        self.kind = copy.kind
        self.sum_column = copy.sum_column.copy()
        self.notes = copy.notes.copy()

    def __init__(out self, *, deinit move: Self):
        self.name = move.name^
        self.title = move.title^
        self.filter = move.filter^
        self.columns = move.columns^
        self.kind = move.kind
        self.sum_column = move.sum_column^
        self.notes = move.notes^


def all_queries() -> List[Query]:
    """The suite, in the order the harness runs it."""
    var out = List[Query]()

    out.append(
        Query(
            String("q1_scan_count"),
            String("rows with a positive trip distance"),
            String('[">","trip_distance",0.0]'),
            [String("trip_distance")],
            K_COUNT,
            String(),
            String("full scan, one column; no partition pruning is possible"),
        )
    )
    out.append(
        Query(
            String("q2_month_range"),
            String("fares over one quarter"),
            String(
                '["and",[">=","tpep_pickup_datetime","2024-03-01T00:00:00"],'
                '["<","tpep_pickup_datetime","2024-06-01T00:00:00"]]'
            ),
            [String("total_amount")],
            K_SUM,
            String("total_amount"),
            String("partition pruning: 3 of 24 partitions survive"),
        )
    )
    out.append(
        Query(
            String("q3_payment_sum"),
            String("cash fares across the whole table"),
            String('["=","payment_type",2]'),
            [String("payment_type"), String("total_amount")],
            K_SUM,
            String("total_amount"),
            String("full scan, two columns; the predicate cuts about a third"),
        )
    )
    out.append(
        Query(
            String("q4_tip_ratio"),
            String("tip ratio on long trips"),
            String(
                '["and",[">","trip_distance",10.0],[">","fare_amount",0.0]]'
            ),
            [
                String("trip_distance"),
                String("fare_amount"),
                String("tip_amount"),
            ],
            K_RATIO,
            String(),
            String("full scan, three columns, two predicates"),
        )
    )
    out.append(
        Query(
            String("q5_top_zones"),
            String("busiest pickup zones"),
            String('[">","trip_distance",0.0]'),
            [String("PULocationID"), String("trip_distance")],
            K_ZONE_COUNT,
            String(),
            String("full scan with a dense group-by over 266 zones"),
        )
    )
    out.append(
        Query(
            String("q6_zone_revenue"),
            String("revenue by pickup zone, 2024"),
            String(
                '["and",[">=","tpep_pickup_datetime","2024-01-01T00:00:00"],'
                '["<","tpep_pickup_datetime","2025-01-01T00:00:00"]]'
            ),
            [String("PULocationID"), String("total_amount")],
            K_ZONE_SUM,
            String(),
            String("pruning to 12 partitions, then a grouped sum"),
        )
    )
    out.append(
        Query(
            String("q7_wide"),
            String("every column of one month"),
            String(
                '["and",[">=","tpep_pickup_datetime","2024-06-01T00:00:00"],'
                '["<","tpep_pickup_datetime","2024-07-01T00:00:00"]]'
            ),
            List[String](),  # empty selection = every column
            K_COUNT,
            String(),
            String(
                "one partition, all 19 columns: wide decode rather than"
                " filtering"
            ),
        )
    )
    out.append(
        Query(
            String("q8_selective"),
            String("long cash trips from one zone"),
            String(
                '["and",["=","PULocationID",132],["=","payment_type",1],'
                '[">","trip_distance",20.0]]'
            ),
            [
                String("PULocationID"),
                String("payment_type"),
                String("trip_distance"),
                String("total_amount"),
            ],
            K_SUM,
            String("total_amount"),
            String(
                "highly selective; row-group and page statistics should do the"
                " work"
            ),
        )
    )
    return out^
