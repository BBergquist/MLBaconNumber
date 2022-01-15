from os.path import exists
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import Row, SparkSession, Window, \
    functions as f, types as t
from graphframes import GraphFrame
import zstandard as zstd


def create_sparksession():
    conf = SparkConf().setAppName("MLBaconNumber") \
        .setMaster("local[*]") \
        .set("spark.jars", "/usr/lib/MLBaconNumber/graphframes-0.8.1-spark3.0-s_2.12.jar")
    context = SparkContext(conf=conf)
    return SparkSession(context)


def get_roster(spark, compressed_roster_path):
    decompressed_roster_path = "/tmp/roster.csv"
    decompress_file(compressed_roster_path, decompressed_roster_path)
    return read_roster_csv(spark, decompressed_roster_path)


def decompress_file(compressed_path, decompressed_path):
    if not exists(decompressed_path):
        decompressor = zstd.ZstdDecompressor()
        with open(compressed_path, "rb") as filein, \
                open(decompressed_path, "wb") as fileout:
            decompression_result = decompressor.copy_stream(
                filein, fileout)
            print("Size (compressed,uncompressed): {}".format(decompression_result))


def read_roster_csv(spark, path):
    schema = t.StructType().add("year", t.IntegerType()) \
        .add("player_id", t.StringType()) \
        .add("last_name", t.StringType()) \
        .add("first_name", t.StringType()) \
        .add("bats", t.StringType()) \
        .add("throws", t.StringType()) \
        .add("team_id", t.StringType()) \
        .add("position", t.StringType())
    return spark.read.csv(path, schema) \
        .drop("bats", "throws", "position") \
        .withColumn("team_id", f.concat("team_id", "year"))


def build_mlb_graph(spark, roster):
    team_vertices = get_team_vertices(roster)
    player_vertices = get_player_vertices(roster)
    first_game_vertex = get_first_game_vertex(spark)

    player_edges = roster.select(f.col("player_id").alias(
        "src"), f.col("team_id").alias("dst"))
    team_edges = roster.select(f.col("team_id").alias(
        "src"), f.col("player_id").alias("dst"))
    first_game_edges = get_first_game_edges(spark)

    vertices = (team_vertices
                .unionAll(player_vertices)
                .unionAll(first_game_vertex))
    edges = team_edges.unionAll(player_edges).unionAll(first_game_edges)
    return GraphFrame(vertices, edges)


def calculate_first_game_shortest_paths(mlb_graph):
    return mlb_graph.shortestPaths(landmarks=["first_game"])


# Create a new graph (a tree) where the only edges are the shortest
# paths between players to the first game
def build_first_game_tree(spark, roster, roster_with_shortest_paths):
    teammate_edges = get_shortest_path_edges(roster_with_shortest_paths)
    w = Window.partitionBy("src").orderBy("team_id")
    reduced_edges = teammate_edges.withColumn("row_num", f.row_number().over(w)) \
                                  .where(f.col("row_num") == 1) \
                                  .drop("row_num")

    vertices = (get_player_vertices(roster)
                .unionAll(get_first_game_vertex(spark)))
    edges = (reduced_edges
             .unionAll(get_first_game_edges(spark)
                       .withColumn("team_id", f.lit("PLAYED_IN"))))
    return GraphFrame(vertices, edges)


def calculate_bacon_numbers(roster, shortest_path_tree):
    return (shortest_path_tree.shortestPaths(landmarks=["first_game"])
            .select(f.col("id").alias("player_id"),
                    (f.col("distances").getItem("first_game") - 1).alias("bacon_number"))
            .drop("distances")
            .join(roster, on="player_id", how="inner")
            .groupBy("player_id", "last_name", "first_name", "bacon_number")
            .agg(f.max("year").alias("max_year")))


def add_bacon_path(bacon_numbers, shortest_path_tree):
    MAX_BACON_NUMBER = 9  # Previously calculated
    df = bacon_numbers.withColumn("v0", f.col("player_id"))
    for i in range(MAX_BACON_NUMBER + 1):
        df = df.join(shortest_path_tree.edges,
                     on=(f.col("v" + str(i)) == f.col("src")), how="leftouter") \
            .withColumn("e" + str(i+1), f.col("team_id")) \
            .withColumn("v" + str(i+1), f.col("dst")) \
            .drop("src", "dst", "team_id")
    return df


def get_team_vertices(roster):
    return roster.select(f.col("team_id").alias("id"),
                         f.col("year").alias("max_year"),
                         f.lit("").alias("last_name"),
                         f.lit("").alias("first_name")).distinct()


def get_player_vertices(roster):
    return roster.groupBy(f.col("player_id").alias("id"),
                          f.col("last_name"), f.col("first_name")) \
        .agg(f.max("year").alias("max_year"))


def get_first_game_vertex(spark):
    return spark.createDataFrame([
        Row(id="first_game", max_year=1871, last_name="", first_name="")
    ])


def get_first_game_edges(spark):
    return spark.createDataFrame([
        ("whitd102", "first_game"),
        ("kimbg101", "first_game"),
        ("paboc101", "first_game"),
        ("allia101", "first_game"),
        ("white104", "first_game"),
        ("prata101", "first_game"),
        ("sutte101", "first_game"),
        ("carlj102", "first_game"),
        ("bassj101", "first_game"),
        ("selmf101", "first_game"),
        ("mathb101", "first_game"),
        ("foraj101", "first_game"),
        ("goldw101", "first_game"),
        ("lennb101", "first_game"),
        ("caret101", "first_game"),
        ("mince101", "first_game"),
        ("mcdej101", "first_game"),
        ("kellb105", "first_game"),
    ], ["src", "dst"])


def add_shortest_paths_to_roster(roster, shortest_paths):
    shortest_paths = shortest_paths.select("id", "distances")
    return (roster.join(shortest_paths, on=shortest_paths.id == roster.team_id,
                        how="leftouter")
            .withColumn("team_shortest_path", f.col("distances").getItem("first_game"))
            .drop("id", "distances")
            .join(shortest_paths, on=shortest_paths.id == roster.player_id, how="leftouter")
            .withColumn("player_shortest_path", f.col("distances").getItem("first_game"))
            .drop("id", "distances"))


def get_shortest_path_edges(roster_shortest_paths):
    left = roster_shortest_paths.alias("left")
    right = roster_shortest_paths.alias("right")
    return (left.join(right, on="team_id", how="inner")
            .where(f.col("left.player_id") != f.col("right.player_id"))
            .where(f.col("left.player_shortest_path")
                   > f.col("right.player_shortest_path"))
            .groupBy(f.col("left.player_id").alias("src"),
                     f.col("right.player_id").alias("dst"))
            .agg(f.min("team_id").alias("team_id")))


# TODO: Add code to take in output path and write regular CSV to that path
# TODO: Get csv file with team codes + names
# TODO: Explore CSV findings in Excel
# TODO: Publish CSVs and findings
compressed_roster = "data/roster.csv.zst"
spark = create_sparksession()
roster = get_roster(spark, compressed_roster)

mlb_graph = build_mlb_graph(spark, roster)
first_game_paths = calculate_first_game_shortest_paths(mlb_graph)

roster_with_shortest_paths = add_shortest_paths_to_roster(
    roster, first_game_paths)
shortest_path_tree = build_first_game_tree(
    spark, roster, roster_with_shortest_paths)
bacon_numbers = calculate_bacon_numbers(roster, shortest_path_tree)
bacon_numbers = add_bacon_path(bacon_numbers, shortest_path_tree)

bacon_numbers.write.csv("/tmp/bacon_roster.csv", mode="overwrite", header=True)
