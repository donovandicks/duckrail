from typing import Literal

import duckdb
from config import (
    DIST_CSV_FILE,
    DIST_CSV_URL,
    DIST_LNG_TBL_NAME,
    DIST_TBL_NAME,
    STAT_CSV_FILE,
    STAT_CSV_URL,
    STAT_TBL_NAME,
    SVCS_BY_MONTH_TBL_NAME,
    SVCS_CSV_FILE,
    SVCS_CSV_URL,
    SVCS_TBL_NAME,
)
from errors import ErrNoResults
from rich.console import Console
from rich.table import Table
from writer import TableMaker

console = Console()


class Analyzer:
    def __init__(self, mode: Literal["remote", "local"] = "remote") -> None:
        self.mode = mode
        self.con = duckdb.connect(":memory:")

        self.services_file = SVCS_CSV_FILE if mode == "local" else SVCS_CSV_URL
        self.stations_file = STAT_CSV_FILE if mode == "local" else STAT_CSV_URL
        self.distances_file = DIST_CSV_FILE if mode == "local" else DIST_CSV_URL

        self.files = {
            "services": self.services_file,
            "stations": self.stations_file,
            "distances": self.distances_file,
        }

        self.subqueries = {
            SVCS_BY_MONTH_TBL_NAME: f"""
            SELECT
                month("Service:Date") AS month,
                "Stop:station name" AS station,
                count(*) AS num_services
            FROM {self.services_file!r}
            GROUP BY ALL
            """,
            DIST_LNG_TBL_NAME: f"""
            UNPIVOT {DIST_TBL_NAME}
            ON COLUMNS (* EXCLUDE station)
            INTO NAME other_station VALUE distance
            """,
            SVCS_TBL_NAME: f"FROM {self.services_file!r}",
            STAT_TBL_NAME: f"FROM {self.stations_file!r}",
            DIST_TBL_NAME: f"FROM read_csv({self.distances_file!r}, nullstr='XXX')",
        }

        if mode == "local":
            self.load_local_data()

    @property
    def is_remote(self) -> bool:
        return self.mode == "remote"

    def create_tbl(self, tbl: str):
        subqry = self.subqueries.get(tbl)
        if not subqry:
            raise Exception(f"No subquery defined for {tbl}")

        self.con.execute(f"""
        CREATE TABLE {tbl} AS ({subqry})
        """)

    def load_local_data(self):
        self.create_tbl(SVCS_TBL_NAME)
        self.create_tbl(SVCS_BY_MONTH_TBL_NAME)
        self.create_tbl(STAT_TBL_NAME)
        self.create_tbl(DIST_TBL_NAME)
        self.create_tbl(DIST_LNG_TBL_NAME)

    def add_cte(self, qry: str, tbl: str) -> str:
        subqry = self.subqueries.get(tbl)
        if not subqry:
            raise Exception(f"No subquery defined for {tbl}")

        return f"""
        WITH {tbl} AS ({subqry})
        {qry}
        """

    def get_tbl_count(self, tbl_name: str) -> str:
        result = self.con.execute(
            f"SELECT format('{{:,}}', count(*)) FROM {tbl_name!r};"
        ).fetchone()

        if not result:
            raise ErrNoResults()

        return result[0]

    def show_count(self):
        for file_name, file in self.files.items():
            result = self.get_tbl_count(file)
            print(f"Total {file_name.capitalize()} Rows: {result}")

    def get_busiest_by_month(self, month_cutoff: int = 6) -> Table:
        qry = f"""
        SELECT
            month,
            arg_max(station, num_services) AS station,
            max(num_services) AS num_services
        FROM {SVCS_BY_MONTH_TBL_NAME}
        WHERE month <= $1
        GROUP BY ALL;
        """

        if self.is_remote:
            qry = self.add_cte(qry, SVCS_BY_MONTH_TBL_NAME)

        result = self.con.execute(qry, [month_cutoff]).fetchall()

        if not result:
            raise ErrNoResults()

        return (
            TableMaker(title="Busiest Station by Month")
            .with_column("month")
            .with_column("station")
            .with_column("num_services")
            .with_rows(result)
            .build()
        )

    def get_top_n_stations_in_period(
        self, n: int = 3, start: int = 1, end: int = 12
    ) -> Table:
        """Get the top `n` stations in the months between `start` and `end`."""
        qry = f"""
        SELECT month, month_name, array_agg(station) AS top_stations
        FROM (
            SELECT
                month,
                strftime(make_date(2023, month, 1), '%B') AS month_name,
                rank() OVER
                    (PARTITION BY month ORDER BY num_services DESC) AS rank,
                station,
                num_services
            FROM {SVCS_BY_MONTH_TBL_NAME}
            WHERE month BETWEEN $1 AND $2
        )
        WHERE rank <= $3
        GROUP BY ALL
        ORDER BY month;
        """

        if self.is_remote:
            qry = self.add_cte(qry, SVCS_BY_MONTH_TBL_NAME)

        result = self.con.execute(qry, [start, end, n]).fetchall()

        if not result:
            raise ErrNoResults()

        return (
            TableMaker(title=f"Top {n} Stations in Months {start}-{end}")
            .with_column("month")
            .with_column("month_name")
            .with_column("top_stations")
            .with_rows(result)
            .build()
        )

    def get_stations(self, limit: int = 5) -> Table:
        qry = f"""
        SELECT
            id,
            name_short,
            name_long,
            country,
            printf('%.2f', geo_lat) AS latitude,
            printf('%.2f', geo_lng) AS longitude
        FROM {STAT_TBL_NAME}
        LIMIT $1
        """

        if self.is_remote:
            qry = self.add_cte(qry, STAT_TBL_NAME)

        result = self.con.execute(qry, [limit]).fetchall()

        if not result:
            raise ErrNoResults()

        return (
            TableMaker(title="Stations")
            .with_column("id")
            .with_column("name_short")
            .with_column("name_long")
            .with_column("country")
            .with_column("latitude")
            .with_column("longitude")
            .with_rows(result)
            .build()
        )

    def get_distances(self, limit: int = 5) -> Table:
        qry = f"""
        SELECT station, other_station, distance
        FROM {DIST_LNG_TBL_NAME}
        LIMIT $1
        """

        if self.is_remote:
            qry = self.add_cte(qry, DIST_LNG_TBL_NAME)

        result = self.con.execute(qry, [limit]).fetchall()
        if not result:
            raise ErrNoResults()

        return (
            TableMaker(title="Station Pair Distances")
            .with_column("station")
            .with_column("other_station")
            .with_column("distance")
            .with_rows(result)
            .build()
        )

    def get_station_pairs(
        self, order: Literal["ASC", "DESC"] = "DESC", limit: int = 3
    ) -> Table:
        qry = f"""
        SELECT
            s1.name_long AS station1,
            s2.name_long AS station2,
            {DIST_LNG_TBL_NAME}.distance
        FROM {DIST_LNG_TBL_NAME}
        JOIN
            {STAT_TBL_NAME} s1 ON {DIST_LNG_TBL_NAME}.station = s1.code
        JOIN
            {STAT_TBL_NAME} s2 ON {DIST_LNG_TBL_NAME}.other_station = s2.code
        WHERE
            s1.country = 'NL'
            AND s2.country = 'NL'
            AND station < other_station
        ORDER BY distance {order}
        LIMIT $1
        """

        result = self.con.execute(qry, [limit]).fetchall()
        if not result:
            raise ErrNoResults()

        return (
            TableMaker(
                title=f"{'Furthest' if order == 'DESC' else 'Shortest'} Station Distances"
            )
            .with_column("station1")
            .with_column("station2")
            .with_column("distance")
            .with_rows(result)
            .build()
        )

    def analyze(self):
        self.show_count()

        console.print(self.get_stations())
        console.print(self.get_distances())
        console.print(self.get_busiest_by_month())
        console.print(self.get_top_n_stations_in_period(n=2, start=6, end=8))
        console.print(self.get_station_pairs())


if __name__ == "__main__":
    Analyzer(mode="local").analyze()
