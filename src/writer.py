from typing import Self

from rich.table import Table


class TableMaker:
    def __init__(self, title: str) -> None:
        self.table = Table(title=title)

        self.cols = []
        self.rows = []

    def with_column(self, name: str) -> Self:
        self.cols.append((name,))
        return self

    def with_rows(self, rows: list[tuple]) -> Self:
        for row in rows:
            if len(row) != len(self.cols):
                raise Exception(
                    f"Mismatched column count: got {len(row)}, expected {len(self.cols)}"
                )

            self.rows.append((str(data) for data in row))

        return self

    def build(self) -> Table:
        for col in self.cols:
            self.table.add_column(*col)

        for row in self.rows:
            self.table.add_row(*row)

        return self.table
