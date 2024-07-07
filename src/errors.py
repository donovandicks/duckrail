class ErrNoResults(Exception):
    def __init__(self) -> None:
        super().__init__("No rows returned")
