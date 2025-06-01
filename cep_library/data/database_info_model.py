from cep_library.cep.model.cep_task import MongoQuery


class DataCarryModel:
    source_host: str
    target_host: str
    query: MongoQuery
    remove_source: bool
    collection: str


class DatabaseInfoModel:
    def __init__(self, name: str, host: str, port: str) -> None:
        self.name: str = name
        self.host: str = host
        self.port: str = port
