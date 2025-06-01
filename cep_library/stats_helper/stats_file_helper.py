from pathlib import Path
from typing import List
from fastapi import FastAPI
from fastapi.responses import FileResponse

from cep_library import configs


class StatsFileHelper:
    def __init__(
        self,
        app: FastAPI,
        header_arr: list[str],
        endpoint_prefix: str,
        endpoint_name: str,
    ) -> None:
        self.app: FastAPI = app
        self.endpoint_prefix: str = endpoint_prefix
        self.endpoint_name: str = endpoint_name
        self.header_arr: List[str] = header_arr

        Path(self.endpoint_prefix).mkdir(parents=True, exist_ok=True)
        self.record_id = 1
        self.prepareStatisticsFiles()
        self.register_stat_endpoints()

    def prepareStatisticsFiles(self) -> None:
        self.file_name = f"{configs.env_host_name}_{self.endpoint_name}.csv"
        self.stats_file = f"{self.endpoint_prefix}/{self.file_name}"
        with open(self.stats_file, "w") as outfile:
            outfile.write(",".join(self.header_arr) + "\n")
        outfile.close()

    def print_device_stats(self, data_arr: List[List[str]]):
        with open(self.stats_file, "w") as outfile:
            outfile.write(",".join(self.header_arr) + "\n")
            for d_a in data_arr:
                d_a.insert(0, str(self.record_id))
                outfile.write(",".join(d_a) + "\n")
        outfile.close()
        self.record_id += 1

    def register_stat_endpoints(self):
        @self.app.get(f"/fetch_stats/{self.endpoint_prefix}/{self.endpoint_name}")
        def _():
            print(
                f"{configs.env_host_name}...requested to send the file: {self.stats_file}....{self.file_name}"
            )
            return FileResponse(path=self.stats_file, filename=self.file_name)
