from fastapi import FastAPI
from cep_library.cep.cep_management_service import CEPManagementService
import cep_library.configs as configs
from cep_library.data.database_management_service import DatabaseManagementService


class RegisterCEPClient:
    def __init__(self, app:FastAPI, db:DatabaseManagementService) -> None:
        self.client_management = CEPManagementService(configs.env_host_name, app, db=db)
