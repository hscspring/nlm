from dataclasses import dataclass


class Error(Exception):
    pass


@dataclass
class ConfigError(Error):
    code = 10000
    desc = "Config file error."


@dataclass
class InputError(Error):
    code = 20000
    desc = "Input invalid"


@dataclass
class ParameterError(Error):
    code = 20001
    desc = "Parameter type invalid."


@dataclass
class OverstepError(Error):
    code = 40001
    desc = "Your input is overstepped."


@dataclass
class ServerError(Error):
    code = 50000
    desc = "NLM Layer Server internal error."


@dataclass
class DatabaseError(Error):
    code = 50001
    desc = "Database internal error."


@dataclass
class QueryError(Error):
    code = 50002
    desc = "Qeury error, please check your input."

