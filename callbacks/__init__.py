from typing import Callable
from .ceu_cap import ceu_dimension
from .interno_penalty import interno_penalties  # nomes iguais ao ficheiro

Callback = Callable[..., None]

__all__: list[str] = ["ceu_dimension", "interno_penalties", "Callback"]
