from pathlib import Path
from typing import Any

import yaml


def load_yaml(path: str | Path) -> dict[str, Any]:
    """
    Lê um arquivo YAML e retorna um dicionário Python.

    Parameters
    ----------
    path : str | Path
        Caminho do arquivo YAML.

    Returns
    -------
    dict[str, Any]
        Conteúdo do YAML como dicionário.
    """

    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(f"Arquivo de configuração não encontrado: {path}")

    with path.open("r", encoding="utf-8") as file:
        content = yaml.safe_load(file)

    if content is None:
        return {}

    return content