from pathlib import Path
import yaml


def load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def ensure_dir(path: str | Path) -> Path:
    path_obj = Path(path)
    path_obj.mkdir(parents=True, exist_ok=True)
    return path_obj


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def table_path_from_config(config: dict, table_key: str) -> Path:
    path = project_root() / "data" / config["tables"][table_key]
    ensure_dir(path.parent)
    return path


def landing_dataset_path(config: dict, dataset_name: str) -> Path:
    return project_root() / config["paths"]["landing_dir"] / dataset_name