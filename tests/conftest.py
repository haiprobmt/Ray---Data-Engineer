import pytest
from ray_de.config import Project, ProjectConfig
from ray_de.state import StateStore

WS = "11111111-1111-1111-1111-111111111111"
ITEM = "22222222-2222-2222-2222-222222222222"


@pytest.fixture
def project(tmp_path):
    directory = tmp_path / "project-a"
    repo = directory / "repo"
    repo.mkdir(parents=True)
    (repo / "main.py").write_text("answer = 1\n")
    (directory / "CONTEXT.md").write_text("Project A private conventions")
    config = ProjectConfig.model_validate(
        {
            "project_id": "project-a",
            "name": "A",
            "repo_path": "repo",
            "policy": {"local_write": True},
            "fabric": {"workspaces": [{"id": WS, "environment": "DEV"}]},
            "validation_commands": [["{python}", "-c", "pass"]],
        }
    )
    return Project(config, directory / "config.yaml")


@pytest.fixture
def store(tmp_path, project):
    store = StateStore(tmp_path / "state" / "ray.db")
    store.bind(project)
    return store
