"""Tests for SKG-IF JSON-LD serialization."""

from datetime import UTC, datetime
from types import SimpleNamespace

from app.schemas.skg_if import SKG_IF_CONTEXT, build_json_ld


def _make_obj(**kwargs):
    defaults = {
        "id": 1,
        "pids": [{"type": "doi", "value": "10.1234/test"}],
        "object_type": "dataset",
        "title": "Test Dataset",
        "titles": None,
        "creators": None,
        "keywords": None,
        "topics": None,
        "projects": None,
        "external_ids": [],
        "access": None,
        "methods": None,
        "citation_count": None,
        "fwci": None,
        "origin": "source_endpoint",
        "raw_responses": {},
        "created_at": datetime.now(UTC),
        "last_checked": datetime.now(UTC),
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def _make_rel(source_id: int, target_id: int, relation_type: str):
    return SimpleNamespace(
        id=99,
        source_id=source_id,
        target_id=target_id,
        relation_type=relation_type,
        provenance="scholexplorer",
        created_at=datetime.now(UTC),
    )


class TestBuildJsonLd:
    def test_context_present(self):
        source = _make_obj()
        doc = build_json_ld(source, [])
        assert doc["@context"] == SKG_IF_CONTEXT

    def test_graph_contains_source(self):
        source = _make_obj()
        doc = build_json_ld(source, [])
        assert len(doc["@graph"]) == 1
        assert doc["@graph"][0]["type"] == "dataset"

    def test_graph_contains_related(self):
        source = _make_obj(id=1)
        related_obj = _make_obj(
            id=2,
            pids=[{"type": "doi", "value": "10.5678/pub"}],
            object_type="publication",
            title="Related Publication",
        )
        rel = _make_rel(1, 2, "Cites")
        doc = build_json_ld(source, [(related_obj, rel)])
        assert len(doc["@graph"]) == 2

    def test_relationship_predicate(self):
        source = _make_obj(id=1)
        related_obj = _make_obj(id=2, pids=[{"type": "doi", "value": "10.5678/pub"}])
        rel = _make_rel(1, 2, "Cites")
        doc = build_json_ld(source, [(related_obj, rel)])
        source_node = doc["@graph"][0]
        assert "cites" in source_node
        assert source_node["cites"][0]["@id"] == "https://doi.org/10.5678/pub"

    def test_doi_becomes_uri(self):
        source = _make_obj(pids=[{"type": "doi", "value": "10.1234/test"}])
        doc = build_json_ld(source, [])
        assert doc["@graph"][0]["@id"] == "https://doi.org/10.1234/test"

    def test_identifiers_present(self):
        source = _make_obj()
        doc = build_json_ld(source, [])
        node = doc["@graph"][0]
        assert node["identifiers"][0]["scheme"] == "doi"
        assert node["identifiers"][0]["value"] == "10.1234/test"

    def test_citation_count_included(self):
        source = _make_obj(citation_count=42, fwci=1.5)
        doc = build_json_ld(source, [])
        node = doc["@graph"][0]
        assert node["citationCount"] == 42
        assert node["fwci"] == 1.5
