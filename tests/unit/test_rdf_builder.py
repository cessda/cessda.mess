"""Tests for RDF graph builder."""

from datetime import UTC, datetime
from types import SimpleNamespace

from rdflib import RDF, URIRef

from scripts.export_triples import SKG, build_graph


def _make_obj(id_, pids, object_type="dataset", title="Test", origin="source_endpoint"):
    return SimpleNamespace(
        id=id_,
        pids=pids,
        object_type=object_type,
        title=title,
        titles=None,
        creators=None,
        citation_count=None,
        fwci=None,
        external_ids=[],
        origin=origin,
        created_at=datetime.now(UTC),
        last_checked=datetime.now(UTC),
    )


def _make_rel(source_id, target_id, relation_type="Cites", provenance="scholexplorer"):
    return SimpleNamespace(
        id=1,
        source_id=source_id,
        target_id=target_id,
        relation_type=relation_type,
        provenance=provenance,
        created_at=datetime.now(UTC),
    )


class TestBuildGraph:
    def test_builds_without_error(self):
        objs = [_make_obj(1, [{"type": "doi", "value": "10.1234/x"}])]
        graph = build_graph(objs, [])
        assert len(graph) > 0

    def test_research_product_type(self):
        objs = [_make_obj(1, [{"type": "doi", "value": "10.1234/x"}])]
        graph = build_graph(objs, [])
        uri = URIRef("https://doi.org/10.1234/x")
        types = set(graph.objects(uri, RDF.type))
        assert SKG.ResearchProduct in types

    def test_relationship_triple(self):
        objs = [
            _make_obj(1, [{"type": "doi", "value": "10.1234/src"}]),
            _make_obj(2, [{"type": "doi", "value": "10.1234/tgt"}], object_type="publication"),
        ]
        rels = [_make_rel(1, 2, "Cites")]
        graph = build_graph(objs, rels)
        src = URIRef("https://doi.org/10.1234/src")
        tgt = URIRef("https://doi.org/10.1234/tgt")
        assert (src, SKG.cites, tgt) in graph

    def test_empty_input(self):
        graph = build_graph([], [])
        assert len(graph) == 0

    def test_handle_pid_fallback_uri(self):
        objs = [_make_obj(1, [{"type": "handle", "value": "20.500/12345"}])]
        graph = build_graph(objs, [])
        uri = URIRef("urn:handle:20.500/12345")
        assert (uri, RDF.type, SKG.ResearchProduct) in graph
