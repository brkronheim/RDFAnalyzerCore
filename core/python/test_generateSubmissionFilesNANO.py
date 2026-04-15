from unittest.mock import MagicMock

from core.python.generateSubmissionFilesNANO import queryRucio


def _make_file_entry(name, states=None, pfns=None, size_bytes=1_000_000):
    return {
        "name": name,
        "states": states or {},
        "pfns": pfns or {},
        "bytes": size_bytes,
    }


def test_query_rucio_returns_structured_groups_and_site_redirectors():
    lfn = "/store/mc/sample.root"
    entry = _make_file_entry(
        lfn,
        states={
            "T2_US_Nebraska_Disk": "AVAILABLE",
            "T3_US_NotreDame_Disk": "AVAILABLE",
            "T1_US_FNAL_Tape": "AVAILABLE",
        },
        pfns={
            "root://ndcms.crc.nd.edu//store/mc/sample.root": {"rse": "T3_US_NotreDame_Disk"},
            "root://xrootd-local.unl.edu//store/mc/sample.root": {"rse": "T2_US_Nebraska_Disk"},
            "root://cms-xrd-global.cern.ch//store/mc/sample.root": {"rse": "CMS_Global"},
        },
    )

    client = MagicMock()
    client.list_replicas.return_value = iter([entry])

    result = queryRucio("/Dummy/Dataset/NANOAODSIM", 10, [], [], "", client)

    assert set(result.keys()) == {"groups", "site_redirectors"}
    assert 0 in result["groups"]
    assert "root://cms-xrd-global.cern.ch//store/mc/sample.root" in result["groups"][0]

    candidates = result["site_redirectors"][lfn]
    assert "root://ndcms.crc.nd.edu//store/mc/sample.root" in candidates
    assert "root://xrootd-local.unl.edu//store/mc/sample.root" in candidates
    assert "root://xrootd-cms.infn.it//store/test/xrootd/T2_US_Nebraska/" in candidates
    assert "root://xrootd-cms.infn.it//store/test/xrootd/T3_US_NotreDame/" in candidates
    assert all("T1_US_FNAL" not in candidate for candidate in candidates)
    assert "root://cms-xrd-global.cern.ch//store/mc/sample.root" not in candidates


def test_query_rucio_keeps_t3_sites_when_available():
    lfn = "/store/mc/t3.root"
    entry = _make_file_entry(
        lfn,
        states={
            "T3_US_NotreDame_Disk": "AVAILABLE",
        },
    )

    client = MagicMock()
    client.list_replicas.return_value = iter([entry])

    result = queryRucio("/Dummy/T3/NANOAODSIM", 10, [], [], "", client)

    candidates = result["site_redirectors"][lfn]
    assert any("T3_US_NotreDame" in candidate for candidate in candidates)


def test_query_rucio_applies_site_override_to_candidates():
    lfn = "/store/mc/override.root"
    entry = _make_file_entry(
        lfn,
        states={
            "T2_US_Nebraska_Disk": "AVAILABLE",
            "T3_US_NotreDame_Disk": "AVAILABLE",
        },
        pfns={
            "root://ndcms.crc.nd.edu//store/mc/override.root": {"rse": "T3_US_NotreDame_Disk"},
            "root://xrootd-local.unl.edu//store/mc/override.root": {"rse": "T2_US_Nebraska_Disk"},
        },
    )

    client = MagicMock()
    client.list_replicas.return_value = iter([entry])

    result = queryRucio("/Dummy/Override/NANOAODSIM", 10, [], [], "T3_US_NotreDame", client)

    candidates = result["site_redirectors"][lfn]
    assert "root://ndcms.crc.nd.edu//store/mc/override.root" in candidates
    assert all("T2_US_Nebraska" not in candidate for candidate in candidates)
    assert any("T3_US_NotreDame" in candidate for candidate in candidates)
