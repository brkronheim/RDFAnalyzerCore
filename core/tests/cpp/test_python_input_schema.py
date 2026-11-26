"""Real ROOT per-file metadata acquisition; fixtures are filled before checks."""
from array import array
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import ROOT
from rdfschema import ColumnRequirement, check_input_schemas, read_root_schema


class RootSchemaTests(unittest.TestCase):
    def test_each_file_is_checked_without_actions_and_read_handles_are_closed(self):
        with tempfile.TemporaryDirectory(prefix='rdf input schema ') as temporary:
            directory = Path(temporary)
            sources = [directory/name for name in ('good.root', 'wrong-type.root', 'missing-branch.root')]
            for index, path in enumerate(sources):
                handle = ROOT.TFile(str(path), 'RECREATE')
                tree = ROOT.TTree('Events', 'schema fixture')
                event = array('d' if index == 1 else 'Q', [123])
                tree.Branch('event', event, 'event/D' if index == 1 else 'event/l')
                jets = ROOT.std.vector('float')()
                if index != 2:
                    tree.Branch('Jet_pt', jets)
                for _ in range(3):
                    jets.clear(); jets.push_back(40.)
                    tree.Fill()
                tree.Write(); handle.Close()
            original = ROOT.RDataFrame
            nodes = []
            def dataframe(tree):
                node = original(tree)
                nodes.append(node)
                return node
            class ReaderRoot:
                RDataFrame = staticmethod(dataframe)
                def __getattr__(self, name): return getattr(ROOT, name)
            with patch.dict('sys.modules', {'ROOT': ReaderRoot()}):
                result = check_input_schemas(sources,
                    [ColumnRequirement('event', 'ULong64_t', 'event-id'), ColumnRequirement('Jet_pt')])
            self.assertFalse(result.passed)
            self.assertEqual([issue.code for issue in result.report.issues], ['schema.type', 'schema.missing'])
            self.assertIn('wrong-type.root', result.report.issues[0].location)
            self.assertIn('missing-branch.root', result.report.issues[1].location)
            self.assertEqual([node.GetNRuns() for node in nodes], [0, 0, 0])
            self.assertTrue(all(not ROOT.gROOT.GetListOfFiles().FindObject(str(path)) for path in sources))
            # Repeat a good input explicitly: no deduplication or first-file shortcut.
            result = check_input_schemas([sources[0], sources[0]], [ColumnRequirement('Jet_pt')])
            self.assertTrue(result.passed)
            self.assertEqual(len(result.schemas), 2)
            self.assertIn('float', result.schemas[0].columns['Jet_pt'])

    def test_missing_tree_and_file_fail_without_hiding_later_inputs(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary)/'empty.root'
            handle = ROOT.TFile(str(path), 'RECREATE'); handle.Close()
            result = check_input_schemas([path, path.parent/'missing.root'], [ColumnRequirement('event')])
            self.assertFalse(result.passed)
            self.assertEqual([issue.code for issue in result.report.issues], ['schema.input', 'schema.input'])
            self.assertIn('Missing TTree', result.report.issues[0].message)
            self.assertFalse(result.schemas)
            self.assertFalse(ROOT.gROOT.GetListOfFiles().FindObject(str(path)))


if __name__ == '__main__':
    unittest.main()
