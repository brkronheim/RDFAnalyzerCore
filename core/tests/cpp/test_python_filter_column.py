"""Explicit bool-column filtering preserves stages and native variation masks."""
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
import numpy as np
import uproot

REPO=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(REPO/'core/python'))
if os.environ.get('RDF_FILTER_COLUMN_MODULE_DIR'):
    sys.path.insert(0,os.environ['RDF_FILTER_COLUMN_MODULE_DIR'])
from rdfanalysis import Analysis,AnalysisConfig,Stage


class BoolColumnTests(unittest.TestCase):
    def test_migrations_weights_interleaving_and_description_match_expression_path(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);source=root/'events.root'
            with uproot.recreate(source) as output:
                output.mktree('Events',{'pt':np.array([24.,26.,35.]),'shift':np.array([28.,24.,37.]),
                    'weight':np.array([-2.,3.,-4.]),'weight_up':np.array([-2.2,3.3,-4.4])})
            results=[];graphs=[]
            for direct in (False,True):
                analysis=Analysis(AnalysisConfig(files=[str(source)],output=str(root/f'{direct}.root'),skim=False),record_graph=True)
                analysis.register_variation('pt','shape','shift')
                analysis.register_variation('weight','weight_up','weight_up')
                analysis.define('accepted','pt>25.',['pt']).count('before')
                if direct:analysis.filter_column('cut','accepted')
                else:analysis.filter('cut','accepted',['accepted'])
                analysis.define('observable','pt*2.',['pt']).count('after')
                for label in ('Nominal','shape','weight_up'):
                    analysis.histogram(label,'observable',10,0,100,weight='weight',variation=label)
                description=analysis.describe().as_dict()
                self.assertIn('bool-column-filter',description['capabilities'])
                graphs.append(description['details']['graph'])
                self.assertEqual(analysis.native.event_loops,0)
                analysis.run();self.assertEqual(analysis.native.event_loops,1)
                results.append((analysis.counts,analysis.histograms))
                with self.assertRaisesRegex(RuntimeError,'before run'):analysis.filter_column('late','accepted')
            self.assertEqual(results[0],results[1])
            histograms=results[1][1]
            self.assertAlmostEqual(sum(histograms['Nominal']['sumw']),-1.)
            self.assertAlmostEqual(sum(histograms['Nominal']['sumw2']),25.)
            self.assertAlmostEqual(sum(histograms['shape']['sumw']),-6.)
            self.assertAlmostEqual(sum(histograms['shape']['sumw2']),20.)
            self.assertAlmostEqual(sum(histograms['weight_up']['sumw']),-1.1)
            self.assertFalse(any(row['name']=='__python_filter_predicate_cut' for row in graphs[1]['operations']))
            self.assertTrue(any(row['name']=='__python_filter_predicate_cut' for row in graphs[0]['operations']))

    def test_invalid_predicate_does_not_change_graph_and_old_module_has_clear_error(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);source=root/'events.root'
            with uproot.recreate(source) as output:output.mktree('Events',{'pt':np.array([24.,26.])})
            analysis=Analysis(AnalysisConfig(files=[str(source)],output=str(root/'out.root'),skim=False))
            before=analysis.inspect()
            for name,column in (('','pt'),('cut name','pt'),('ok',''),('ok','bad\ncolumn')):
                with self.assertRaises(ValueError):analysis.filter_column(name,column)
            with self.assertRaisesRegex(ValueError,'scalar bool'):analysis.filter_column('bad','pt')
            with self.assertRaisesRegex(ValueError,'does not exist'):analysis.filter_column('bad','missing')
            self.assertEqual(analysis.inspect(),before)
        old=object.__new__(Analysis);old.native=SimpleNamespace()
        with self.assertRaisesRegex(RuntimeError,'rebuild rdfanalyzer'):old.filter_column('cut','accepted')

    def test_staged_bool_filters_and_definitions_interleave_without_extra_loops(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);source=root/'events.root'
            with uproot.recreate(source) as output:output.mktree('Events',{'pt':np.array([24.,26.,35.])})
            analysis=Analysis(AnalysisConfig(files=[str(source)],output=str(root/'out.root'),skim=False))
            stage=Stage('interleaved').define('accepted','pt>25.',['pt']).filter_column('first','accepted')
            stage.define('observable','pt*2.',['pt']).filter('second','observable>60.',['observable']).count('selected')
            analysis.apply(stage)
            self.assertEqual(analysis.native.event_loops,0)
            analysis.run()
            self.assertEqual(analysis.counts['selected'],1)
            self.assertEqual(analysis.native.event_loops,1)


if __name__=='__main__':unittest.main()
