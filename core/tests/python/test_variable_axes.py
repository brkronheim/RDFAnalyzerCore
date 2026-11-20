"""Variable-axis declarations validate without ROOT and snapshot mutable edges."""
from dataclasses import FrozenInstanceError
import json
import unittest
from rdfanalysis import VariableAxis
from rdfdeclarations import Stage, HistogramGroup, Observable, RegularAxis


class VariableAxisTests(unittest.TestCase):
    def test_immutable_edges_and_group_expansion(self):
        edges=[0,1,3,10];axis=VariableAxis(edges);edges[1]=2
        self.assertEqual(axis.edges,(0.,1.,3.,10.))
        with self.assertRaises(FrozenInstanceError):axis.edges=(0.,2.)
        group=HistogramGroup('mixed',(Observable('a','pt',axis),Observable('b','pt',RegularAxis(5,0,10))))
        items=group.resolve(['Nominal'],budget=2)
        self.assertEqual(items[0]['edges'],axis.edges);self.assertNotIn('bins',items[0])
        self.assertEqual(items[1]['bins'],5);self.assertNotIn('edges',items[1])
        stage=Stage('plots').histograms(group,batch=True)
        stage.histogram_variable('direct','pt',[0,2,10])
        json.dumps(stage.describe().as_dict())
        with self.assertRaisesRegex(ValueError,'phases'):stage.filter('late','pt>0.')

    def test_invalid_edges_fail_during_declaration(self):
        for edges in ([],[0],[0,0],[1,0],[0,float('nan')],[0,float('inf')],[True,2],['0',1],range(3),
                      [2**53,2**53+1],[0,10**400]):
            with self.subTest(edges=edges),self.assertRaises(ValueError):VariableAxis(edges)
        with self.assertRaises(TypeError):Observable('pt','pt',[0,1])
        with self.assertRaisesRegex(ValueError,'region or selection'):
            Stage('bad').histogram_variable('pt','pt',[0,1],region='a',selection='b')


if __name__=='__main__':unittest.main()
