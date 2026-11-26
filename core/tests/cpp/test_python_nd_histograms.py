"""Native packed ND persistence/oracles, kept separate from ordinary TH1 APIs."""
from array import array
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

import numpy as np
import uproot
import awkward as ak
import ROOT

REPO=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(REPO/'core/python'))
if os.environ.get('RDF_ND_MODULE_DIR'):sys.path.insert(0,os.environ['RDF_ND_MODULE_DIR'])
from rdfanalysis import Analysis, AnalysisConfig, AnalysisSession, NDHistogramSettings, NDHistogramGroup, NDHistogramSpec, NDCategoryAxis, RegularAxis, VariableAxis, Stage


def cells(histogram):
    result={}
    for index in range(histogram.GetNbins()):
        coordinates=array('i',[0]*5)
        content=histogram.GetBinContent(index,coordinates)
        result[tuple(coordinates)]=(content,histogram.GetBinError2(index))
    return result


class NativeNDHistogramTests(unittest.TestCase):
    def inputs(self, root):
        source=root/'input.root'
        self.pt=np.array([.25,1.25,2.25,-.5,3.,1.25],dtype='float32')
        self.up=self.pt+1.;self.down=self.pt-.5
        self.weight=np.array([2.,-2.,4.,-1.,1.,2.],dtype='float32')
        self.channel=np.array([.5,1.5,.5,1.5,.5,1.5])
        jets=ak.values_astype(ak.Array([[.25,1.25],[],[-.5,3.5],[.25],[1.25],[]]),np.float32)
        with uproot.recreate(source) as output:
            output.mktree('Events',dict(pt=self.pt,pt_up=self.up,pt_down=self.down,
                weight=self.weight,weight_up=self.weight*2,weight_down=self.weight*.5,
                channel=self.channel,jets=jets))
        return source

    def analysis(self, root, source, backend='boost', precision='float', dense=0, **kwargs):
        options=NDHistogramSettings(backend=backend,precision=precision,dense_limit_mib=dense).native_options()
        options.update(kwargs.pop('options',{}))
        analysis=Analysis(AnalysisConfig(files=[str(source)],output=str(root/'output.root'),skim=False,options=options,**kwargs))
        analysis.native.AddPlugin('histogramManager','NDHistogramManager')
        return analysis

    def oracle(self, labels, *, filtered, variable):
        bins=array('i',[2,1,1,len(labels),2]);low=array('d',[0,0,0,0,0]);high=array('d',[2,1,1,len(labels),3])
        reference=ROOT.THnSparseF('oracle','',5,bins,low,high,16 * 1024)
        reference.Sumw2()
        if variable:reference.SetBinEdges(4,array('d',[0.,1.,3.]))
        for slot,label in enumerate(labels):
            values=self.up if label=='moveUp' else self.down if label=='moveDown' else self.pt
            weights=self.weight*2 if label=='scaleUp' else self.weight*.5 if label=='scaleDown' else self.weight
            for i,value in enumerate(values):
                if filtered and value<=1:continue
                reference.Fill(array('d',[self.channel[i],0.,0.,float(slot),float(value)]),float(weights[i]))
        return cells(reference)

    def test_all_nominal_groups_interleaved_filters_and_automatic_persistence(self):
        for backend,precision,dense in [('root','float',0),('root','float',64),('boost','float',0),('boost','float',64),('boost','double',0)]:
            with self.subTest(backend=backend,precision=precision,dense=dense),tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);source=self.inputs(root);analysis=self.analysis(root,source,backend,precision,dense)
                analysis.register_variation_pair('pt','move','pt_up','pt_down')
                analysis.register_variation_pair('weight','scale','weight_up','weight_down')
                category=NDCategoryAxis('channel',RegularAxis(2,0,2),['a','b'])
                before=NDHistogramGroup('before',[NDHistogramSpec('inclusive','pt',RegularAxis(2,0,3),'weight')],channel=category)
                analysis.nd_histograms(before)
                analysis.define('accepted','pt>1.f',['pt']).filter_column('cut','accepted')
                after=NDHistogramGroup('after',[NDHistogramSpec('selected','pt',VariableAxis([0.,1.,3.]),'weight',force_sparse=True)],channel=category)
                analysis.nd_histograms(after)
                nominal=NDHistogramGroup('nominal',[NDHistogramSpec('diagnostic','pt',RegularAxis(2,0,3),'weight')],channel=category,variations='nominal')
                analysis.nd_histograms(nominal)
                analysis.histogram('ordinary','pt',2,0,3,weight='weight')
                self.assertEqual(analysis.native.event_loops,0)
                self.assertIn('packed-nd-histograms',analysis.describe().capabilities)
                json.dumps(analysis.describe().as_dict())
                analysis.run();self.assertEqual(analysis.native.event_loops,1)
                self.assertEqual(set(analysis.histograms),{'ordinary'})
                metadata=analysis.config.native_config()['metaFile']
                saved=ROOT.TFile.Open(metadata)
                self.assertTrue(saved and not saved.IsZombie())
                try:
                    schema=json.loads(str(saved.Get('__rdf_histogram_schema_v1').GetString().Data()))
                    self.assertEqual(schema['schema_version'],1)
                    self.assertEqual(schema['histograms'][0]['axes'][0]['labels'],['a','b'])
                    for name,gname,filtered,variable in [('inclusive','before',False,False),('selected','after',True,True),('diagnostic','nominal',True,False)]:
                        labels=analysis.inspect()['nd_histograms'][gname]['native']['systematic_labels']
                        if gname!='nominal':self.assertEqual(set(labels),{'Nominal','moveUp','moveDown','scaleUp','scaleDown'})
                        self.assertEqual(cells(saved.Get('nd_histograms_v1/'+name+'_')),self.oracle(labels,filtered=filtered,variable=variable))
                    self.assertEqual(analysis.inspect()['nd_histograms']['nominal']['native']['systematic_labels'],['Nominal'])
                    self.assertEqual(len(schema['histograms']),3)
                finally:saved.Close()
                result=analysis.result();json.dumps(result.as_dict())
                self.assertEqual(len(result.details['nd_histograms']),3)
                with self.assertRaisesRegex(RuntimeError,'before run'):analysis.nd_histograms(before)

    def test_unweighted_via_explicit_unit_weight_and_aligned_empty_collections(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=self.inputs(root);analysis=self.analysis(root,source)
            analysis.define_nominal('unit','1.f')
            declaration=NDHistogramGroup('jets',[NDHistogramSpec('jets','jets',VariableAxis([0.,1.,3.]),'unit')],variations='nominal')
            analysis.nd_histograms(declaration).run()
            saved=ROOT.TFile.Open(analysis.config.native_config()['metaFile'])
            try:
                hist=saved.Get('nd_histograms_v1/jets_');self.assertEqual(sum(v[0] for v in cells(hist).values()),6.)
                self.assertEqual(sum(v[1] for v in cells(hist).values()),6.)
            finally:saved.Close()
            self.assertEqual(analysis.native.event_loops,1)

    def test_per_slot_collections_persist_shapes_weights_and_interleaved_stages(self):
        nominal=[[],[.25,1.25],[-.5],[],[3.5,.25],[1.25]]
        shifted=[[1.25],[],[-.5,3.5],[.25,1.25],[],[.25]]
        weights=np.array([2.,-2.,1.,.5,-1.,2.],dtype='float32')
        passes=np.array([False,True,True,False,True,True])
        shape_passes=np.array([True,False,True,True,False,True])
        for backend in ('root','boost'):
            with self.subTest(backend=backend),tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);source=root/'ragged.root'
                with uproot.recreate(source) as output:
                    output.mktree('Events',dict(
                        jets=ak.values_astype(ak.Array(nominal),np.float32),
                        jets_shift=ak.values_astype(ak.Array(shifted),np.float32),
                        weight=weights,weight_up=weights*2,weight_down=weights*0,
                        accepted=passes,accepted_shift=shape_passes))
                options=NDHistogramSettings(collection_layout='per-slot').native_options()
                options['histogramBackend']=backend
                analysis=self.analysis(root,source,options=options)
                analysis.register_variation('jets','shapeOnly','jets_shift')
                analysis.register_variation('accepted','shapeOnly','accepted_shift')
                analysis.register_variation_pair('weight','scale','weight_up','weight_down')
                axis=VariableAxis([0.,1.,3.])
                early=Stage('early').nd_histograms(NDHistogramGroup('early',[
                    NDHistogramSpec('inclusive','jets',axis,'weight')]))
                late=Stage('late').filter_column('accepted','accepted').nd_histograms(
                    NDHistogramGroup('late',[NDHistogramSpec('selected','jets',axis,'weight')])).nd_histograms(
                    NDHistogramGroup('nominal',[NDHistogramSpec('nominal','jets',axis,'weight')],variations='nominal'))
                analysis.apply(early).apply(late)
                self.assertIn('packed-nd-per-slot',analysis.describe().capabilities)
                self.assertEqual(analysis.native.event_loops,0)
                analysis.run();self.assertEqual(analysis.native.event_loops,1)
                saved=ROOT.TFile.Open(analysis.config.native_config()['metaFile'])
                try:
                    schema=json.loads(str(saved.Get('__rdf_histogram_schema_v1').GetString().Data()))
                    self.assertEqual(len(schema['histograms']),3)
                    for group,name,filtered in [('early','inclusive',False),('late','selected',True),('nominal','nominal',True)]:
                        labels=analysis.inspect()['nd_histograms'][group]['native']['systematic_labels']
                        reference=ROOT.THnSparseF('ragged_oracle','',5,array('i',[1,1,1,len(labels),2]),
                            array('d',[0.,0.,0.,0.,0.]),array('d',[1.,1.,1.,float(len(labels)),3.]),16 * 1024)
                        reference.Sumw2();reference.SetBinEdges(4,array('d',[0.,1.,3.]))
                        for slot,label in enumerate(labels):
                            objects=shifted if label=='shapeOnly' else nominal
                            accepted=shape_passes if label=='shapeOnly' else passes
                            scale=2. if label=='scaleUp' else 0. if label=='scaleDown' else 1.
                            if not scale:continue
                            for event,values in enumerate(objects):
                                if filtered and not accepted[event]:continue
                                for value in values:
                                    reference.Fill(array('d',[0.,0.,0.,float(slot),value]),float(weights[event]*scale))
                        self.assertEqual(cells(saved.Get('nd_histograms_v1/'+name+'_')),cells(reference))
                    self.assertEqual(analysis.result().details['nd_histograms']['late']['configured_storage_options']['histogramCollectionLayout'],'per-slot')
                finally:saved.Close()

    def test_per_slot_misaligned_object_weights_fail_instead_of_broadcasting(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=self.inputs(root)
            analysis=self.analysis(root,source,options={'histogramCollectionLayout':'per-slot'})
            # A separate collection with different lengths is an invalid object
            # weight, even though a scalar event weight would broadcast.
            declaration=NDHistogramGroup('bad',[NDHistogramSpec('bad','jets',RegularAxis(2,0,3),'jets')])
            analysis.nd_histograms(declaration)
            # Keep the same vector input valid first; mismatching coverage is
            # independently tested below using an explicit input branch.
            analysis.run();self.assertEqual(analysis.native.event_loops,1)
            bad_source=root/'bad.root'
            with uproot.recreate(bad_source) as output:
                output.mktree('Events',dict(jets=ak.Array([[.25,1.25],[]]),weight=ak.Array([[1.],[]])))
            bad=Analysis(AnalysisConfig(files=[str(bad_source)],output=str(root/'bad-output.root'),skim=False,
                options=NDHistogramSettings(collection_layout='per-slot').native_options()))
            bad.native.AddPlugin('histogramManager','NDHistogramManager')
            bad.nd_histograms(NDHistogramGroup('bad',[NDHistogramSpec('bad','jets',RegularAxis(2,0,3),'weight')]))
            with self.assertRaisesRegex(Exception,'weight length mismatch'):bad.run()

    def test_collisions_missing_role_late_labels_and_raw_escape_hatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=self.inputs(root);analysis=self.analysis(root,source)
            raw=sys.modules['rdfanalyzer']
            analysis.native.bookNDHistograms('histogramManager',[raw.HistInfo('raw','pt','','weight',2,0,3)],[])
            declaration=NDHistogramGroup('typed',[NDHistogramSpec('typed','pt',RegularAxis(2,0,3),'weight')])
            analysis.nd_histograms(declaration)
            inventory=list(analysis.columns)
            with self.assertRaisesRegex(ValueError,'Duplicate'):
                analysis.native._book_nd_histograms([('raw','pt','','weight',2,0.,3.,[],False)], [('zero__',1,0.,1.,['Default'])]*3,True)
            self.assertEqual(list(analysis.columns),inventory)
            with self.assertRaisesRegex(ValueError,'collides'):
                analysis.native.bookNDHistograms('histogramManager',[raw.HistInfo('typed','pt','','weight',2,0,3)],[])
            with self.assertRaisesRegex(RuntimeError,'Cannot clear'):analysis.native.clearNDHistograms('histogramManager')
            analysis.register_variation('pt','late','pt_up')
            with self.assertRaisesRegex(ValueError,'Register all nuisance'):
                analysis.native._book_nd_histograms([('later','pt','','weight',2,0.,3.,[],False)], [('zero__',1,0.,1.,['Default'])]*3,False)
            self.assertEqual(analysis.native.event_loops,0)

    def test_nominal_first_stages_preserve_filter_definition_order_and_one_loop(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=self.inputs(root);analysis=self.analysis(root,source)
            analysis.register_variation('pt','shape','pt_up')
            early=NDHistogramGroup('early',[NDHistogramSpec('early','pt',RegularAxis(2,0,3),'weight')],variations='nominal')
            late=NDHistogramGroup('late',[NDHistogramSpec('late','twopt',RegularAxis(2,0,6),'weight')])
            first=Stage('first').nd_histograms(early)
            second=Stage('second').define('accepted','pt>1.f',['pt']).filter_column('cut','accepted').define('twopt','pt*2.f',['pt']).nd_histograms(late)
            json.dumps(first.preview(analysis).as_dict());analysis.apply(first)
            json.dumps(second.preview(analysis).as_dict());analysis.apply(second)
            self.assertEqual(analysis.native.event_loops,0)
            self.assertEqual(analysis.inspect()['nd_histograms']['late']['native']['systematic_labels'],['Nominal','shape'])
            json.dumps(analysis.describe().as_dict());analysis.run()
            self.assertEqual(analysis.native.event_loops,1)
            saved=ROOT.TFile.Open(analysis.config.native_config()['metaFile'])
            try:
                self.assertEqual(sum(v[0] for v in cells(saved.Get('nd_histograms_v1/early_')).values()),6.)
                self.assertEqual(sum(v[0] for v in cells(saved.Get('nd_histograms_v1/late_')).values()),12.)
            finally:saved.Close()

    def test_absent_or_wrong_role_missing_inputs_and_disabled_systematics(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=self.inputs(root)
            declaration=NDHistogramGroup('plots',[NDHistogramSpec('pt','pt',RegularAxis(2,0,3),'weight')])
            absent=Analysis(AnalysisConfig(files=[str(source)],output=str(root/'absent.root'),skim=False))
            with self.assertRaisesRegex(RuntimeError,'histogramManager'):absent.nd_histograms(declaration)
            wrong=Analysis(AnalysisConfig(files=[str(source)],output=str(root/'wrong.root'),skim=False))
            wrong.native.AddPlugin('histogramManager','CorrectionManager')
            with self.assertRaisesRegex(RuntimeError,'NDHistogramManager'):wrong.nd_histograms(declaration)
            self.assertEqual(wrong.native.event_loops,0)
            disabled=self.analysis(root,source,systematics=False)
            disabled.register_variation('pt','ignored','pt_up')
            disabled.nd_histograms(declaration)
            self.assertEqual(disabled.inspect()['nd_histograms']['plots']['native']['systematic_labels'],['Nominal'])
            self.assertEqual(disabled.native.event_loops,0)

    def test_raw_frozen_axis_rejects_late_label_before_first_typed_group(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=self.inputs(root);analysis=self.analysis(root,source)
            raw=sys.modules['rdfanalyzer']
            analysis.native.bookNDHistograms('histogramManager',[raw.HistInfo('raw','pt','','weight',2,0,3)],[])
            analysis.register_variation('pt','late','pt_up')
            before=list(analysis.columns)
            with self.assertRaisesRegex(ValueError,'frozen'):
                analysis.native._book_nd_histograms([('typed','pt','','weight',2,0.,3.,[],False)], [('zero__',1,0.,1.,['Default'])]*3,False)
            self.assertEqual(list(analysis.columns),before);self.assertEqual(analysis.native.event_loops,0)

    def test_session_returns_detached_nd_bookings_without_loading_sparse_contents(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=self.inputs(root)
            def build(analysis):
                analysis.native.AddPlugin('histogramManager','NDHistogramManager')
                analysis.nd_histograms(NDHistogramGroup('plots',[NDHistogramSpec('pt','pt',RegularAxis(2,0,3),'weight')]))
            with AnalysisSession(max_jobs=1) as session:
                result=session.run(AnalysisConfig(files=[str(source)],output=str(root/'session.root'),skim=False),build)
            self.assertEqual(result.event_loops,1)
            self.assertEqual(result.histograms,{})
            self.assertEqual(result.nd_histograms['plots']['histograms'][0]['native_key'],'pt_')
            detached=result.as_dict();detached['nd_histograms'].clear()
            self.assertIn('plots',result.nd_histograms)
            json.dumps(result.to_run_result().as_dict())


if __name__=='__main__':unittest.main()
