import copy
import json
import pathlib
import tempfile
import unittest

import yaml

from visualization import content, project


def model_fixture():
    source='source.vintage_data.raw.demo'
    base='model.vintage_data.base_demo'
    uid='model.vintage_data.fct_demo'
    def col(name,typ,**meta):
        dimension={'type':'timestamp' if 'timestamp' in typ else 'string'}
        if 'timestamp' in typ:
            dimension['time_intervals']=['RAW','DAY','WEEK','MONTH','YEAR']
        return {'name':name,'description':name,'data_type':typ,'config':{'meta':{'dimension':dimension,**meta}}}
    meta={'grain':'id, observed_at','label':'Demo',
          'metrics':{'entities':{'type':'count_distinct','sql':'${TABLE}.id'},
                     'reported':{'type':'sum','sql':'${TABLE}.amount','label':'Reported amount'}},
          'vintage':{'visualization':{'dimension':'category','metric':'entities','time':'observed_at','title':'Entities by category','description':'Distinct entities within category; categories may overlap.','detail_fields':['id','category','observed_at'],
              'analysis':{'headline':'how many demo entities each category publishes.','event_time':'happened_at','grain':'MONTH','controls':['category'],
                  'trends':[
                      {'slug':'volume','kind':'total','title':'Entities per month','description':'Distinct entities by event month; absent months are absent, not zero.'},
                      {'slug':'by-category','kind':'breakdown','breakdown':'category','top_n':4,'title':'Entities per month by category','description':'Largest four categories only; the remainder is excluded, not aggregated.'},
                      {'slug':'mix','kind':'composition','breakdown':'category','metric':'reported','grain':'DAY','time':'observed_at','window_days':30,'title':'Reported amount mix per day','description':'Stacked reported amount by category over the collection window.'}]}}}}
    return {'metadata':{'adapter_type':'duckdb'},'sources':{source:{'name':'demo','source_name':'raw'}},'nodes':{
        base:{'name':'base_demo','resource_type':'model','package_name':'vintage_data','original_file_path':'models/base/base_demo.sql','depends_on':{'nodes':[source]}},
        uid:{'name':'fct_demo','unique_id':uid,'description':'Demo observations','resource_type':'model','package_name':'vintage_data','original_file_path':'models/marts/demo/fct_demo.sql',
             'config':{'materialized':'table','meta':meta},'columns':{'id':col('id','varchar'),'category':col('category','varchar'),
                 'observed_at':col('observed_at','timestamp with time zone'),'happened_at':col('happened_at','timestamp with time zone')},'depends_on':{'nodes':[base]}}}}


class ContentTest(unittest.TestCase):
    def test_coverage_requires_chart_dashboard_fields_and_metadata(self):
        manifest=model_fixture()
        with tempfile.TemporaryDirectory() as directory:
            path=pathlib.Path(directory)
            self.assertFalse(project.coverage(manifest,path)['ok'])
            content.write_content(manifest,destination=path)
            self.assertTrue(project.coverage(manifest,path)['ok'])
            self.assertEqual(content.validate_schemas(path),[])
            self.assertEqual(content.write_content(manifest,check=True,destination=path),[])
            manifest['nodes']['model.vintage_data.fct_demo']['columns'].pop('category')
            result=project.coverage(manifest,path)
            self.assertFalse(result['ok'])
            self.assertTrue(any('unknown field' in issue for issue in result['models'][0]['issues']))

    def test_trend_charts_render_verified_shapes_and_reach_the_dashboard(self):
        manifest=model_fixture()
        outputs=content.render(manifest)
        total=outputs['charts/fct-demo-volume.yml']
        self.assertEqual(total['metricQuery']['dimensions'],['fct_demo_happened_at_month'])
        self.assertEqual(total['chartConfig']['config']['eChartsConfig']['series'][0]['type'],'line')
        self.assertNotIn('pivotConfig',total)
        self.assertNotIn('stack',total['chartConfig']['config']['layout'])
        self.assertEqual(total['metricQuery']['filters'],{})
        breakdown=outputs['charts/fct-demo-by-category.yml']
        self.assertEqual(breakdown['pivotConfig'],{'columns':['fct_demo_category']})
        self.assertEqual(breakdown['chartConfig']['config']['columnLimit'],4)
        # Largest series must survive columnLimit, so the metric sorts first.
        self.assertEqual([sort['fieldId'] for sort in breakdown['metricQuery']['sorts']],
                         ['fct_demo_entities','fct_demo_happened_at_month'])
        composition=outputs['charts/fct-demo-mix.yml']
        self.assertEqual(composition['chartConfig']['config']['eChartsConfig']['series'][0]['type'],'bar')
        self.assertTrue(composition['chartConfig']['config']['layout']['stack'])
        self.assertEqual(composition['metricQuery']['metrics'],['fct_demo_reported'])
        window=composition['metricQuery']['filters']['dimensions']['and'][0]
        self.assertEqual(window['target']['fieldId'],'fct_demo_observed_at')
        self.assertEqual(window['values'],[30])
        dashboard=outputs['dashboards/demo.yml']
        charts=[tile['properties']['chartSlug'] for tile in dashboard['tiles'] if tile['type']=='saved_chart']
        self.assertEqual(charts,['fct-demo-volume','fct-demo-by-category','fct-demo-mix','fct-demo','fct-demo-details'])
        self.assertTrue(any(tile['type']=='heading' for tile in dashboard['tiles']))
        # Date zoom re-grains every tile, so only grains both axes declare are offered.
        self.assertEqual(dashboard['config']['dateZoomGranularities'],['Day','Week','Month','Year'])
        self.assertFalse(dashboard['config']['isDateZoomDisabled'])
        self.assertEqual([rule['target']['fieldId'] for rule in dashboard['filters']['dimensions']],['fct_demo_category'])
        self.assertTrue(dashboard['filters']['dimensions'][0]['disabled'])

    def test_gaps_are_scheduled_work_and_bad_specifications_are_contract_issues(self):
        manifest=model_fixture()
        node=manifest['nodes']['model.vintage_data.fct_demo']
        with tempfile.TemporaryDirectory() as directory:
            path=pathlib.Path(directory)
            content.write_content(manifest,destination=path)
            report=project.coverage(manifest,path)
            self.assertEqual(report['models'][0]['gaps'],[])
            self.assertEqual(report['trend_covered_count'],1)
            self.assertEqual(report['trend_chart_count'],3)
            analysis=node['config']['meta']['vintage']['visualization']['analysis']
            trends=analysis.pop('trends')
            missing=project.coverage(manifest,path)
            self.assertTrue(missing['ok'])
            self.assertTrue(any('no time series' in gap for gap in missing['models'][0]['gaps']))
            self.assertEqual(missing['gap_count'],1)
            analysis['trends']=[trend for trend in trends if trend['kind']=='total']
            partial=project.coverage(manifest,path)
            self.assertTrue(any('no dimensional trend' in gap for gap in partial['models'][0]['gaps']))
            analysis['trends']=[dict(trends[0],time='observed_at')]
            collection_only=project.coverage(manifest,path)
            self.assertTrue(any('collection time only' in gap for gap in collection_only['models'][0]['gaps']))
            analysis['trends']=[dict(trends[0],grain='QUARTER')]
            invalid=project.coverage(manifest,path)
            self.assertFalse(invalid['ok'])
            self.assertTrue(any('does not declare time_intervals entry QUARTER' in issue
                                for issue in invalid['models'][0]['issues']))

    def test_check_family_validates_schema_yaml_offline(self):
        model={'name':'fct_demo','description':'Demo observations',
               'config':{'meta':{'grain':'id','label':'Demo',
                   'metrics':{'entities':{'type':'count_distinct'},'reported':{'type':'sum'}},
                   'vintage':{'visualization':{'dimension':'category','metric':'entities','time':'observed_at',
                       'title':'t','description':'d','detail_fields':['id'],
                       'analysis':{'headline':'h.','event_time':'observed_at','grain':'DAY','controls':['category'],
                           'trends':[{'slug':'volume','kind':'total','title':'t','description':'d'},
                                     {'slug':'mix','kind':'composition','breakdown':'category','metric':'reported','title':'t','description':'d'}]}}}}},
               'columns':[{'name':'id','description':'id','data_type':'varchar','config':{'meta':{'dimension':{'type':'string'}}}},
                          {'name':'category','description':'category','data_type':'varchar','config':{'meta':{'dimension':{'type':'string'}}}},
                          {'name':'observed_at','description':'observed','data_type':'timestamp with time zone',
                           'config':{'meta':{'dimension':{'type':'timestamp','time_intervals':['RAW','DAY','MONTH']}}}}]}
        with tempfile.TemporaryDirectory() as directory:
            path=pathlib.Path(directory)/'_demo_models.yml'
            path.write_text(yaml.safe_dump({'version':2,'models':[model]}))
            result=project.check_family(path)
            self.assertTrue(result['ok'],result)
            self.assertEqual([trend['slug'] for trend in result['models'][0]['trends']],['volume','mix'])
            model['config']['meta']['vintage']['visualization']['analysis']['trends'][1].pop('breakdown')
            path.write_text(yaml.safe_dump({'version':2,'models':[model]}))
            broken=project.check_family(path)
            self.assertFalse(broken['ok'])
            self.assertTrue(any('requires a breakdown' in error for error in broken['models'][0]['errors']))

    def test_bundle_preserves_source_and_normalizes_only_serving_artifact(self):
        manifest=model_fixture();original=copy.deepcopy(manifest)
        with tempfile.TemporaryDirectory() as directory:
            path=pathlib.Path(directory)
            project.bundle(manifest,path,'another_database')
            normalized=json.loads((path/'target/manifest.json').read_text())
            self.assertEqual(normalized['metadata']['adapter_type'],'postgres')
            self.assertEqual(set(normalized['nodes']),{'model.vintage_data.fct_demo'})
            self.assertEqual(normalized['nodes']['model.vintage_data.fct_demo']['database'],'another_database')
            profile=json.loads((path/'profiles.yml').read_text())['vintage_serving']['outputs']['serving']
            self.assertEqual(profile['sslmode'],'disable')
            self.assertIn("env_var('LIGHTDASH_READER_PASSWORD')",profile['password'])
            self.assertEqual(json.loads((path/'source-manifest.json').read_text()),original)
        self.assertEqual(manifest,original)
        self.assertEqual(project.source_ancestors(manifest,'model.vintage_data.fct_demo'),{'demo'})

    def test_all_checked_in_content_matches_pinned_schemas(self):
        self.assertEqual(content.validate_schemas(),[])

    def test_dimension_aliases_are_used_in_charts_and_filters(self):
        manifest=model_fixture()
        node=manifest['nodes']['model.vintage_data.fct_demo']
        node['columns']['category']['config']['meta']['dimension']['name']='cat'
        node['columns']['observed_at']['config']['meta']['dimension']['name']='time'
        with tempfile.TemporaryDirectory() as directory:
            path=pathlib.Path(directory)
            content.write_content(manifest,destination=path)
            self.assertTrue(project.coverage(manifest,path)['ok'])
            chart=content.render(manifest)['charts/fct-demo.yml']
            self.assertEqual(chart['metricQuery']['dimensions'],['fct_demo_cat'])
            self.assertEqual(chart['metricQuery']['filters']['dimensions']['and'][0]['target']['fieldId'],'fct_demo_time')
