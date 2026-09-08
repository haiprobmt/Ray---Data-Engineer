"""Behavioral regression tests with real local state/processes, synthetic model/cloud."""
import hashlib
import json
import sqlite3
import sys

import pytest

from conftest import WS, ITEM
from ray_de.analytics import Analytics, compile_select
from ray_de.context import load_context
from ray_de.conversation import ConversationService
from ray_de.fabric import FabricGateway
from ray_de.orchestrator import Orchestrator
from ray_de.processes import run_checked
from ray_de.service import TaskService
from ray_de.task_context import capture_handoff, observe, read
from test_recovery import output


def snapshot(project):
    return {"project_id": project.id, "binding": project.binding}


def test_validation_returns_bounded_redacted_traceback(tmp_path):
    lines = []
    program = "print('password=synthetic-secret-value'); print('https://user:private@host/a?token=hidden'); print('x'*9000); raise AssertionError('expected 3 rows, got 2')"
    code = run_checked([sys.executable, '-c', program], cwd=tmp_path, env={}, timeout=10, diagnostics=lines)
    text = '\n'.join(lines)
    assert code == 1 and 'AssertionError: expected 3 rows, got 2' in text
    assert 'synthetic-secret-value' not in text and 'private@' not in text
    assert 'oversized diagnostic' in text and len(text) <= 12000


def test_pipe_flood_does_not_deadlock_or_store_unbounded_output(tmp_path):
    lines = []
    code = run_checked([sys.executable, '-c', "import sys; [print('a'*100) for _ in range(10000)]; sys.stderr.write('ValueError: missing key\\n')"],
                       cwd=tmp_path, env={}, timeout=10, diagnostics=lines)
    assert code == 0 and len('\n'.join(lines)) < 12500
    assert any('missing key' in line for line in lines)


def test_failed_test_is_repaired_and_freshly_reviewed(project, store, tmp_path):
    project.config = project.config.model_copy(update={'validation_commands': [['{python}', 'main.py']]})
    task = store.create(project.id, 'Return exactly three rows', 'write')
    class Runner:
        authors = reviews = 0
        def run(self, p, prompt, schema, **kw):
            if 'verdict' in schema['properties']:
                self.reviews += 1
                assert kw['read_only'] and not kw.get('thread_id')
                return dict(verdict='PASS', summary='Verified', findings=[], evidence=['main.py passed'])
            self.authors += 1
            if self.authors == 2:
                assert 'AssertionError: expected 3 rows' in prompt
                assert 'LOCAL engineering failure' in prompt
            return dict(output(), artifacts=[{'path': 'main.py', 'content':
                "assert 2 == 3, 'expected 3 rows'\n" if self.authors == 1 else 'assert 3 == 3\n'}])
    runner = Runner()
    result = TaskService(store, runner, tmp_path).run(project, task['id'], task['objective'], 'actor', snapshot=snapshot(project))
    assert result['status'] == 'completed'
    assert runner.authors == 2 and runner.reviews == 1
    assert result['repair_history'][0]['reason'] == 'validation'
    assert read(store, project.id, task['id'])['execution']['model_calls'] == 3


@pytest.mark.parametrize('verdict,expected', [('REWORK', 2), ('BLOCK', 1)])
def test_review_feedback_repairs_only_rework(project, store, verdict, expected):
    task = store.create(project.id, 'Fix duplicate handling', 'write')
    class Runner:
        authors = reviews = 0
        def run(self, p, prompt, schema, **kw):
            if 'verdict' in schema['properties']:
                self.reviews += 1
                assert kw['read_only'] and not kw.get('thread_id')
                return dict(verdict=verdict if self.reviews == 1 else 'PASS', summary='Deduplicate keys', findings=['Preserve leading zeros'], evidence=['main.py'])
            self.authors += 1
            if self.authors == 2:
                assert 'Preserve leading zeros' in prompt
            return dict(output(), artifacts=[{'path':'main.py','content':f'version = {self.authors}\n'}])
    runner = Runner()
    result = Orchestrator(store, runner).run(project, task['id'], task['objective'])
    assert runner.authors == expected and runner.reviews == expected
    assert result['status'] == ('completed' if verdict == 'REWORK' else 'blocked')


def test_repair_is_bounded_even_when_source_keeps_changing(project, store):
    project.config = project.config.model_copy(update={'validation_commands': [['{python}', '-c', 'raise SystemExit(1)']]})
    task = store.create(project.id, 'Repair', 'write')
    class Runner:
        calls = 0
        def run(self, p, prompt, schema, **kw):
            self.calls += 1
            return dict(output(), artifacts=[{'path':'main.py','content':f'version={self.calls}\n'}])
    runner = Runner()
    result = Orchestrator(store, runner).run(project, task['id'], task['objective'])
    assert runner.calls == 4 and result['repair_stop'] == 'attempt_limit'
    assert not result['cloud_eligible']


def test_old_config_binding_is_preserved(project):
    config = project.config.model_dump()
    for key in ('reasoning_effort', 'execution'):
        config.pop(key)
    config['fabric'].pop('tenant')
    config.pop('post_validation_commands')
    for key in ('write_targets', 'allow_definition_export', 'workspace_write', 'create_items'):
        config['fabric'].pop(key)
    expected = hashlib.sha256(json.dumps({'config':config,'repo':str(project.repo),'directory':str(project.directory)},sort_keys=True).encode()).hexdigest()
    assert project.binding == expected
    project.config = project.config.model_copy(update={'reasoning_effort':'high'})
    assert project.binding != expected


def test_handoff_uses_only_same_actor_project_and_survives_restart(project, store):
    class Runner:
        def run(self, *a, **kw):
            return {'message':'Option 2 keeps the existing table names', 'offer_work':True}
    chat = ConversationService(store, Runner())
    chat.reply(project, project, 'actor-a', 'Use option 2 and keep table names')
    chat.reply(project, project, 'actor-b', 'OTHER_ACTOR_SECRET_MARKER')
    with store.connect() as db:
        db.execute("INSERT INTO conversations(actor,role,text,project_id) VALUES (?,?,?,?)", ('actor-a','user','OTHER_PROJECT_MARKER','different-project'))
    task = store.create(project.id, 'Build it', 'write')
    capture_handoff(store, project.id, task['id'], 'actor-a')
    from ray_de.state import StateStore
    reopened = StateStore(store.path)
    reopened.bind(project)
    prompt = load_context(project, reopened.task(project.id, task['id']))
    assert 'Use option 2 and keep table names' in prompt
    assert 'OTHER_ACTOR_SECRET_MARKER' not in prompt and 'OTHER_PROJECT_MARKER' not in prompt
    assert task['objective'] == 'Build it' and 'not accepted decisions or approvals' in prompt


def test_task_observations_are_bounded_and_latest_replaces_old(project, store):
    task = store.create(project.id, 'Investigate', 'read')
    for index in range(30):
        observe(store, project.id, task['id'], [{'request':{'table':str(index)},'data':'x'*3000,'captured_at':str(index)}])
    observe(store, project.id, task['id'], [{'request':{'table':'29'},'data':'latest','captured_at':'new'}])
    records = read(store, project.id, task['id'])['observations']
    assert len(json.dumps(records)) <= 50000 and records[-1]['data'] == 'latest'
    assert sum(r['request']['table']=='29' for r in records) == 1
    with pytest.raises(ValueError):
        read(store, 'other-project', task['id'])


def test_followup_uses_current_request_for_memory(project, store):
    from ray_de.memory import save_decision
    save_decision(project, 'Kho dữ liệu', 'warehouse', 'Use SQL warehouse for aggregation', 'Track keys', 'actor')
    task = store.create(project.id, 'Discuss general setup', 'read')
    assert 'Use SQL warehouse for aggregation' in load_context(project, task, current_request='Bây giờ triển khai kho dữ liệu')


def test_guidance_can_find_tail_and_reference_and_vietnamese(tmp_path, monkeypatch):
    from ray_de import skills
    root = tmp_path / 'upstream'
    skill = root/'skills'/'powerbi-report-authoring'
    (skill/'references').mkdir(parents=True)
    (skill/'SKILL.md').write_text('# Power BI report\n' + 'intro\n'*2500 + '\n# Zebrametric\nZebrametric execution detail here.\n')
    (skill/'references'/'formatting.md').write_text('# PowerBI report formatting\nFormatting table headers and colors.')
    monkeypatch.setattr(skills,'ROOT',root)
    assert any('execution detail here' in r['content'] for r in skills.search_guidance('Zebrametric'))
    assert any('references/formatting.md' in r['path'] for r in skills.search_guidance('formatting table headers'))
    assert any('powerbi-report-authoring' in r['path'] for r in skills.search_guidance('Tạo báo cáo'))
    assert all('sha256' in r and 'start_line' in r for r in skills.search_guidance('report'))


def test_guidance_requests_continue_without_source_changes(project, store, tmp_path):
    task = store.create(project.id, 'Find DAX guidance', 'read')
    class Runner:
        calls = 0
        def run(self, p, prompt, schema, **kw):
            self.calls += 1
            if self.calls == 1:
                return dict(output('working'), guidance_requests=['semantic model DAX guidelines'])
            assert 'pinned_guidance' in prompt and 'sha256' in prompt
            return output()
    runner = Runner()
    result = TaskService(store,runner,tmp_path).run(project,task['id'],task['objective'],'actor',snapshot=snapshot(project))
    assert result['status']=='completed' and runner.calls==2
    assert not result['changed_files']


def test_model_budget_pauses_before_next_call(project, store, tmp_path):
    project.config = project.config.model_copy(update={'execution': project.config.execution.model_copy(update={'max_model_calls':1})})
    task = store.create(project.id,'Investigate','read')
    class Runner:
        calls = 0
        def run(self,*a,**kw):
            self.calls += 1
            return dict(output('working'),guidance_requests=['spark errors'])
    runner=Runner()
    result=TaskService(store,runner,tmp_path).run(project,task['id'],task['objective'],'actor',snapshot=snapshot(project))
    assert result['status']=='paused' and runner.calls==1 and result['phase']=='execution_limit'
    assert store.task(project.id,task['id'])['status']=='PAUSED'


@pytest.mark.parametrize('bad', ['Sales]; DROP TABLE X;--','dbo.Sales','token;exec','a b'])
def test_analytical_identifiers_cannot_inject_sql(bad):
    with pytest.raises(ValueError):
        compile_select('lakehouse_profile','dbo','Sales',{'columns':[bad]})


def test_aggregate_parameters_do_not_become_sql():
    value="x'; DROP TABLE Sales;--"
    sql, params=compile_select('lakehouse_aggregate','dbo','Sales',{'group_by':['plant'], 'metrics':[{'function':'sum','column':'amount'}],
        'filters':[{'column':'plant','operator':'eq','value':value}]})
    assert value not in sql and params==(value,)
    assert 'SUM([amount]) AS metric_0' in sql and 'ORDER BY [plant]' in sql


@pytest.mark.parametrize('spec', [ {'columns':['password']}, {'columns':['id'],'query':'DROP TABLE X'}, {'columns':['id'],'filters':[{'column':'id','operator':'eq','value':None}]}])
def test_analytics_rejects_credentials_raw_sql_and_ambiguous_null(spec):
    with pytest.raises(ValueError):
        Analytics.model_validate(spec)


def test_multiset_comparison_counts_duplicates_and_nulls():
    sql, params=compile_select('lakehouse_compare','main','source',{'columns':['id','amount'],'compare_schema':'main','compare_table':'target'})
    # SQLite executes this subset with COUNT(*) in place of SQL Server COUNT_BIG.
    db=sqlite3.connect(':memory:')
    db.execute('CREATE TABLE source(id TEXT, amount INTEGER)')
    db.execute('CREATE TABLE target(id TEXT, amount INTEGER)')
    db.executemany('INSERT INTO source VALUES (?,?)',[('001',10),('001',10),(None,5),('002',9)])
    db.executemany('INSERT INTO target VALUES (?,?)',[('001',10),(None,5),('003',7)])
    assert db.execute(sql.replace('COUNT_BIG','COUNT'),params).fetchone()==(2,1)
    db.close()


def test_analytical_read_resolves_same_lakehouse_target(project, store, tmp_path):
    requests=[]
    def metadata(endpoint):
        if '/lakehouses/' in endpoint:
            return {'id':ITEM,'properties':{'sqlEndpointProperties':{'id':WS,'provisioningStatus':'Success','connectionString':'sample.datawarehouse.fabric.microsoft.com'}}}
        return {'id':WS,'type':'SQLEndpoint','displayName':'Lakehouse'}
    req={'operation':'lakehouse_compare','workspace_id':WS,'item_id':ITEM,'table_name':'source','analytics':{'columns':['id'],'compare_table':'target'}}
    gate=FabricGateway(project,store,tmp_path,executor=metadata,sql_executor=lambda r:requests.append(r) or {'columns':['missing_from_target'],'rows':[[2]]})
    result=gate.read(req)
    assert requests[0]['database']=='Lakehouse' and requests[0]['analytics']['compare_table']=='target'
    assert result['source']=='provided_executor'
    with pytest.raises(ValueError):
        gate.read(dict(req,workspace_id='33333333-3333-3333-3333-333333333333'))
    assert len(requests)==1


def test_definition_requires_export_policy_and_reads_index_then_part(project,store,tmp_path):
    import base64
    calls=[]
    def executor(endpoint,**kwargs):
        calls.append((endpoint,kwargs))
        if endpoint.endswith('getDefinition?format=ipynb'):
            return {'status_code':200,'text':{'definition':{'parts':[{'path':'notebook.ipynb','payloadType':'InlineBase64','payload':base64.b64encode(b'print(42)\npassword=hidden-secret').decode()}]}}}
        return {'id':ITEM,'type':'Notebook'}
    gate=FabricGateway(project,store,tmp_path,executor=executor)
    req={'operation':'get_item_definition','workspace_id':WS,'item_id':ITEM}
    with pytest.raises(ValueError):
        gate.read(req)
    assert calls==[]
    project.config=project.config.model_copy(update={'fabric':project.config.fabric.model_copy(update={'allow_definition_export':True})})
    assert gate.read(req)['data']['parts'][0]['path']=='notebook.ipynb'
    result=gate.read(dict(req,part_path='notebook.ipynb'))
    assert 'print(42)' in result['data']['content'] and 'hidden-secret' not in json.dumps(result)
    assert any(kw=={'definition':True,'envelope':True} for _,kw in calls)


def test_generic_job_read_checks_identity(project,store,tmp_path):
    req={'operation':'get_job_status','workspace_id':WS,'item_id':ITEM,'job_id':WS}
    gate=FabricGateway(project,store,tmp_path,executor=lambda endpoint:{'id':ITEM,'status':'Completed'})
    with pytest.raises(ValueError):
        gate.read(req)


def test_rework_cannot_pass_unchanged_source_by_reviewer_flip(project, store):
    task = store.create(project.id, 'Correct the source', 'write')
    class Runner:
        reviews = 0
        def run(self, p, prompt, schema, **kw):
            if 'verdict' in schema['properties']:
                self.reviews += 1
                return dict(verdict='REWORK' if self.reviews == 1 else 'PASS', summary='Check key handling', findings=[], evidence=['main.py'])
            return dict(output(), artifacts=[{'path':'main.py','content':'answer=2\n'}])
    result=Orchestrator(store,Runner()).run(project,task['id'],task['objective'])
    assert result['status']=='blocked' and result['repair_stop']=='no_progress'
    assert not result['cloud_eligible']


def test_budget_expiry_during_validation_remains_paused(project,store,tmp_path,monkeypatch):
    from ray_de.execution import ExecutionLimit
    task=store.create(project.id,'Change source','write')
    class Runner:
        def run(self,*a,**kw):
            return dict(output(),artifacts=[{'path':'main.py','content':'answer=2\n'}])
    def expire(*a,**kw):
        raise ExecutionLimit('limit')
    monkeypatch.setattr('ray_de.orchestrator.run_checked',expire)
    result=TaskService(store,Runner(),tmp_path).run(project,task['id'],task['objective'],'actor',snapshot=snapshot(project))
    assert result['status']=='paused' and result['phase']=='execution_limit'
    assert store.pending_source(project.id,task['id'])==['main.py']


def test_past_resolution_requires_verification_and_project_scope(project,store):
    from ray_de.task_context import recall_completed
    previous=store.create(project.id,'Fix duplicate customer rows','write')
    store.update(project.id,previous['id'],'COMPLETED',result=output())
    assert recall_completed(store,project.id,'duplicate customer')==[]
    result=dict(output(),host_validation=['Validation 1: exit 0'],review={'verdict':'PASS'})
    store.update(project.id,previous['id'],'COMPLETED',result=result)
    assert recall_completed(store,project.id,'duplicate customer')[0]['task_id']==previous['id']
    assert recall_completed(store,'other-project','duplicate customer')==[]


def test_async_definition_only_polls_validated_operation(project,store,tmp_path):
    project.config=project.config.model_copy(update={'fabric':project.config.fabric.model_copy(update={'allow_definition_export':True})})
    calls=[]
    def executor(endpoint,**kw):
        calls.append(endpoint)
        if 'getDefinition' in endpoint:
            return {'status_code':202,'headers':{'Location':f'https://api.fabric.microsoft.com/v1/operations/{WS}'}}
        if endpoint.endswith('/result'):
            return {'definition':{'parts':[]}}
        if endpoint.startswith('operations/'):
            return {'status':'Succeeded'}
        return {'id':ITEM,'type':'DataPipeline'}
    gate=FabricGateway(project,store,tmp_path,executor=executor)
    req={'operation':'get_item_definition','workspace_id':WS,'item_id':ITEM}
    assert gate.read(req)['data']['parts']==[]
    assert calls[-2:]==['operations/'+WS,'operations/'+WS+'/result']
    gate.executor=lambda endpoint,**kw: {'status_code':202,'headers':{'Location':'https://evil.example/operations/'+WS}} if 'getDefinition' in endpoint else {'id':ITEM,'type':'DataPipeline'}
    with pytest.raises(ValueError):
        gate.read(req)


def test_aggregate_executes_correct_filtered_totals():
    sql,params=compile_select('lakehouse_aggregate','main','sales',{'group_by':['plant'],'metrics':[{'function':'sum','column':'amount'}],
         'filters':[{'column':'year','operator':'eq','value':2026}]})
    db=sqlite3.connect(':memory:')
    db.execute('CREATE TABLE sales(plant TEXT, amount INT, year INT)')
    db.executemany('INSERT INTO sales VALUES (?,?,?)',[('A',10,2026),('A',20,2026),('B',99,2025),('B',7,2026)])
    assert db.execute(sql.replace('TOP (101) ',''),params).fetchall()==[('A',30),('B',7)]
    db.close()


def test_sdk_usage_events_are_recorded_without_model_text(tmp_path):
    from openai_codex.generated.v2_all import ThreadTokenUsageUpdatedNotification, TokenUsageBreakdown
    from openai_codex.models import Notification
    from ray_de.sdk_worker import consume_turn
    from test_sdk_stream import message, completed, StreamTurn
    usage = TokenUsageBreakdown(input_tokens=10, output_tokens=5, total_tokens=15,
                               cached_input_tokens=0, reasoning_output_tokens=2)
    event = Notification('thread/tokenUsage/updated', ThreadTokenUsageUpdatedNotification.model_validate({
        'threadId': 'thread', 'turnId': 'turn',
        'tokenUsage': {'last': usage.model_dump(by_alias=True), 'total': usage.model_dump(by_alias=True)}}))
    turn = StreamTurn([event, message('{"ok":true}'), completed()])
    metadata = {'configured_model': None, 'resolved_model': None, 'usage': None}
    assert consume_turn(turn, tmp_path/'progress.json', metadata) == {'ok': True}
    assert metadata['usage']['total_tokens'] == 15 and metadata['resolved_model'] is None
    saved = json.loads((tmp_path/'runtime.json').read_text())
    assert saved == metadata and turn.closed
