import dagster
from dagster import In, Out
import os
from os.path import join
import re

@dagster.resource(config_schema={'credentials':str})
def connection(context):
    credentials = context.resource_config['credentials']
    return credentials.upper()

@dagster.resource(config_schema={'basedir':str})
def paths(context):
    basedir = context.resource_config['basedir']
    rundir = join(basedir, context.run_id)
    return locals()

@dagster.op(out={'sent': Out(dagster_type=str, io_manager_key='custom_io_manager')},
         required_resource_keys={'connection', 'paths'},
            config_schema={'data_key':str})
def get_string(context):
    context.log.info(f"Connection to context.resources.connection: {context.resources.connection}")
    data = {'1': 'A#$.or?>)(*@alpha',
            '2': 'b!*@Or><@!BeTa(*#@'}
    return data[context.op_config['data_key']]

@dagster.op(ins={'sent': In(dagster_type=str)},
            out={'sent_norm': Out(dagster_type=str, #io_manager_key='custom_io_manager'
                                  )},
            required_resource_keys={'paths'},
         config_schema={'norm':str, 'force_error':bool})
def normalize_string(context, sent):
    # Note: if the variable names after context don't match the definitions in 'ins',
    # dagster.DagsterInvalidDefinitionError will be raised
    if context.op_config['force_error']:
        raise ValueError('We forced an error here')
    norm = context.op_config['norm']
    if norm.upper() == 'UPPER':
        return sent.upper()
    elif norm.upper() == 'LOWER':
        return sent.lower()

@dagster.op(ins={'sent_norm': In(dagster_type=str)},
            out={'sent_clean': Out(dagster_type=str, io_manager_key='custom_io_manager')},
            required_resource_keys={'paths'},
            config_schema={'hyperparams': dict})
def clean_string(context, sent_norm):
    hyperparams = context.op_config['hyperparams']
    def remove_punc(x, **hyperparams):
        context.log.info(f"Received {len(hyperparams)} kwargs")
        return re.sub(r'[^\w\s]', r' ', x)
    return remove_punc(sent_norm, **hyperparams)


class CustomIOManager(dagster.IOManager):
    def __init__(self, basedir, run_id):
        self.basedir = basedir
        self.run_id = run_id
        self.savefolder = join(basedir, run_id)
        os.makedirs(self.savefolder, exist_ok=True)

    def handle_output(self, context, obj):
        op_identifier = context.get_output_identifier()
        filename = context.name+'.txt'
        filepath = join(self.savefolder, filename)
        context.log.info(f"handle_output:\n\tOutput identifier: {op_identifier}\n\tWrite path: {filepath}")
        with open(filepath, 'w') as f:
            f.write(str(obj))
        context.log_event(
            dagster.AssetMaterialization(
                asset_key=context.name, description="Persisted result to storage",
                metadata = {'len': len(str(obj))}
            )
        )

    def load_input(self, context):
        filename = context.upstream_output.name+'.txt'
        filepath = join(self.savefolder, filename)
        context.log.info(f"load_input: loading from filepath {filepath}")
        with open(filepath, 'r') as f:
            lines = f.readlines()
        return '\n'.join(lines)

@dagster.io_manager(required_resource_keys={'paths'})
def customiomanager(context):
    basedir = context.resources.paths['basedir']
    datadir = join(basedir, 'data')
    run_id = context.run_id
    return CustomIOManager(datadir, run_id)

@dagster.graph
def clean_string_graph():
    clean_string(normalize_string(get_string()))

clean_string_job = clean_string_graph.to_job(
    resource_defs={'connection': connection,
                   'paths': paths,
                   'custom_io_manager': customiomanager},
    config={'ops':{'get_string': {'config': {'data_key': '1'}},
                   'normalize_string': {'config': {'norm':'upper', 'force_error':False}},
                   'clean_string': {'config': {'hyperparams': {'dim':10, 'reg':0.01}}}
                   },
            'resources':{
                'connection': {'config': {'credentials': 'HORRIBLE_PASSWORD'}},
                'paths': {'config': {'basedir': './'}},
                'custom_io_manager': {'config': {'basedir': './history'}}
                }
            }
)

@dagster.schedule(job=clean_string_job, cron_schedule="0/1 * * * *", execution_timezone="US/Pacific")
def every_minute_string_job_schedule(context):
    scheduled_date = context.scheduled_execution_time.strftime("%Y-%m-%d %H:%M:%S")
    return RunRequest(
        job=clean_string_job,
        tags={"date": scheduled_date},
    )

@dagster.repository
def repo_1():
    return [clean_string_job, every_minute_string_job_schedule]

