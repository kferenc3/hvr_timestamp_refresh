#!/usr/bin/python3

import json
import getopt
import datetime
import os
import sys
import re
from time import sleep
import traceback
import pyhvr
import pyodbc
from pytz import timezone
from dotenv import load_dotenv

load_dotenv()

class Options:
  channel = ''
  context = ''
  context_initial = 'initial'
  context_incremental = 'incremental'
  context_other = 'channel_change'
  ddl_check = 'no'
  ehvr_config = ''
  execution_context = ''
  hana_timezone = 'UTC'
  hub = ''
  location = ''
  me = ''
  mode = ''
  username = ''
  parallel_sessions = 6
  password = ''
  path_block_file = ''
  path_state_file = ''
  retries = 0
  rewind_minutes = 1
  source_loc = ''
  state_directory = ''
  state_file = ''
  target_dsn = ''
  target_loc = ''
  target_table = ''
  trace = 0
  ts_column = ''
  ts_low_watermark = None
  uri = ''


options = Options()

##### Support functions #########################################################
class ExecutionError(Exception):
  pass


def version_check():

  global python3
  python3 = sys.version_info[0] == 3


def print_raw(_msg, tgt= None):

  _msg= re.sub(r'!\{[^}]*\}!', '!{xxxxxxxx}!', _msg)

  if tgt is None:
    tgt = sys.stdout

  tgt.write(_msg)
  tgt.flush()


def print_message(_msg, *args, **kwargs):

  _msg = str(_msg).format(*args, **kwargs)

  # convert newlines to <space><backslash><newline><space> like HVR exceptions
  _msg = _msg.replace("\n", " \\\n ")
  print_raw("{0}\n".format(_msg))


def trace(level, _msg, *args, **kwargs):

  global options

  if level > options.trace:
     return
  header = "TRACE=" + str(level) + ": "
  _msg = str(_msg).format(*args, **kwargs)
  _msg = _msg.replace("\n", " \\\n ")
  _msg = header + _msg
  print_raw("{0}\n".format(_msg))


def load_execution_env():

  execution_env= {}

  for k,v in os.environ.items():
    k= str(k)
    if k not in execution_env:
      execution_env[k]= str(v)

  return execution_env


def load_environment():

  evars = load_execution_env()

  if 'HVR_HUB' in evars:
    options.hub = evars['HVR_HUB']
  if 'HVR_CONFIG' in evars:
    options.ehvr_config = evars['HVR_CONFIG']
  if 'HVR_JOB_RETRIES' in evars:
    options.retries = int(evars['HVR_JOB_RETRIES'])
  if 'HVR_VAR_TS_LOW_WATERMARK' in evars:
    options.ts_low_watermark = (evars['HVR_VAR_TS_LOW_WATERMARK']).replace("'","")


def print_environment():

  if options.trace < 2:
    return
  trace(2, "============================================")
  env = os.environ
  if python3:
    for key, value in env.items():
      if key.find('HVR') != -1:
        trace(2, "{} = {}", key, value)
  else:
    for key, value in env.iteritems():
      if key.find('HVR') != -1:
        trace(2, "{} = {}", key, value)
  trace(2, "============================================")


def print_options():

  if options.trace < 1:
    return
  trace(1, "============================================")
  trace(1, "channel = {}", options.channel) 
  trace(1, "context = {}", options.context)
  trace(1, "context_initial = {}", options.context_initial)
  trace(1, "context_incremental = {}", options.context_incremental)
  trace(1, "context_other = {}", options.context_other)
  trace(1, "ddl_check = {}", options.ddl_check)
  trace(1, "ehvr_config = {}", options.ehvr_config) 
  trace(1, "execution_context = {}", options.execution_context) 
  trace(1, "hana_timezone = {}", options.hana_timezone) 
  trace(1, "hub = {}", options.hub) 
  trace(1, "location = {}", options.location) 
  trace(1, "me = {}", options.me) 
  trace(1, "mode = {}", options.mode) 
  trace(1, "username = {}", options.username) 
  trace(1, "password = {}", options.password) 
  trace(1, "parallel_sessions = {}", options.parallel_sessions) 
  trace(1, "path_block_file = {}", options.path_block_file) 
  trace(1, "path_state_file = {}", options.path_state_file) 
  trace(1, "retries = {}", options.retries) 
  trace(1, "rewind_minutes = {}", options.rewind_minutes) 
  trace(1, "source_loc = {}", options.source_loc) 
  trace(1, "state_directory = {}", options.state_directory) 
  trace(1, "state_file = {}", options.state_file) 
  trace(1, "target_dsn = {}", options.target_dsn)
  trace(1, "target_loc = {}", options.target_loc)
  trace(1, "target_table = {}", options.target_table)
  trace(1, "trace = {}", options.trace)
  trace(1, "ts_column = {}", options.ts_column)
  trace(1, "ts_high_watermark = {}", options.ts_high_watermark)
  trace(1, "ts_low_watermark = {}", options.ts_low_watermark)
  trace(1, "uri = {}", options.uri) 
  trace(1, "============================================")


def usage(extra):

    if extra is not None:
      print_message('')
      print_message(extra)
    print_message('')
    print_message("Usage: {} <mode> <chn> <loc> [userargs]", options.me)
    print_message('')
    print_message("        with <mode> <chn> <loc> in line with the use of an AgentPlugin (https://fivetran.com/docs/local-data-processing/action-reference/agentplugin)")
    print_message('')
    print_message("        [userargs] must be passed in as a single argument, enclosed in double quotes")
    print_message('')
    print_message("        mode is [prepare_channel|refresh|remove_block|reset_state]")
    print_message("             [prepare_channel]: to create the actions needed for the timestamp refresh based channel")
    print_message("             [refresh]: to submit a new incremental refresh (updating the watermarks)")
    print_message("             [remove_block]: to remove a block file so a refresh can run again")
    print_message("             [reset_state]: to reset the refresh state for the channel")
    # TODO requirement for loc should be removed as will not be used as AgentPlugin
    print_message("        <loc> is not used but must be provided when using for ldp")
    print_message('')
    print_message("        userargs")
    print_message('')
    print_message("        where userargs= [-c context [agentplugin|ldp]]")
    print_message("                                  [agentplugin] execute as AgentPlugin code")
    print_message("                                  [ldp] interact with LDP (HVR) - requires -h, -u, -p and -l")
    print_message("                        [-d DDL check] check table DDL; defaults no (optional in mode ldp)")
    print_message("                        [-e execution_context [" + options.context_initial + "," + options.context_incremental +"]] (mandatory for mode agentplugin)")
    print_message("                                  [" + options.context_initial + "] use for an initial load")
    print_message("                                  [" + options.context_incremental + "] use for an incremental load")
    print_message("                        [-h hub] (mandatory for mode ldp)")
    print_message("                        [-l URI (URL) to LDP] (mandatory for mode ldp)")
    print_message("                        [-p password] (mandatory for mode ldp)")
    print_message("                        [-P parallelism] parallelism for the refresh job (defaults to 6; optional for mode ldp)")
    print_message("                        [-C timestamp column] column used to find the max replicated timestamp on target (required with -D)")
    print_message("                        [-D target DSN] ODBC connection string to query max replicated timestamp from target")
    print_message("                        [-r source location (read)]")
    print_message("                        [-T target table] fully qualified target table (schema.table) to query for max timestamp (required with -D)")
    print_message("                        [-t trace level]")
    print_message("                        [-u username] (mandatory for mode ldp)")
    print_message("                        [-w target location (write)]")
    print_message('')
    print_message("        when using https traffic environment variable REQUESTS_CA_BUNDLE may need to be set and point to the file containing certificate authority.")
    print_message('')
    exit(1)


def get_options(argv):

  global options
  global ev_warn

  ev_warn = None
  options.me = os.path.basename(argv[0])

  if len(argv) < 4:
    usage('Missing mode,chn,loc arguments');
  elif len(argv) > 5:
    usage('Extra arguments after userargs argument')

  options.mode = argv[1]
  options.channel = argv[2]
  options.location = argv[3]
  userargs= []
  if len(argv) >= 5 and argv[4]:
    userargs= re.split(r'\s+', argv[4].strip())

  load_environment()

  if userargs:
    try:
      opts, args = getopt.getopt(userargs, 'c:d:e:h:l:p:P:r:t:u:w:C:D:T:')
    except getopt.GetoptError as e:
      usage(str(e))

    for (opt_key, opt_val) in opts:
      if opt_key == '-c':  
        options.context = opt_val
      elif opt_key == '-d':  
        options.ddl_check = opt_val
      elif opt_key == '-e':  
        options.execution_context = opt_val
      elif opt_key == '-h':  
        options.hub = opt_val
      elif opt_key == '-l':  
        options.uri = opt_val
      elif opt_key == '-p':  
        options.password = opt_val
      elif opt_key == '-P':  
        options.parallel_sessions = int(opt_val)
      elif opt_key == '-r':
        options.source_loc = opt_val
      elif opt_key == '-t':
        options.trace = int(opt_val)
      elif opt_key == '-u':  
        options.username = opt_val
      elif opt_key == '-w':
        options.target_loc = opt_val
      elif opt_key == '-C':
        options.ts_column = opt_val
      elif opt_key == '-D':
        options.target_dsn = opt_val
      elif opt_key == '-T':
        options.target_table = opt_val

  trace(2, "Arguments {} {} {} {}", options.mode, options.channel, options.location, userargs)
  if userargs and args:
    usage('extra userargs specified')

  options.state_directory = options.ehvr_config + "/ts_refresh/" + options.hub + "/" + options.channel
  options.state_file = options.channel + ".refr_state"
  options.path_block_file = options.state_directory + "/" + options.channel + ".block"
  options.path_state_file = options.state_directory + "/" + options.state_file


def validate_options():

  if not options.ehvr_config:
    usage("HVR_CONFIG is not defined; check the environment")
  if not options.context:
    usage("Context must be provided as agentplugin or ldp")
  if options.context not in ['agentplugin','ldp']:
    usage("Context must be provided as agentplugin or ldp")
  if options.context == 'ldp':
    if not options.username:
      usage("Username must be provided with context ldp")
    if not options.password:
      usage("Password is mandatory with context ldp")
    if not options.uri:
      usage("URL to LDP end point must be provided with context ldp")
    if options.mode not in ['refresh','prepare_channel','remove_block','reset_state']:
      usage('Missing valid mode: one of [refresh,prepare_channel,remove_block,reset_state]')
    if options.target_dsn and not options.ts_column:
      usage("-C (timestamp column) is required when -D (target DSN) is provided")
    if options.target_dsn and not options.target_table:
      usage("-T (target table) is required when -D (target DSN) is provided")


def read_json_from_file(filename):

  with open(filename, "r") as f:
    data = json.load(f)

  return data


def write_json_to_file(json_data, filename):

  with open(filename, "w") as f:
    f.write(json.dumps(json_data))

  return filename


##### Main functions #########################################################
def report(msg):

  print(msg)


def get_max_target_timestamp():
  """Query the target database for the maximum value of the timestamp column.

  Returns a datetime object if a non-NULL max is found, or None if the table
  is empty (e.g. after the very first load has not yet completed).
  Raises an exception on connection / query errors so the caller can decide
  whether to abort or fall back.
  """

  trace(1, "Querying target '{}' for MAX({}) FROM {}", options.target_dsn, options.ts_column, options.target_table)

  conn = pyodbc.connect(options.target_dsn)

  try:

    cursor = conn.cursor()
    cursor.execute("SELECT MAX({col}) FROM {tbl}".format(col=options.ts_column, tbl=options.target_table))
    row = cursor.fetchone()

  finally:

    conn.close()

  if row is None or row[0] is None:

    trace(1, "No rows found in target table '{}'; treating as empty", options.target_table)
    return None

  max_ts = row[0]

  # pyodbc returns datetime objects for datetime/datetime2 columns, but may
  # return a string for some drivers — normalise to datetime if needed.
  if isinstance(max_ts, str):
    max_ts = datetime.datetime.fromisoformat(max_ts)

  trace(1, "Max timestamp on target: {}", max_ts.isoformat())

  return max_ts


def ldp():

  try: 

    hvr_client = pyhvr.client(
        username = options.username
      , password = options.password
      , uri = options.uri
      )

  except pyhvr.PyhvrError as e:
    usage(e)

  if options.mode == 'refresh':

    jobs = hvr_client.get_hubs_jobs(hub = options.hub, channel = options.channel)

    for job in jobs:

      if job == options.channel + "-refr-" + options.source_loc + "-" + options.target_loc:

        state = jobs[job].get('state', None)

        if state == "FAILED" and os.path.isfile(options.path_block_file):

          print("Job " + job + " is in " + state + " state; deleting block file")
          os.remove(options.path_block_file)

        elif state == "RUNNING":

          raise Exception("Job is still running; a new job cannot yet be created")

        else:

          print("Job state is " + state)


  if os.path.isfile(options.path_block_file) and options.mode != 'remove_block':

    raise Exception("Per " + options.path_block_file + " a refresh is running; changes are not allowed at this time")

  else:

    if options.mode == 'refresh':

      if os.path.isfile(options.path_state_file):

        options.execution_context = options.context_incremental

      table_list = []

      tables = hvr_client.get_hubs_definition_channels_tables(hub = options.hub, channel = options.channel)

      for t in tables:
        table_list.append(t)

      if options.execution_context == options.context_incremental:

        if not os.path.isfile(options.path_state_file):

          report(options.me + " can only be used for refresh following the initial load that initialized the refresh state")
          exit(1)

        existing_state = read_json_from_file(options.path_state_file)

        if options.target_dsn:

          # Derive the low watermark from the actual max timestamp present on
          # the target.  This is the authoritative source of truth for what
          # has already been replicated, regardless of what was stored in the
          # state file from the previous run.
          max_target_ts = get_max_target_timestamp()

          if max_target_ts is None:

            # Target table exists but is empty — fall back to the state file
            # value so we do not widen the window unexpectedly.
            report("Target table appears empty; falling back to state-file high watermark for low watermark derivation")
            max_target_ts = datetime.datetime.fromisoformat(existing_state["high_watermark"])

          new_low_watermark = max_target_ts
          report("Low watermark derived from target MAX({col}): {ts}".format(
            col = options.ts_column,
            ts  = max_target_ts.isoformat(),
          ))

        else:

          # No target DSN provided — fall back to the previous behaviour of
          # using the high watermark stored in the state file.
          existing_high_watermark = datetime.datetime.fromisoformat(existing_state["high_watermark"])
          new_low_watermark = existing_high_watermark - datetime.timedelta(minutes = options.rewind_minutes)
          report("Low watermark derived from state-file high watermark (no target DSN configured)")

        new_high_watermark = options.ts_high_watermark

        #INCLUDE REFERENCES TO UPSERT CAPABILITY
        if options.ddl_check == 'yes':

          refresh_data = {
            "start_immediate": True,
            "source_loc": options.source_loc,
            "target_loc": options.target_loc,
            "tables": table_list,
            "create_tables": {
                "keep_structure": False,
                "force_recreate": False,
                "index": False,
                "keep_existing_data": False,
                "recreate_if_mismatch": True
              },
            "context_variables": {
               "ts_low_watermark": "'" + new_low_watermark.isoformat() + "'"
               },
            "granularity": "bulk",
            "upsert": True,
            "parallel_sessions": options.parallel_sessions,
            "contexts": [options.execution_context]
          }

        else:

          refresh_data = {
            "start_immediate": True,
            "source_loc": options.source_loc,
            "target_loc": options.target_loc,
            "tables": table_list,
            "context_variables": {
               "ts_low_watermark": "'" + new_low_watermark.isoformat() + "'"
               },
            "granularity": "bulk",
            "upsert": True,
            "parallel_sessions": options.parallel_sessions,
            "contexts": [options.execution_context]
          }

        new_state = {"low_watermark": new_low_watermark.isoformat(), "retries": options.retries}
        write_json_to_file(new_state, options.path_state_file)

      else:

        options.execution_context = options.context_initial

        if options.ddl_check == 'yes':

          refresh_data = {
            "start_immediate": True,
            "source_loc": options.source_loc,
            "target_loc": options.target_loc,
            "tables": table_list,
            "create_tables": {
                "keep_structure": False,
                "force_recreate": False,
                "index": False,
                "keep_existing_data": False,
                "recreate_if_mismatch": True
              },
            "granularity": "bulk",
            "parallel_sessions": options.parallel_sessions,
            "contexts": [options.execution_context]
          }

        else:

          refresh_data = {
            "start_immediate": True,
            "source_loc": options.source_loc,
            "target_loc": options.target_loc,
            "tables": table_list,
            "granularity": "bulk",
            "parallel_sessions": options.parallel_sessions, 
            "contexts": [options.execution_context]
          }

      refresh = hvr_client.post_hubs_channels_refresh( hub = options.hub
                                                     , channel = options.channel
                                                     , **refresh_data
                                                     )

    elif options.mode == 'remove_block':

      if os.path.isfile(options.path_block_file):

        os.remove(options.path_block_file)
        report("Removed " + options.path_block_file)

      else:

        report("Block file does not exist")

    elif options.mode == 'reset_state':

      if os.path.isfile(options.path_state_file):

        os.remove(options.path_state_file)
        report("Reset refresh state by removing " + options.path_state_file)

        if os.path.isfile(options.path_block_file):

          os.remove(options.path_block_file)
          report("Removed " + options.path_block_file)

      else:

        report("State reset not required because there is no existing state")

    elif options.mode == 'prepare_channel':

      actions = hvr_client.get_hubs_definition_channels_actions(hub = options.hub, channel = options.channel)

      pl = []

      for a in actions:

        property_type = a.get('type', None)
        scope = a.get('loc_scope', None)
        params = a.get('params')

        if (property_type == "ColumnProperties" and params.get('Name') == "hvr_rowid" and params.get('SurrogateKey') == 1):

          pl.append(a)

        if (property_type in ["ColumnProperties", "TableProperties", "Restrict"]) and (params.get('Context') in [options.context_initial,options.context_incremental,options.context_other]):

          pl.append(a)

      hvr_client.post_hubs_definition_channels_actions_delete(
          hub = options.hub
        , channel = options.channel
        , actions = pl
        )

      pl.clear()

      pl.append({'loc_scope': 'SOURCE', 'table_scope': '*', 'type': 'Restrict', 'params': {'RefreshCondition': "{hana_load_timestamp} <= now()", 'Context': options.context_initial}})
      pl.append({'loc_scope': 'SOURCE', 'table_scope': '*', 'type': 'Restrict', 'params': {'RefreshCondition': "{hana_load_timestamp} <= now()", 'Context': options.context_other}})

      pl.append({'loc_scope': 'SOURCE', 'table_scope': '*', 'type': 'Restrict', 'params': {'RefreshCondition': "{hana_load_timestamp} > {hvr_var_ts_low_watermark}", 'Context': options.context_incremental}})

      hvr_client.patch_hubs_definition_channels_actions(
          hub = options.hub
        , channel = options.channel
        , actions = pl
      )

    else:

      exit(1)


def main(argv):
  version_check()
  get_options(argv)
  validate_options()

  ldp()

  print_options()

if __name__ == "__main__":

  try:
    main(sys.argv)
    sys.stdout.flush() 
    sys.exit(0) 
  except Exception as err:
    sys.stdout.flush() 
    sys.stderr.write("F_JX0D01: {0}\n".format(err)) 
    sys.stderr.flush()
    sys.exit(1)

    # hvrrefresh -r sqlserver -l bq_fk -P4 -u -V'ts_low_watermark=2026-03-27T09:32:33.210000' -R https://34.118.14.4:4341 hvrhub sqls_ts_refr