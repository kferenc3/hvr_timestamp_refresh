#!/usr/bin/python3

import json
import getopt
import datetime
import os
import sys
import re
import pyhvr
import pyodbc
from time import sleep
from dotenv import load_dotenv

load_dotenv()

class Options:
  channel = ''
  context = ''
  context_initial = 'initial'
  context_incremental = 'incremental'
  # context_other = 'channel_change'
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
  trace = 2
  ts_column = ''
  ts_low_watermark = None
  uri = ''
  remove_block = 'no'
  verify = True

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

  execution_env = {}

  for k,v in os.environ.items():
    k = str(k)
    if k not in execution_env:
      execution_env[k] = str(v)

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
  if 'HVR_HUB' in evars:
    options.hub = evars['HVR_HUB']
  if 'HVR_URI' in evars:
    options.uri = evars['HVR_URI']
  if 'HVR_USERNAME' in evars:
    options.username = evars['HVR_USERNAME']
  if 'HVR_PASSWORD' in evars:
    options.password = evars['HVR_PASSWORD']
  if 'HVR_VERIFY_SSL' in evars:
    options.verify = False if evars['HVR_VERIFY_SSL'].lower() == 'false' else True

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
  # trace(1, "context = {}", options.context)
  trace(1, "context_initial = {}", options.context_initial)
  trace(1, "context_incremental = {}", options.context_incremental)
  # trace(1, "context_other = {}", options.context_other)
  trace(1, "ddl_check = {}", options.ddl_check)
  trace(1, "ehvr_config = {}", options.ehvr_config) 
  trace(1, "execution_context = {}", options.execution_context) 
  trace(1, "hana_timezone = {}", options.hana_timezone) 
  trace(1, "hub = {}", options.hub) 
  # trace(1, "location = {}", options.location) 
  trace(1, "me = {}", options.me) 
  trace(1, "mode = {}", options.mode) 
  trace(1, "username = {}", options.username) 
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
  # trace(1, "ts_high_watermark = {}", options.ts_high_watermark)
  trace(1, "ts_low_watermark = {}", options.ts_low_watermark)
  trace(1, "uri = {}", options.uri)
  trace(1, "remove_block = {}", options.remove_block)
  trace(1, "============================================")

def usage(extra):

    if extra is not None:
      print_message('')
      print_message(extra)
    print_message('')
    print_message("Usage: {} <mode> <chn> [userargs]", options.me)
    print_message('')
    print_message("        with <mode> <chn> identifying the channel to operate on, and [userargs] providing additional arguments as needed for the different modes")
    print_message('')
    print_message("        [userargs] must be passed in as a single argument, enclosed in double quotes")
    print_message('')
    print_message("        mode is [dry_run|refresh]")
    print_message("             [dry_run]: No changes are made and no jobs are created; the tool just outputs the actions it would take based on the existing state and the provided arguments")
    print_message("             [refresh]: Modify the channel with a Restrict action if needed and create a refresh job with the extracted watermark from target")
    print_message('')
    print_message("        userargs")
    print_message('')
    print_message("        where userargs= ")
    print_message("                        [-d DDL check] check table DDL [yes|no]; defaults no (optional)")
    print_message("                        [-e execution_context [" + options.context_initial + "," + options.context_incremental +"]]")
    print_message("                                  [" + options.context_initial + "] use for an initial load")
    print_message("                                  [" + options.context_incremental + "] use for an incremental load")
    print_message("                        [-p parallelism] parallelism for the refresh job (defaults to 6; optional)")
    print_message("                        [-C timestamp column] column used to find the max replicated timestamp on target (required with -D)")
    print_message("                        [-D target DSN] ODBC connection string to query max replicated timestamp from target")
    print_message("                        [-r source location (read)]")
    print_message("                        [-T target table] fully qualified target table (schema.table) to query for max timestamp (required with -D)")
    print_message("                        [-t trace level]")
    print_message("                        [-u username] (mandatory for mode ldp)")
    print_message("                        [-b remove block] remove block file to unblock refreshes after a failure [yes|no]; defaults no (optional)")
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

  if len(argv) < 3:
    usage('Missing mode,chn arguments');
  elif len(argv) > 4:
    usage('Extra arguments after userargs argument')

  options.mode = argv[1]
  options.channel = argv[2]
  userargs= []
  if len(argv) >= 4 and argv[3]:
    userargs= re.split(r'\s+', argv[3].strip())

  load_environment()

  if userargs:
    try:
      # opts, args = getopt.getopt(userargs, 'c:d:e:h:l:p:P:r:t:u:w:C:D:T:')
      opts, args = getopt.getopt(userargs, 'd:e:p:r:t:w:C:D:T:b:')
    except getopt.GetoptError as e:
      usage(str(e))

    for (opt_key, opt_val) in opts:
      if opt_key == '-d':  
        options.ddl_check = opt_val
      elif opt_key == '-e':  
        options.execution_context = opt_val
      elif opt_key == '-p':  
        options.parallel_sessions = int(opt_val)
      elif opt_key == '-r':
        options.source_loc = opt_val
      elif opt_key == '-t':
        options.trace = int(opt_val)
      elif opt_key == '-w':
        options.target_loc = opt_val
      elif opt_key == '-C':
        options.ts_column = opt_val
      elif opt_key == '-D':
        options.target_dsn = opt_val
      elif opt_key == '-T':
        options.target_table = opt_val
      elif opt_key == '-b':
        options.remove_block = True if opt_val == 'yes' else False

  trace(2, "Arguments {} {} {}", options.mode, options.channel, userargs)
  if userargs and args:
    usage('extra userargs specified')

  options.state_directory = options.ehvr_config + "/ts_refresh/" + options.hub + "/" + options.channel
  options.state_file = options.channel + ".refr_state"
  options.path_block_file = options.state_directory + "/" + options.channel + ".block"
  options.path_state_file = options.state_directory + "/" + options.state_file
  print(os.path.dirname(options.path_block_file))
  os.makedirs(os.path.dirname(options.path_block_file), exist_ok=True)

def validate_options():

  if not options.ehvr_config:
    usage("HVR_CONFIG is not defined; check the environment")
  if not options.username:
    usage("Username must be provided")
  if not options.password:
    usage("Password is mandatory")
  if not options.uri:
    usage("URL to LDP end point must be provided")
  if options.mode not in ['refresh','dry_run']:
    usage('Missing valid mode: one of [refresh, dry_run]')
  if options.target_dsn and not options.ts_column:
    usage("-C (timestamp column) is required when -D (target DSN) is provided")
  if options.target_dsn and not options.target_table:
    usage("-T (target table) is required when -D (target DSN) is provided")

def read_json_from_file(filename):

  with open(filename, "r") as f:
    data = json.load(f)

  return data

def write_json_to_file(json_data, filename):

  with open(filename, "w+") as f:
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

  conn = pyodbc.connect(f"DSN={options.target_dsn}", autocommit=True)

  try:

    cursor = conn.cursor()
    cursor.execute("SELECT MAX({col}) FROM {tbl}".format(col=options.ts_column, tbl=options.target_table))
    row = cursor.fetchone()
  except pyodbc.Error as e:
    sqlstate = e.args[0]
    if sqlstate == '42S02':
        trace(1, "Target table '{}' not found; treating as empty", options.target_table)
        options.ddl_check = 'yes'
        row = None
    else:
        raise 

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

  max_ts = max_ts + datetime.timedelta(microseconds=1)
  trace(1, "Max timestamp on target: {}", max_ts.isoformat())

  return max_ts

def ldp():

  try: 

    hvr_client = pyhvr.client(
        username = options.username
      , password = options.password
      , uri = options.uri
      , verify = options.verify
      )

  except Exception as e:
    usage(e)

  jobs = hvr_client.get_hubs_jobs(hub = options.hub, channel = options.channel)

  for job in jobs:
    if job == options.channel + "-refr-" + options.source_loc + "-" + options.target_loc:

      state = jobs[job].get('state', None)
      if options.mode != 'dry_run':
        
        if state == "FAILED" and os.path.isfile(options.path_block_file):
          print("Job " + job + " is in " + state + " state; deleting block file")
          os.remove(options.path_block_file)
        elif state == "RUNNING":
          raise Exception("Job is still running; a new job cannot yet be created")
        else:
          print("Job state is " + state)
      
      else:
        print("Job state is " + state)


  if os.path.isfile(options.path_block_file) and not options.remove_block:

    if options.mode == 'refresh':
      raise Exception("Per " + options.path_block_file + " a refresh is running; changes are not allowed at this time")
    else:
      print(f"Per {options.path_block_file} a refresh is running and remove block option is set to {options.remove_block}")
  
  if os.path.isfile(options.path_block_file) and options.remove_block:
    if options.mode == 'refresh':
      os.remove(options.path_block_file)
      report("Removed " + options.path_block_file)
    else:
      report(f"Block file {options.path_block_file} exists and remove block option is set to {options.remove_block}")

    
    # 1. Prepare the channel:
    #  - Delete any existing Restrict actions on the channel for the relevant contexts to ensure a clean slate, and then add new Restrict actions with the appropriate conditions based on the timestamp variable for the initial and incremental contexts

  actions = hvr_client.get_hubs_definition_channels_actions(hub = options.hub, channel = options.channel)

  pl = []

  for a in actions:

    property_type = a.get('type', None)
    scope = a.get('loc_scope', None)
    params = a.get('params')

    if (property_type in ["Restrict"]) and (params.get('Context') in [options.context_initial,options.context_incremental]):

      pl.append(a)

  if options.mode != 'dry_run':
    hvr_client.post_hubs_definition_channels_actions_delete(
        hub = options.hub
      , channel = options.channel
      , actions = pl
      )
  else:
    report("Dry run mode: the following actions would be deleted from the channel:")
    for a in pl:
      report(a)

  pl.clear()

  # pl.append({'loc_scope': 'SOURCE', 'table_scope': '*', 'type': 'Restrict', 'params': {'RefreshCondition': "{hana_load_timestamp} <= now()", 'Context': options.context_incremental}})
  # pl.append({'loc_scope': 'SOURCE', 'table_scope': '*', 'type': 'Restrict', 'params': {'RefreshCondition': "{hana_load_timestamp} <= now()", 'Context': options.context_other}})

  pl.append({'loc_scope': 'SOURCE', 'table_scope': '*', 'type': 'Restrict', 'params': {'RefreshCondition': f"{options.ts_column} > {{hvr_var_ts_low_watermark}}", 'Context': options.context_incremental}})

  if options.mode != 'dry_run':
    hvr_client.patch_hubs_definition_channels_actions(
        hub = options.hub
      , channel = options.channel
      , actions = pl
    )
  else:
    report("Dry run mode: the following actions would be added to the channel:")
    for a in pl:
      report(a)
  
  # 2. Check state file:
    #  - if exists compare with the extracted ts, raise a warning if mismatched, mode: incremental
    #  - if it does not exist, mode: initial 
  if os.path.isfile(options.path_state_file):
      options.execution_context = options.context_incremental
      existing_state = read_json_from_file(options.path_state_file)
      existing_low_watermark = datetime.datetime.fromisoformat(existing_state["low_watermark"])
  else:
      options.execution_context = options.context_initial
      existing_state = {"low_watermark": '1900-01-01T00:00:00', "retries": options.retries}
      existing_low_watermark = None

  table_list = []

  tables = hvr_client.get_hubs_definition_channels_tables(hub = options.hub, channel = options.channel)

  for t in tables:
    table_list.append(t)

  if options.target_dsn:

    # Derive the low watermark from the actual max timestamp present on
    # the target.  This is the authoritative source of truth for what
    # has already been replicated, regardless of what was stored in the
    # state file from the previous run.
    new_low_watermark = get_max_target_timestamp()

    if new_low_watermark is None:

      # Target table exists but is empty, fall back to initial load
      options.execution_context = options.context_initial
      report("Target table appears empty; starting initial load.")
      if existing_low_watermark:  
        report("Existing low watermark from state file: {ts}".format(
          ts = existing_low_watermark.isoformat(),
        ))
      new_low_watermark = datetime.datetime.fromisoformat(existing_state["low_watermark"])

    
    report("Low watermark from target MAX({col}): {ts}".format(
      col = options.ts_column,
      ts  = new_low_watermark.isoformat(),
    ))

    if existing_low_watermark and new_low_watermark != existing_low_watermark:
      report("WARNING: Low watermark from target does not match existing low watermark from state file: {ts}".format(
        ts = existing_low_watermark.isoformat(),
      ))

  else:
    usage("Target DSN must be provided to query for max timestamp on target")
    exit(1)

  # 3. Create refresh job with the appropriate context
  if options.execution_context == options.context_incremental:
    if options.ddl_check == 'yes':

      refresh_data = {
        "start_immediate": True,
        "source_loc": options.source_loc,
        "target_loc": options.target_loc,
        "tables": table_list,
        "create_tables": {
            "keep_structure": False,
            "force_recreate": False,
            "index": True,
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
        "upsert": True,
        "parallel_sessions": options.parallel_sessions
      }
    else:

      refresh_data = {
        "start_immediate": True,
        "source_loc": options.source_loc,
        "target_loc": options.target_loc,
        "tables": table_list,
        "granularity": "bulk",
        "upsert": True,
        "parallel_sessions": options.parallel_sessions
      }

  # 4. Trigger the refresh
  if options.mode != 'dry_run':
    try:
      refresh = hvr_client.post_hubs_channels_refresh( hub = options.hub
                                            , channel = options.channel
                                            , **refresh_data
                                            )
      # Create a block file to prevent further refresh attempts until the job completes
      with open(options.path_block_file, "w+") as f:
        f.write("Refresh job created with watermark {wm} and is in progress; block file created to prevent further refresh attempts until the job completes".format(wm=new_low_watermark.isoformat()))
      report("Created block file " + options.path_block_file + " to prevent further refresh attempts until the job completes")
    except Exception as e:
      # Create a block file to prevent further refresh attempts until the issue is investigated and resolved
      with open(options.path_block_file, "w") as f:
        f.write("Refresh job creation failed with error: {err}".format(err=str(e)))
      report("Refresh job creation failed with error: {err}. Created block file {bf} to prevent further refresh attempts until the issue is investigated and resolved.".format(err=str(e), bf=options.path_block_file))
      raise e
  else:
    report("Dry run mode: the following refresh job would be created with the above channel changes:")
    report(refresh_data)
    refresh = None

  # 5. Poll the api for job status until completion, and if the job fails create a block file to prevent further refresh attempts until the issue is investigated and resolved; 
    #  If the job succeeds remove any existing block file and update the state file with the new watermark extracted from target
  refr_job = refresh.get('job', None)
  if refr_job:
    check_count = 3
    while check_count > 0:
      check_count -= 1
      job = hvr_client.get_hubs_jobs(hub = options.hub, channel = options.channel, job = refr_job)
      job_status = job[refr_job].get('state', None)
      

      if job_status in ['PENDING']:
        break

      elif job_status in ['FAILED', 'RETRY']:
        with open(options.path_block_file, "w") as f:
          f.write("Refresh job failed with status " + job_status + "; block file created to prevent further refresh attempts until the issue is investigated and resolved")
        report("Refresh job failed with status " + job_status + "; created block file " + options.path_block_file + " to prevent further refresh attempts until the issue is investigated and resolved")
        exit(1)

      trace(1, "Job {} is still in {} state; sleeping for 30 seconds before checking again", refr_job, job_status)
      sleep(30)

  # Once the job is completed, update the state file with the new low watermark from target, and remove any existing block file to unblock future refreshes
  new_state = {"low_watermark": get_max_target_timestamp().isoformat(), "retries": options.retries}
  write_json_to_file(new_state, options.path_state_file)

  if os.path.isfile(options.path_block_file):
        os.remove(options.path_block_file)
        report("Removed existing block file " + options.path_block_file + " as job is in " + job_status + " state and completed successfully")
  
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
