from typing import Dict, List, Tuple, Set, Any
import itertools
import operator
from dataclasses import dataclass, asdict
from pathlib import Path
import shutil
from datetime import datetime
import pprint # noqa: F401
import psutil
import math
from contextlib import nullcontext # For optional file writing

from sphenixprodrules import RuleConfig, InputConfig
from sphenixprodrules import pRUNFMT,pSEGFMT
from sphenixdbutils import cnxn_string_map, dbQuery, list_to_condition
from simpleLogger import CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401
from sphenixjobdicts import inputs_from_output
from sphenixmisc import binary_contains_bisect, shell_command

from collections import namedtuple
FileHostRunSegStat = namedtuple('FileHostRunSeg',['filename','daqhost','runnumber','segment','status'])

""" This file contains the classes for matching runs and files to a rule.
    MatchConfig is the steering class for db queries to
    find appropriate input files and name the output files.
    It is constructed from a RuleConfig object.
"""

# Striving to keep Dataclasses immutable (frozen=True)
# All modifications should be done in the constructor

# ============================================================================

@dataclass( frozen = True )
class MatchConfig:
    dsttype:        str
    runlist_int:    str
    input_config:   InputConfig
    dataset:        str
    outtriplet:     str
    physicsmode:    str
    filesystem:     Dict
    rungroup_tmpl:  str

    # Internal, derived variables
    dst_type_template: str
    in_types:          Any # Fixme, should always be List[str]
    input_stem:        Any
    # ------------------------------------------------
    @classmethod
    def from_rule_config(cls, rule_config: RuleConfig):
        """
        Constructs a MatchConfig object partially from a RuleConfig object.

        Args:
            rule_config: The RuleConfig object to extract data from.

        Returns:
            A MatchConfig object with fields pre-populated from the RuleConfig.
        """

        dsttype       = rule_config.dsttype
        runlist_int   = rule_config.runlist_int
        input_config  = rule_config.input_config
        dataset       = rule_config.dataset
        outtriplet    = rule_config.outtriplet
        physicsmode   = rule_config.physicsmode
        filesystem    = rule_config.job_config.filesystem
        rungroup_tmpl = rule_config.job_config.rungroup_tmpl
        
        ## derived
        dst_type_template = f'{dsttype}'
        # This test should be equivalent to if 'raw' in input_config.db
        if 'TRIGGERED' in dsttype or 'STREAMING' in dsttype:
            dst_type_template += '_%'
            dst_type_template += '%'

        ### Assemble leafs, where needed
        input_stem = inputs_from_output[dsttype]
        CHATTY( f'Input files are of the form:\n{pprint.pformat(input_stem)}')
        if isinstance(input_stem, dict):
            in_types = list(input_stem.values())
        else :
            in_types = input_stem

        return cls(
            dsttype       = dsttype,
            runlist_int   = runlist_int,
            input_config  = input_config,
            dataset       = dataset,
            outtriplet    = outtriplet,
            physicsmode   = physicsmode,
            filesystem    = filesystem,
            rungroup_tmpl = rungroup_tmpl,
            ## derived
            dst_type_template = dst_type_template,
            in_types=in_types,
            input_stem=input_stem,
        )

    # ------------------------------------------------
    def dict(self):
        return { k: str(v) for k, v in asdict(self).items() if v is not None }

    # ------------------------------------------------
    def good_runlist(self) -> List[int]:
        ### Run quality
        CHATTY(f"Resident Memory: {psutil.Process().memory_info().rss / 1024 / 1024:.0f} MB")
        # Here would be a  good spot to check against golden or bad runlists and to enforce quality cuts on the runs

        INFO("Checking runlist against run quality cuts.")
        run_quality_tmpl="""
select distinct(runnumber) from run
 where
runnumber>={runmin} and runnumber <= {runmax}
 and
runtype='{physicsmode}'
 and
eventsinrun >= {min_run_events}
 and
EXTRACT(EPOCH FROM (ertimestamp-brtimestamp)) >={min_run_time}
order by runnumber
;
"""
        run_quality_query=run_quality_tmpl.format(
            runmin=min(self.runlist_int),
            runmax=max(self.runlist_int),
            physicsmode=self.physicsmode,
            min_run_events=self.input_config.min_run_events,
            min_run_time=self.input_config.min_run_time,
        )
        goodruns=[ int(r) for (r,) in dbQuery( cnxn_string_map['daqr'], run_quality_query).fetchall() ]
        # tighten run condition now
        runlist_int=[ run for run in self.runlist_int if run in goodruns ]
        if runlist_int==[]:
            return []
        INFO(f"{len(runlist_int)} runs pass run quality cuts.")
        DEBUG(f"Runlist: {runlist_int}")
        return runlist_int

    # ------------------------------------------------
    def get_files_in_db(self, runnumbers: Any) :

        exist_query  = f"""select filename from datasets
        where tag='{self.outtriplet}'
        and dataset='{self.dataset}'
        and dsttype like '{self.dst_type_template}'"""

        run_condition=list_to_condition(runnumbers)
        if run_condition!="" :
            exist_query += f"\n\tand {run_condition}"
        existing_output = [ c.filename for c in dbQuery( cnxn_string_map['fcr'], exist_query ) ]
        existing_output.sort()
        return existing_output

    # ------------------------------------------------
    def get_output_files(self, filemask: str = r"\*.root:\*", dstlistname: str=None, dryrun: bool=True) -> List[str]:
        ### Which find command to use for lustre?
        find=shutil.which('find')
        lfind = shutil.which('lfs')
        if lfind is None:
            WARN("'lfs find' not found")
            lfind = shutil.which('find')
        else:
            lfind = f'{lfind} find'
            INFO(f'Using find={find} and lfind="{lfind}.')

        if dstlistname:
            INFO(f"Piping output to {dstlistname}")
            if not dryrun:
                Path(dstlistname).unlink(missing_ok=True)
            else:
                dstlistname="/dev/null"
                INFO(f"Dryrun. Piping output to {dstlistname}")

        outlocation=self.filesystem['outdir']
        # Further down, we will simplify by assuming finaldir == outdir, otherwise this script shouldn't be used.
        finaldir=self.filesystem['finaldir']
        if finaldir != outlocation:
            ERROR("Found finaldir != outdir. Use/adapt dstlakespider instead." )
            print(f"finaldir = {finaldir}")
            print(f"outdir = {outlocation}")
            exit(1)
        INFO(f"Directory tree: {outlocation}")

        # All leafs:
        leafparent=outlocation.split('/{leafdir}')[0]
        leafdirs_cmd=rf"{find} {leafparent} -type d -name {self.dsttype}\* -mindepth 1 -a -maxdepth 1"
        leafdirs = shell_command(leafdirs_cmd)
        CHATTY(f"Leaf directories: \n{pprint.pformat(leafdirs)}")

        # Run groups that we're interested in
        sorted_runlist = sorted(self.runlist_int)
        def rungroup(run):
            return self.rungroup_tmpl.format(a=100*math.floor(run/100), b=100*math.ceil((run+1)/100))
        desirable_rungroups = { rungroup(run) for run in sorted_runlist }
        runs_by_group = { group : [] for group in desirable_rungroups}
        outidentifier=f'{self.dataset}_{self.outtriplet}'
        for run in sorted_runlist:
            # runs_by_group[rungroup(run)].append(str(run))
            runstr=f'{outidentifier}-{run:{pRUNFMT}}'
            ## could also add segment, runstr+=f'-{segment:{pSEGFMT}}'
            runs_by_group[rungroup(run)].append(runstr)

        # INFO(f"Size of the filter dictionary is {sys.getsizeof(runs_by_group)} bytes")
        # INFO(f"Length of the filter dictionary is {len(runs_by_group.keys())}")
        # INFO(f"Size of one entry is {sys.getsizeof(runs_by_group['run_00072000_00072100'])} bytes")
        # INFO(f"Size of one string is {sys.getsizeof(runs_by_group['run_00072000_00072100'][0])} bytes")
        ## --> Negligible. << 1MB

        ### Walk through leafs - assume rungroups may change between run groups
        ret=[]

        tstart=datetime.now()
        with open(dstlistname,"w") if dstlistname else nullcontext() as dstlistfile:
            for leafdir in leafdirs :
                CHATTY(f"Searching {leafdir}")
                available_rungroups = shell_command(rf"{find} {leafdir} -name run_\* -type d -mindepth 1 -a -maxdepth 1")
                DEBUG(f"Resident Memory: {psutil.Process().memory_info().rss / 1024 / 1024:.0f} MB")
                
                # Want to have the subset of available rungroups where a desirable rungroup is a substring (cause the former have the full path)
                rungroups = {rg for rg in available_rungroups if any( drg in rg for drg in desirable_rungroups) }
                DEBUG(f"For {leafdir}, we have {len(rungroups)} run groups to work on")                
                for rungroup in rungroups:
                    runs_str=runs_by_group[Path(rungroup).name]
                    find_command=f"{lfind} {rungroup} -type f -name {filemask}"
                    CHATTY(find_command)
                    group_runs = shell_command(find_command)
                    # Enforce run number constraint
                    group_runs = [ run for run in group_runs if any( dr in run for dr in runs_str) ]
                    if dstlistfile:
                        for run in group_runs:
                            dstlistfile.write(f"{run}\n")
                    else:
                        ret += group_runs
        INFO(f"List creation took {(datetime.now() - tstart).total_seconds():.2f} seconds.")
        return ret

    # ------------------------------------------------
    def get_prod_status(self, runnumbers):
        ### Check production status
        INFO('Checking for output already in production...')
        status_query  = f"""select dstfile,status from production_status
        where dstname like '%{self.dst_type_template}%'
        and dstname like '%{self.outtriplet}%'"""

        run_condition=list_to_condition(runnumbers)
        if run_condition!="" :
            status_query += f"\n\tand {run_condition.replace('runnumber','run')}"

        status_query += self.input_config.status_query_constraints
        existing_status = { c.dstfile : c.status for c in dbQuery( cnxn_string_map['statr'], status_query ) }
        return existing_status

    # ------------------------------------------------
    def daqhosts_for_combining(self) -> Dict[int, Set[int]]:
        ### Which DAQ hosts have all required segments present in the file catalog for a given run?

        # Run quality:
        goodruns=self.good_runlist()
        if goodruns==[]:
            INFO( "No runs pass run quality cuts.")
            return {}
        INFO(f"{len(goodruns)} runs pass run quality cuts.")
        DEBUG(f"Runlist: {goodruns}")
        if goodruns==[]:
            return {}
        run_condition=list_to_condition(goodruns)

        # If we only care about segment 0, we can skip a lot of the checks
        if self.input_config.combine_seg0_only:
            INFO("Only combining segment 0. Skipping detailed checks.")
            
            # Which hosts have a segment 0 in the file catalog?
            lustre_query =   "select runnumber,daqhost from datasets"
            lustre_query += f" WHERE {run_condition}"
            lustre_query += f" AND daqhost in {tuple(self.in_types)}"
            lustre_query += f" AND segment=0 AND status::int > 0;"
            lustre_result = dbQuery( cnxn_string_map[ self.input_config.db ], lustre_query ).fetchall()
            daqhosts_for_combining = {}
            for r,h in lustre_result:
                if r not in daqhosts_for_combining:
                    daqhosts_for_combining[r] = set()
                daqhosts_for_combining[r].add(h)
            for run in daqhosts_for_combining:
                CHATTY(f"Available on lustre for run {run}: {daqhosts_for_combining.get(run,set())}")

            return daqhosts_for_combining

        ### More general case, need to check all segments
        # How many segments were produced per daqhost?
        seg_query=   "select runnumber,hostname,count(sequence) from filelist"
        seg_query+= f" WHERE {run_condition}"
        seg_query+= f" and hostname in {tuple(self.in_types)}"
        seg_query+=  " group by runnumber,hostname;"
        seg_result = dbQuery( cnxn_string_map['daqr'], seg_query ).fetchall()
        run_segs = {}
        for r,h,s in seg_result:
            if r not in run_segs:
                run_segs[r] = {}
            run_segs[r][h] = s

        ### How many segments are actually present in the file catalog?
        lustre_query =   "select runnumber,daqhost,count(status) from datasets"
        lustre_query += f" where {run_condition}"
        lustre_query += f" and daqhost in {tuple(self.in_types)}"
        lustre_query += f" and status::int > 0"
        lustre_query +=  " group by runnumber,daqhost;"
        lustre_result = dbQuery( cnxn_string_map[ self.input_config.db ], lustre_query ).fetchall()
        lustre_segs = {}
        for r,h,s in lustre_result:
            if r not in lustre_segs:
                lustre_segs[r] = {}
            lustre_segs[r][h] = s
        
        ## Now compare the two and decide which runs to use
        ## For a given host, all segments must be present
        daqhosts_for_combining = {}
        for r, hosts in run_segs.items():
            WARN(f"Produced segments for run {r}: {hosts}")
            if r in lustre_segs:
                WARN(f"Available on lustre for run {r}: {lustre_segs[r]}")
                for h, s in hosts.items():
                    if h in lustre_segs[r]:
                        if s == lustre_segs[r][h]:
                            daqhosts_for_combining[r] = daqhosts_for_combining.get(r, set())
                            daqhosts_for_combining[r].add(h)
                            CHATTY(f"Run {r} has all {s} segments for host {h}. Using this host.")
                        else:
                            CHATTY(f"Run {r} host {h} has only {lustre_segs[r][h]} out of {s} segments on lustre. Not using this host.")

        return daqhosts_for_combining

    # ------------------------------------------------
    def devmatches(self) :
        ### Match parameters are set, now build up the list of inputs and construct corresponding output file names
        # The logic for combination and downstream jobs is sufficiently different to warrant separate functions
        start=datetime.now()
        if 'raw' in self.input_config.db:
            rule_matches = {}
            segswitch="seg0fromdb"
            if not self.input_config.combine_seg0_only:
                segswitch="allsegsfromdb"
            daqhosts_for_combining = self.daqhosts_for_combining()
            if daqhosts_for_combining=={}:
                WARN("No runs satisfy the segment availability criteria. No jobs to submit.")
                return {}
            INFO(f"{len(daqhosts_for_combining)} runs satisfy the segment availability criteria.")

            ## Now check against production status and existing files
            for runnumber in daqhosts_for_combining:
                existing_output=self.get_files_in_db(runnumber)
                if existing_output==[]:
                    DEBUG(f"No output files yet for run {runnumber}")
                else:
                    DEBUG(f"Already have {len(existing_output)} output files for run {runnumber}")

                existing_status=self.get_prod_status(runnumber)
                if existing_status=={}:
                    DEBUG(f"No output files yet in the production db for run {runnumber}")
                else:   
                    DEBUG(f"Already have {len(existing_status)} output files in the production db")

                for leaf, daqhost in self.input_stem.items():
                    if daqhost not in daqhosts_for_combining[runnumber]:
                        CHATTY(f"No inputs from {daqhost} for run {runnumber}.")
                        continue
                    # We still could explicitly query the input files from the db here, but we already know that all segments are present
                    dsttype  = f'{self.dsttype}_{leaf}'
                    dsttype += f'_{self.dataset}'
                    outbase=f'{dsttype}_{self.outtriplet}'
                    # For combining, use segment 0 as key for logs and for existing output
                    logbase=f'{outbase}-{runnumber:{pRUNFMT}}-{0:{pSEGFMT}}'
                    dstfile=f'{logbase}.root'
                    if dstfile in existing_output:
                        CHATTY(f"Output file {dstfile} already exists. Not submitting.")
                        continue

                    if dstfile in existing_status:
                        WARN(f"Output file {dstfile} already has production status {existing_status[dstfile]}. Not submitting.")
                        continue

                    # DEBUG(f"Creating {dstfile} for run {runnumber} with {len(files_for_run[daqhost])} input segments")
                    DEBUG(f"Creating {dstfile} for run {runnumber}.")

                    rule_matches[dstfile] = [segswitch], outbase, logbase, runnumber, 0, daqhost, self.dsttype+'_'+leaf

            INFO(f'[Parsing time ] {(datetime.now() - start).total_seconds():.2f} seconds')
            return rule_matches
        else:
            return self.matches()

    # ------------------------------------------------
    def matches(self) :
        ### Match parameters are set, now build up the list of inputs and construct corresponding output file names
        # Despite the "like" clause, this is a fast query. Extra cuts or substitute cuts like
        # 'and runnumber>={self.runMin} and runnumber<={self.runMax}'
        # can be added if the need arises.
        # Note: If the file database is not up to date, we can use a filesystem search in the output directory
        # Note: The db field in the yaml is for input queries only, all output queries go to the FileCatalog

        # TODO: Move this query and use it only for combination jobs
        goodruns=self.good_runlist()
        if goodruns==[]:
            INFO( "No runs pass run quality cuts.")
            return {}
        INFO(f"{len(goodruns)} runs pass run quality cuts.")
        DEBUG(f"Runlist: {goodruns}")

        ####################################################################################
        ###### Now get all existing input files
        ####################################################################################
        # TODO: Support rule.printquery

        # Manipulate the input types to match the database
        in_types=self.in_types # local copy, member is frozen
        if 'raw' in self.input_config.db:
            descriminator='daqhost'
            in_types.insert(0,'gl1daq') # all raw daq files need an extra GL1 file
        else:
            descriminator='dsttype'
        # Transform list to ('<v1>','<v2>', ...) format. (one-liner doesn't work in python 3.9)
        in_types_str = f'( QUOTE{"QUOTE,QUOTE".join(in_types)}QUOTE )'
        in_types_str = in_types_str.replace("QUOTE","'")

        # Need status==1 for all files in a given run,host combination
        # Easier to check that after the SQL query
        infile_query = f"""select filename,{descriminator} as daqhost,runnumber,segment,status
        from {self.input_config.table}
        where \n\t{descriminator} in {in_types_str}\n
        """
        intriplet=self.input_config.intriplet
        if intriplet and intriplet!="":
            infile_query+=f"\tand tag='{intriplet}'"
        if 'raw' in self.input_config.db:
            infile_query+= f" and dataset='{self.physicsmode}'"
        else:
            infile_query=infile_query.replace('status','\'1\' as status')
        infile_query += self.input_config.infile_query_constraints
        # Keeping the run condition as a fallback; it should never matter though
        run_condition=list_to_condition(goodruns)
        if run_condition!="" :
            infile_query += f"\n\tand {run_condition}"
        # Perform queries inside the run loop. More db reads but much smaller RAM

        #### Now build up potential output files from what's available
        start=datetime.now()
        rule_matches = {}

        ### Runnumber is the prime differentiator
        INFO(f"Resident Memory: {psutil.Process().memory_info().rss / 1024 / 1024} MB")
        for runnumber in goodruns:
            # Files to be created are checked against this list. Could use various attributes but most straightforward is just the filename
            ## Note: Not all constraints are needed, but they may speed up the query
            existing_output=self.get_files_in_db(runnumber)
            if existing_output==[]:
                DEBUG(f"No output files yet for run {runnumber}")
            else:
                DEBUG(f"Already have {len(existing_output)} output files for run {runnumber}")

            existing_status=self.get_prod_status(runnumber)
            if existing_status=={}:
                DEBUG(f"No output files yet in the production db for run {runnumber}")
            else:   
                DEBUG(f"Already have {len(existing_status)} output files in the production db")

            # Potential input files for this run
            run_query = infile_query + f"\n\t and runnumber={runnumber} "
            CHATTY(f"run_query:\n{run_query}")
            exit()
            db_result = dbQuery( cnxn_string_map[ self.input_config.db ], run_query ).fetchall()
            candidates = [ FileHostRunSegStat(c.filename,c.daqhost,c.runnumber,c.segment,c.status) for c in db_result ]
            CHATTY(f"Run: {runnumber}, Resident Memory: {psutil.Process().memory_info().rss / 1024 / 1024} MB")
            if len(candidates) == 0 :
                # # By construction of runlist, every runnumber now should have at least one file
                # TODO: No longer true, check
                # ERROR(f"No input files found for run {runnumber}. That should not happen at this point. Skipping run.")
                DEBUG(f"No input files found for run {runnumber}. Skipping run.")
                continue
            DEBUG(f"Found {len(candidates)} input files for run {runnumber}.")

            ### Simplest case, 1-to-1:For every segment, there is exactly one output file, and exactly one input file from the previous step
            # If the output doesn't exist yet, use input files to create the job
            # TODO: or 'CALOFITTING' or many other job types
            if 'TRKR_SEED' in self.dsttype:
                for infile in candidates:
                    outbase=f'{self.dsttype}_{self.dataset}_{self.outtriplet}'
                    logbase= f'{outbase}-{infile.runnumber:{pRUNFMT}}-{infile.segment:{pSEGFMT}}'
                    dstfile = f'{logbase}.root'
                    if binary_contains_bisect(existing_output,dstfile):
                        CHATTY(f"Output file {dstfile} already exists. Not submitting.")
                        continue
                    if dstfile in existing_status:
                        WARN(f"Production status of {dstfile} is {existing_status[dstfile]}. Not submitting.")
                        continue
                    in_files_for_seg=[infile]
                    CHATTY(f"Creating {dstfile} from {in_files_for_seg}")
                    #rule_matches[dstfile] = in_types_str, outbase, logbase, infile.runnumber, infile.segment, "dummy", self.dsttype
                    rule_matches[dstfile] = ["dbinput"], outbase, logbase, infile.runnumber, infile.segment, "dummy", self.dsttype
                continue

            ####### NOT 1-1, requires more work:
            # For every segment, there is exactly one output file, and exactly one input file _from each stream_ OR from the previous step
            ######## Cut up the candidates into streams/daqhost≈ƒs
            candidates.sort(key=lambda x: (x.runnumber, x.daqhost)) # itertools.groupby depends on data being sorted
            files_for_run = { k : list(g) for
                              k, g in itertools.groupby(candidates, operator.attrgetter('daqhost')) }

            # daq file lists all need GL1 files. Pull them out and add them to the others
            if ( 'gl1daq' in in_types_str ):
                gl1_files = files_for_run.pop('gl1daq',None)
                if gl1_files is None:
                    WARN(f"No GL1 files found for run {runnumber}. Skipping this run.")
                    continue
                CHATTY(f'All GL1 files for for run {runnumber}:\n{gl1_files}')

                ### Important change, 07/15/2025: By default, only care about segment 0!
                segswitch="seg0fromdb"
                segments=set()
                for host in files_for_run:
                    for f in files_for_run[host]:
                        if f.status==1:
                            segments.add(f.segment)
                if segments:
                    CHATTY(f"Run {runnumber} has {len(segments)} segments in the input streams: {sorted(segments)}")

                okforseg0=(segments=={0})
                if not self.input_config.combine_seg0_only:
                    if okforseg0:
                        DEBUG(f"Run {runnumber} has {len(segments)} segments in the input streams: {sorted(segments)}. Skipping this run.")
                        continue
                    DEBUG("Using, and requiring, all input segments")
                    segswitch="allsegsfromdb"
                    for host in files_for_run:
                        files_for_run[host] = gl1_files + files_for_run[host]
                        any_bad_status = any(file_tuple.status != 1 for file_tuple in files_for_run[host])
                        # Now enforce status!=0 for all files from this host
                        if any_bad_status :
                            files_for_run[host]=[]
                    # Done with the non-default.
                else: ### Use only segment 0; this is actually a bit harder
                    CHATTY("Using only input segment 0")
                    if not okforseg0:
                        DEBUG(f"Run {runnumber} has {len(segments)} segments in the input streams: {sorted(segments)}. Skipping this run.")
                        continue
                    # GL1 file?
                    gl1file0=None
                    for f in gl1_files:
                        if f.segment==0 and f.status==1:
                            gl1file0=f
                            break
                    if not gl1file0:
                        CHATTY(f"No segment 0 GL1 file found for run {runnumber}. Skipping this run.")
                        for host in files_for_run:
                            files_for_run[host]=[]
                        continue

                    # With a segment0 gl1 file, we can now go over the other hosts
                    for host in files_for_run:
                        for f in files_for_run[host]:
                            if f.segment==0 and f.status==1:
                                files_for_run[host]=[gl1file0,f]
                                break
                        else:  # remember that python's for-else executes when the break doesn't
                            CHATTY(f"No segment 0 file found for run {runnumber}, host {host}. Skipping this run.")
                            files_for_run[host]=[]
                # \combine_seg0_only
            # \if gl1daq in intypes
            

            ####### "Easy" case. One way to identify this case is to see if gl1 is not needed
            #  If the input has a segment number, then the output will have the same segment number
            #  - These are downstream objects (input is already a DST)
            #  - This can be 1-1 or many-to-1 (usually 2-1 for SEED + CLUSTER --> TRACKS)
            if 'gl1daq' not in in_types_str:
                ### Get available input
                DEBUG("Getting available daq hosts for run {runnumber}")
                ## TODO: Split between seb-like and not seb-like for tracking and calo!
                #                 daqhost_query=f"""
                # select hostname from hostinfo
                # where hostname not like 'seb%' and hostname not like 'gl1%'
                # and runnumber={runnumber}"""
                daqhost_query=f"""select hostname,serverid from hostinfo where runnumber={runnumber}"""
                daqhost_serverid=[ (c.hostname,c.serverid) for c in dbQuery( cnxn_string_map['daqr'], daqhost_query).fetchall() ]
                available_tpc=set()
                available_tracking=set()
                available_seb=set()
                for (hostname,serverid) in daqhost_serverid:
                    if hostname=='ebdc39' : # special case for TPOT
                        available_tracking.add(hostname)
                        continue
                    if 'ebdc' in hostname:
                        available_tpc.add(f"{hostname}_{serverid}")
                        continue
                    if 'seb' in hostname:
                        available_seb.add(hostname)
                        continue
                    # remainder is other tracking detectors (and gl1)
                    if not 'gl1' in hostname:
                        available_tracking.add(hostname)
                    
                DEBUG (f"Found {len(available_tpc)} TPC hosts in the run db")
                CHATTY(f"{available_tpc}")
                DEBUG (f"Found {len(available_tracking)} other tracking hosts in the run db")
                CHATTY(f"{available_tracking}")
                DEBUG (f"Found {len(available_seb)} sebXX hosts in the run db")
                CHATTY(f"{available_seb}")
                ### Here we could enforce both mandatory and masked hosts

                # TPC hardcoding
                if 'TRKR_CLUSTER' in self.dsttype:
                    # 1. require at least N=30 out of the 48 ebdc_[0-24]_[01] to be turned on in the run
                    #    This is an early breakpoint to see if the run can be used for tracking
                    #    CHANGE 08/21/2025: On request from jdosbo, change back to requiring all ebdcs.
                    ### Important note: NO such requirement for cosmics. FIXME?
                    minNTPC=48
                    if len(available_tpc) < minNTPC and not self.physicsmode=='cosmics':
                        WARN(f"Skip run. Only {len(available_tpc)} TPC detectors turned on in the run.")
                        continue
                    
                    # 2. How many are TPC hosts are actually there in this run.
                    #    Not necessarily the same as above, if input DSTs aren't completely produced yet.
                    #    Other reason could be if the daq db is wrong.
                    present_tpc_files=set()
                    for host in files_for_run:
                        for available in available_tpc:
                            if available in host:
                                present_tpc_files.add(host)
                                continue                
                    if len(present_tpc_files) < minNTPC and not self.physicsmode=='cosmics':
                        WARN(f"Skip run {runnumber}. Only {len(present_tpc_files)} TPC detectors actually in the run.")
                        #WARN(f"Available TPC hosts in the daq db: {sorted(available_tpc)}")
                        #WARN(f"Present TPC hosts: {sorted(present_tpc_files)}")
                        missing_hosts = [host for host in available_tpc if not any(host in present for present in present_tpc_files)]
                        if missing_hosts:
                            WARN(f"Missing TPC hosts: {missing_hosts}")
                        continue
                    DEBUG (f"Found {len(present_tpc_files)} TPC files in the catalog")

                    # 3. For INTT, MVTX, enforce that they're all available if possible
                    present_tracking=set(files_for_run).symmetric_difference(present_tpc_files)
                    CHATTY(f"Available non-TPC hosts in the daq db: {present_tracking}")
                    ### TODO: Only checking length here. Probably okay forever though.
                    if len(present_tracking) != len(available_tracking) and not self.physicsmode=='cosmics':
                        WARN(f"Skip run {runnumber}. Only {len(present_tracking)} non-TPC detectors actually in the run. {len(available_tracking)} possible.")
                        missing_hosts = [host for host in available_tracking if not any(host in present for present in present_tracking)]
                        if missing_hosts:
                            WARN(f"Missing non-TPC hosts: {missing_hosts}")
                        # WARN(f"Available non-TPC hosts in the daq db: {sorted(available_tracking)}")
                        # WARN(f"Present non-TPC leafs: {sorted(present_tracking)}")
                        continue
                    DEBUG (f"Found {len(present_tracking)} other tracking files in the catalog")

                # Sort and group the input files by segment. Reject if not all hosts are present in the segment yet
                segments = None
                rejected = set()
                for host in files_for_run:
                    files_for_run[host].sort(key=lambda x: (x.segment))
                    new_segments = list(map(lambda x: x.segment, files_for_run[host]))
                    if segments is None:
                        segments = new_segments
                    elif segments != new_segments:
                        rejected.update( set(segments).symmetric_difference(set(new_segments)) )
                        segments = list( set(segments).intersection(new_segments))

                if len(rejected) > 0  and not self.physicsmode=='cosmics' :
                    DEBUG(f"Run {runnumber}: Removed {len(rejected)} segments not present in all streams.")
                    CHATTY(f"Rejected segments: {rejected}")
                

                # If the output doesn't exist yet, use input files to create the job
                # outbase=f'{self.dsttype}_{self.outtriplet}_{self.outdataset}'
                outbase=f'{self.dsttype}_{self.dataset}_{self.outtriplet}'
                for seg in segments:
                    logbase= f'{outbase}-{runnumber:{pRUNFMT}}-{seg:{pSEGFMT}}'
                    dstfile = f'{logbase}.root'
                    if dstfile in existing_output:
                        CHATTY(f"Output file {dstfile} already exists. Not submitting.")
                        continue
                    if dstfile in existing_status:
                        CHATTY(f"Output file {dstfile} already has production status {existing_status[dstfile]}. Not submitting.")
                        continue
                    # in_files_for_seg= []
                    # for host in files_for_run:
                    #     in_files_for_seg += [ f.filename for f in files_for_run[host] if f.segment == seg ]
                    # in_files_for_seg=[ "foo", "bar", "baz" ]
                    # CHATTY(f"Creating {dstfile} from {in_files_for_seg}")
                    ## in_types as first return?
                    rule_matches[dstfile] = ["dbinput"], outbase, logbase, runnumber, seg, "dummy", self.dsttype

            ######## Streaming and triggered daq combination
            # In this case, provide ALL input files for the run, and the output will produce its own segment numbers
            # Output and input segment number have no correlation. Not possible to check for all possible existing outfiles
            # so we have to assume if one exists for segment0, it exists for all. This is then the file we key on in prod db as well.
            if 'gl1daq' in in_types_str:
                ### Important change, 07/15/2025: By default, only care about segment 0!
                # Sort and group the input files by host
                for leaf, daqhost in self.input_stem.items():
                    if daqhost not in files_for_run:
                        CHATTY(f"No inputs from {daqhost} for run {runnumber}.")
                        continue
                    if files_for_run[host]==[]:
                        continue
                    dsttype  = f'{self.dsttype}_{leaf}'
                    dsttype += f'_{self.dataset}' # DST_STREAMING_EVENT_%_run3auau
                    outbase=f'{dsttype}_{self.outtriplet}'
                    # Use segment 0 as key for logs and for existing output
                    logbase=f'{outbase}-{runnumber:{pRUNFMT}}-{0:{pSEGFMT}}'
                    dstfile=f'{logbase}.root'
                    if dstfile in existing_output:
                        CHATTY(f"Output file {dstfile} already exists. Not submitting.")
                        continue

                    if dstfile in existing_status:
                        WARN(f"Output file {dstfile} already has production status {existing_status[dstfile]}. Not submitting.")
                        continue

                    DEBUG(f"Creating {dstfile} for run {runnumber} with {len(files_for_run[daqhost])} input files")

                    files_for_run[daqhost].sort(key=lambda x: (x.segment)) # not needed but tidier
                    rule_matches[dstfile] = [segswitch], outbase, logbase, runnumber, 0, daqhost, self.dsttype+'_'+leaf
                # \if gl1daq, i.e. combining or not
            # \for run
        INFO(f'[Parsing time ] {(datetime.now() - start).total_seconds():.2f} seconds')

        return rule_matches
# ============================================================================
def parse_lfn(lfn: str, rule: RuleConfig) -> Tuple[str,...] :
    # Notably, input is not necessarily a true lfn, but:
    # If there's a colon, throw everything away after the first one; that's another parser's problem
    try:
        name=lfn.split(':')[0]
        name=Path(name).name # could throw an error instead if we're handed a full path.
        #  split at, and remove, run3auau_new_nocbdtag_v001, remainder is 'DST_...', '-00066582-00000.root' (or .finished)
        # dsttype,runsegend=name.split(f'_{rule.outtriplet}_{rule.dataset}')
        dsttype,runsegend=name.split(f'_{rule.dataset}_{rule.outtriplet}')
        if runsegend=='.root':
            raise ValueError("killkillkill")
        _,run,segend=runsegend.split('-')
        seg,end=segend.split('.')
    except ValueError as e:
        print(f"[parse_lfn] Caught error {e}")
        print(f"lfn = {lfn}")
        raise
        # else:
        #     exit(-1)
    return dsttype,int(run),int(seg),end


# ============================================================================
def parse_spiderstuff(filename: str) -> Tuple[str,...] :
    try:
        size=-1
        ctime=-1
        if 'size' in filename and 'ctime'in filename:
            lfn,_,nevents,_,first,_,last,_,md5,_,size,_,ctime,_,dbid = filename.split(':')
        else:
            lfn,_,nevents,_,first,_,last,_,md5,_,dbid = filename.split(':')

        lfn=Path(lfn).name
    except Exception as e:
        ERROR(f"Error: {e}")
        print(filename)
        print(filename.split(':'))
        exit(-1)

    return lfn,int(nevents),int(first),int(last),md5,int(size),int(ctime),int(dbid)

# ============================================================================
