import argparse

# ============================================================================
def submission_args():
    """Handle command line tedium for submitting jobs."""

    arg_parser = argparse.ArgumentParser( prog='create_submission.py',
                    description='"Production script to submit jobs to the batch system for sPHENIX."',
    #               epilog=''
                    )

    arg_parser = argparse.ArgumentParser()

    # General arguments
    arg_parser.add_argument( '--dry-run', '--no-submit', help="Job will not be submitted, DBs notupdated. Just print things", dest="submit", action="store_false")     # would be nice to allow -n for dry run but that's currently occupied by nevents
    arg_parser.add_argument( '--test-mode',dest="test_mode",default=False,help="Sets testing mode, which will mangle DST names and directory paths.",action="store_true")
    arg_parser.add_argument( "--force", "--doit", dest="force", action="store_true" ) #TODO: deprecate doit
    arg_parser.add_argument( '--print-query',dest='printquery',help="Print the query after parameter substitution and exit", action="store_true")
    
    vgroup = arg_parser.add_argument_group('Logging level')
    exclusive_vgroup = vgroup.add_mutually_exclusive_group()
    exclusive_vgroup.add_argument( '-v', '--verbose', help="Prints more information per repetition", action='count', default=0)
    exclusive_vgroup.add_argument( '-d', '--debug', help="Prints even more information", action="store_true")
    exclusive_vgroup.add_argument( '-c', '--chatty', help="Prints the most information", action="store_true")
    exclusive_vgroup.add_argument( '--loglevel', dest='loglevel', default='INFO', help="Specific logging level (CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL)" )
    
    arg_parser.add_argument( '--sublogdir', dest='sublogdir', default=None, help="Directory for submission script logging (defaults under /tmp)" )
    ## TODO dbinput is now the default, different forms should be deprecated
    arg_parser.add_argument( "--dbinput", default=True, action="store_true",help="Passes input filelist through the production status db rather than the argument list of the production script." )
    arg_parser.add_argument( "--no-dbinput", dest="dbinput", action="store_false",help="Unsets dbinput flag." )

    # Job description arguments
    arg_parser.add_argument( '--config', dest='config', help="Name of the YAML file containing production rules.", default="DST_STREAMING_run3auau_new_2024p012.yaml")
    arg_parser.add_argument( '--rulename',   dest='rulename',   help="Name of submission rule", default="DST_EVENT" )

    rgroup = arg_parser.add_argument_group('Run selection')
    exclusive_rgroup = rgroup.add_mutually_exclusive_group()
    exclusive_rgroup.add_argument( '--runs', nargs='+', help="One argument for a specific run.  Two arguments an inclusive range.  Three or more, a list", default=['56900'] )
    exclusive_rgroup.add_argument( '--runlist', help="Flat text file containing list of runs to process, separated by whitespace / newlines.", default=None )
    # arg_parser.add_argument( '--segments', nargs='+', help="One argument for a specific run.  Two arguments an inclusive range.  Three or more, a list", default=[] )
    arg_parser.add_argument( '--physics-mode','--experiment-mode',dest="physicsmode",help="Specifies the experiment mode (cosmics, commissioning, physics) for direct lookup of input files.",default="physics")

    arg_parser.add_argument( '-N', '--nevents', '-n', default=0, dest='nevents', help='Number of events to process.  0=all.', type=int)
    ## sPHENIX files have specific names and locations. Overridde for testing or special purposes.
    arg_parser.add_argument( '--mangle-dstname',dest='mangle_dstname',help="Replaces 'DST' with the specified name.", default=None )
    arg_parser.add_argument( '--mangle-dirpath',dest='mangle_dirpath',help="Inserts string after sphnxpro/ (or tmp/) in the directory structure", default=None, type=int )

    # copy additional file to the job work directory
    arg_parser.add_argument( '--append-to-rsync', dest='append2rsync', default=None,help="Appends the argument to the list of rsync files to copy to the worker node" )

    # Queue-related constraints
    arg_parser.add_argument( '--maxjobs',dest="maxjobs",help="Maximum number of jobs to pass to condor", default=None )
    arg_parser.add_argument( '--limit', help="Limit for input db queries", default=0, type=int )
    # arg_parser.add_argument( '-u', '--unblock-state', nargs='*', dest='unblock',  choices=["submitting","submitted","started","running","evicted","failed","finished"] )
    arg_parser.add_argument( '-r', '--resubmit', dest='resubmit', default=False, action='store_true', help='Existing filecatalog entry does not block a job')
    arg_parser.add_argument( '--docstring',default=None,help="Appends a documentation string to the log entry")
    # batch_name should be set in JobConfig <-- TODO: allow override?
    # arg_parser.add_argument( "--batch-name", dest="batch_name", default=None ) #default="$(name)_$(build)_$(tag)_$(version)"
    
    # args, userargs = arg_parser.parse_known_args()

    args = arg_parser.parse_args()
    if args.verbose==1 :
        args.loglevel = 'INFO'
    if args.debug or args.verbose==2 :
        args.loglevel = 'DEBUG'
    if args.chatty or args.verbose==3 :
        args.loglevel = 'CHATTY'
    
    return args

# ============================================================================
