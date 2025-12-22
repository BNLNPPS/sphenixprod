### Simplified steps to submit a production


First clone the repository

```bash
$ git clone git@github.com:BNLNPPS/sphenixprod.git
$ source sphenixprod/this_sphenixprod.sh
```

Then run a submission using the submission script `sphenixprod/create_submission.py`

For help, use `create_submission.py -h` and for testing purposes with verbose output use `create_submission.py -n`, using `-v`, `-vv`, or `-vvv` for more/less verbosity. Using `-n` will explicitly not submit anything

Example submission, which requires

1. a configuration yaml file
2. a rule in that yaml file to run
3. a run range

```
create_submission.py \
--config ProdFlow/short/run3auau/v001_combining_run3_new_nocdbtag.yaml \
--rule DST_TRIGGERED_EVENT_run3physics \
--runs 69600 72000 \
-vv -n
```

To run a production job for real, you need to first clean out the file catalog (FC) for your particular jobs. Jobs will not run if there are existing entries in the FC so as to not overwrite actively running jobs inadvertently

```
psql -d Production -h sphnxproddbmaster.sdcc.bnl.gov -U argouser -c "delete from production_status where run>=66517 and  run<=66685 and dsttype like 'DST_TRIGGERED_EVENT%' ;"
```

Before submitting, check / change / ask Kolja to change the autopilot schedule to make sure your freshly freed up job opportunities don't get picked up by the production dispatcher.

Then you can submit in one swoop with


```
create_submission.py \
--config ProdFlow/short/run3auau/v001_combining_run3_new_nocdbtag.yaml \
--rule DST_TRIGGERED_EVENT_run3physics \
--runs 69600 72000 \
-vv -andgo
```

The condor ID will be printed out such that you can monitor the status of the job. Keep in mind that at most 50k jobs can be running on a particular node.

###Steps for Complete Reset of a Production

First step: Stop the autopilot submission. For example, if we wanted to stop the autopilot for CLUSTER and downstream SEED jobs for run3pp full production, we would modify `ProdFlow/short/run3pp/run3pp_production.yaml` like the following, making sure to check that we're disabling all instances of the types of jobs we want to reset across all production hosts. The key is to turn off the submit, dstspider, and finishmon flags:

```
  # TRKR_CLUSTER physics
  Full_TRKR_CLUSTER_run3pp:
    config: ppstreaming_from523_physics_ana526_2025p008_v001.yaml
    runs: [79300 90000]
    jobprio: 60
    submit: off
    dstspider: off
    finishmon: off

  # TRKR_SEED physics
  Full_TRKR_SEED_run3pp:
    config: ppstreaming_from523_physics_ana526_2025p008_v001.yaml
    runs: [79300 90000]
    jobprio: 30
    submit: off
    dstspider: off
    finishmon: off
```

Next step: Now kill all queued jobs. If you start typing `condor_rm` and then press the up arrow a few times, you should get to a command with ` ... -const 'JobBatchName..."', that you can use (and modify with the correct batch name if it isn't right in the history). The full command should look something like the following if you can't find it, just make sure the syntax matches the specific job types you want to disable:

```
condor_rm -const 'JobBatchName=="main.Full_TRKR_CLUSTER_run3pp_run3pp_ana526_2025p008_v001"'
```

Next step: You should double-check we didn't miss any cron-generated create_submission processes that were on-going when you changed the steer file. That can be done with:

```
ps axuww|grep create
```

Cleanup step: We can do one of two things:

1. Delete all the output directories on lustre (and the logs on gpfs while we're at it)
2. Leave the existing output on disk, it will be overwritten. However, the auto-spider is off, but there are probably `.root:nevents:...` files still lying around that would confuse the spider when it's restarted. In that case, you'd run a final dstspider command on both of these (with the run range we're interested in).

1 is the cleaner way to do it, but it could involve deleting millions of files and take a painfully long time. Best way is to use a combination of lustre tools, namely their own (less powerful) version of `find`, and a specific bare-bones version of `rm`, namely `munlink`. Building the command in pieces:
Example directories to search: 
```
/sphenix/lustre01/sphnxpro/production/run3pp/physics/ana526_2025p008_v001/DST_TRKR_*/run_00079*
```
You can use all kinds of constraints, like -name DST\*. We want just want all regular files (directories gove error messages), so the way to find it all is (The lfs uses the lustre tool instead of the linux tool.):
```
lfs find [directories] -type f
```
Finally, there's some dark magic you just need to note down or memorize. Print one result per line, with the right separator, select the right separator in `xargs`, then use `munlink`.
Final complete line is (those are zeros in `print0` and `xargs -0`):
```
lfs find /sphenix/lustre01/sphnxpro/production/run3pp/physics/ana526_2025p008_v001/DST_TRKR_*/run_00079* -type f -print0 | xargs -0 munlink
```
You canpaste the line without the | xargs ... first to see that it returns the files yopu'd expect.

At the same time, do the same to the gpfs files. Frustratingly, munlink would become unlink, the `print0 ... xargs -0` stuff is different etc. But regular lfs is more powerful and I can just use `-delete`. Full line to use is:
```
find /sphenix/data/data02/sphnxpro/production/run3pp/physics/ana526_2025p008_v001/DST_TRKR_*/run_00079* -type f -delete
```

Next step: clean out the Production db and FileCatalog. The production command will look something like the following adjusted for the specfic file types you want to remove:

```
psql -d Production -h sphnxproddbmaster.sdcc.bnl.gov -U argouser -c "delete from production_status where dstname like 'DST_TRKR_CLUSTER%run3pp%ana526%' and run between 79200 and 80000;"
```

The FileCatalog query can be opened with `psql -d FileCatalog -h sphnxdbmaster.sdcc.bnl.gov` and you will want to run something like the following adjusted for the conditions you want to remove:

```
delete from files
USING datasets 
WHERE
  files.lfn=datasets.filename
and
  datasets.dsttype='DST_TRKR_CLUSTER'
and
  datasets.runnumber between 79200 and 80000
and
  datasets.tag='ana526_2025p008_v001'
;
```
If you need to find which other things you can cut on with your query, try running `select * from datasets` or from `files` and it should give you a printout of the available variables

You also need to delete from datasets as well, which will be the same as the above query but you change the first 2 lines to be just `delete from datasets`.

Lastly, check each of the 4 hosts to make sure there isn't anything that has been overlooked, then turn the automatic production flags back on!

