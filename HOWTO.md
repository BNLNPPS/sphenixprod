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

### Steps to Overwrite Full Production Chain

1. Delete from the production db. This is generally pretty safe. The only problem would be a bit of inconsistency if jobs you want to resubmit are currently queued.
   ```
   psql -d Production -h sphnxproddbmaster.sdcc.bnl.gov -U argouser -c "delete from production_status where dstname like 'DST_TRKR_CLUSTER%run3cosmics%' and status!='finished' and run in ( ... );"
   ```
   You can usually even ignore the run constraint. The prod db is only used to not accidentally submit the same thing multiple times, and once a run is finished (and spidered if you want to keep the result), the entries can be safely deleted.
2. Remove them from the files table. This needs to come before updating datasets db because it has very little info besides the name so the query needs to be cross linked.  Command is like this, where you'd also fill in a list or range of run numbers:
   ```
   delete from files
   USING datasets
   WHERE
       files.lfn=datasets.filename
   and
   datasets.dsttype='DST_TRKR_CLUSTER'
   and
   datasets.runnumber in ( 68715, 68716, ... )
   ;
   ```
Annoyingly, delete and select have different syntax for two tables. So to check before you delete, you'd replace the first three lines with
   ```
   select files.lfn from files, datasets
   WHERE
   ```
3. Finally, you can delete them from datasets. Straightforward
   ```
   delete from datasets
   WHERE
       dsttype='DST_TRKR_CLUSTER'
   and
       runnumber in ( 68715, 68716, ... )
   ;
   ```
   The last two are started with
   ```
   psql -d FileCatalog -h sphnxdbmaster.sdcc.bnl.gov
   ```
