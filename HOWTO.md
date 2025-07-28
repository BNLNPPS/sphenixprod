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