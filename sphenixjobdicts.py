### Tracking detectors
# Physical detectors are streaming: many outputs, but each is 1-to-1
inputs_from_output={
    'DST_STREAMING_EVENT' : { f'INTT{n}'    : f'intt{n}'    for n in range(0,8) }
                        |   { f'MVTX{n}'    : f'mvtx{n}'    for n in range(0,6) }
                        |   { f'TPC{n:02}'  : f'ebdc{n:02}' for n in range(0,24) }
                        |   {  'TPOT'       :  'ebdc39' }
}

# Clusters and seeds: many-to-1
inputs_from_output['DST_TRKR_CLUSTER'] = list('DST_STREAMING_EVENT_' + LEAF for LEAF in inputs_from_output['DST_STREAMING_EVENT'].keys())
inputs_from_output['DST_TRKR_SEED']    = ['DST_TRKR_CLUSTER']

# Tracks: From clusters and seeds, i.e. 2-1
inputs_from_output['DST_TRKR_TRACKS']  = ['DST_TRKR_CLUSTER','DST_TRKR_SEED']

### Calorimeters -
# Physical detectors are triggered
# Trigger file is %GL1%.evt; input files are /bbox%/{cosmics|physics|}'%emcal%.prdf', '%HCal%.prdf', '%LL1%.prdf', '%mbd%.prdf', '%ZDC%.prdf'
# many-to-1
# inputs_from_output['DST_TRIGGERED_EVENT'] = [ 'GL1%.evt'
#                                            , 'emcal%.prdf'
#                                            , 'HCal%.prdf'
#                                            , 'LL1%.prdf'
#                                            , 'mbd%.prdf'
#                                            , 'ZDC%.prdf'
#                                            ]
# inputs_from_output['DST_TRIGGERED_EVENT'] = [ f'seb{n:02}' for n in range(0,24) ]
inputs_from_output['DST_TRIGGERED_EVENT'] = { f'SEB{n:02}' : f'seb{n:02}' for n in range(0,24) }

# Downstream products fitting
inputs_from_output['DST_CALOFITTING'] = list('DST_TRIGGERED_EVENT_' + LEAF for LEAF in inputs_from_output['DST_TRIGGERED_EVENT'].keys())

# Downstream products
inputs_from_output['DST_CALO'] = ['DST_CALOFITTING']

# Calo jets - KK: Not clear to me what these are in detail
inputs_from_output['DST_JETS'] = ['DST_CALO']
inputs_from_output['DST_JETCALO'] = ['DST_CALOFITTING']
