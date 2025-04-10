
### Tracking detectors
# Physical detectors are streaming: many outputs, but each is 1-to-1
InputsFromOutput={
    'DST_STREAMING_EVENT' : { f'DST_STREAMING_EVENT_INTT{n}'    : f'intt{n}'    for n in range(0,8) }
                        |   { f'DST_STREAMING_EVENT_MVTX{n}'    : f'mvtx{n}'    for n in range(0,6) } 
                        |   { f'DST_STREAMING_EVENT_TPC{n:02}'  : f'ebdc{n:02}' for n in range(0,24) } 
                        |   {  'DST_STREAMING_EVENT_TPOT'       :  'ebdc39' }
}

# Clusters and seeds: many-to-1
InputsFromOutput['DST_TRKR_CLUSTER'] = list(InputsFromOutput['DST_STREAMING_EVENT'].keys())
InputsFromOutput['DST_TRKR_SEED']    = list(InputsFromOutput['DST_TRKR_CLUSTER'])

# Tracks: From clusters and seeds, i.e. 2-1
InputsFromOutput['DST_TRKR_TRACKS']  = ['DST_TRKR_CLUSTER','DST_TRKR_SEED']

### Calorimeters - 
# Physical detectors are triggered
# Trigger file is %GL1%.evt; input files are /bbox%/{cosmics|physics|}'%emcal%.prdf', '%HCal%.prdf', '%LL1%.prdf', '%mbd%.prdf', '%ZDC%.prdf'
# many-to-1
InputsFromOutput['DST_TRIGGERED_EVENT'] = [ 'GL1%.evt'
                                           , 'emcal%.prdf'
                                           , 'HCal%.prdf'
                                           , 'LL1%.prdf'
                                           , 'mbd%.prdf'
                                           , 'ZDC%.prdf'
                                           ]

# Downstream products fitting
InputsFromOutput['DST_CALOFITTING'] = ['DST_TRIGGERED_EVENT']

# Downstream products
InputsFromOutput['DST_CALO'] = ['DST_CALOFITTING']

# Calo jets - KK: Not clear to me what these are in detail
InputsFromOutput['DST_JETS'] = ['DST_CALO']
InputsFromOutput['DST_JETCALO'] = ['DST_CALOFITTING']
