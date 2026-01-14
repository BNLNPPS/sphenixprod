### Tracking detectors
# Physical detectors are streaming: many outputs, but each is 1-to-1
# Keeping it as a dictionary until otherwise specified
inputs_from_output={
    'DST_STREAMING_EVENT' : { f'intt{n}'       : f'intt{n}'      for n in range(0,8) }
                        |   { f'mvtx{n}'       : f'mvtx{n}'      for n in range(0,6) }
                        |   { f'ebdc{n:02}_0'  : f'ebdc{n:02}'   for n in range(0,24) }
                        |   { f'ebdc{n:02}_1'  : f'ebdc{n:02}:1' for n in range(0,24) }
                        |   {  'ebdc39'        :  'ebdc39' }
}

# Clusters and seeds: many-to-1
inputs_from_output['DST_TRKR_CLUSTER'] = list('DST_STREAMING_EVENT_' + LEAF for LEAF in inputs_from_output['DST_STREAMING_EVENT'].keys())
inputs_from_output['DST_TRKR_MVTXME']  = list('DST_STREAMING_EVENT_' + LEAF for LEAF in inputs_from_output['DST_STREAMING_EVENT'].keys())
inputs_from_output['DST_TRKR_SEED']    = ['DST_TRKR_CLUSTER']

# Tracks: From clusters and seeds, i.e. 2-1
inputs_from_output['DST_TRKR_TRACKS']  = ['DST_TRKR_SEED']

### Calorimeters -
# Physical detectors are triggered
# Keeping it as a dictionary until otherwise specified
inputs_from_output['DST_TRIGGERED_EVENT'] = { f'seb{n:02}' : f'seb{n:02}' for n in range(0,24) }

# Downstream products fitting
inputs_from_output['DST_CALOFITTING'] = list('DST_TRIGGERED_EVENT_' + LEAF for LEAF in inputs_from_output['DST_TRIGGERED_EVENT'].keys())

# Downstream products
inputs_from_output['DST_CALO'] = ['DST_CALOFITTING']

# Calo jets - KK: Not clear to me what these are in detail
inputs_from_output['DST_JETS'] = ['DST_CALO']
inputs_from_output['DST_JETCALO'] = ['DST_CALOFITTING']
