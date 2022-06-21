from hhAnalysis.multilepton.samples.metaDict_2017_hh import meta_dictionary as meta_dictionary_hh
from hhAnalysis.multilepton.samples.metaDict_2017_private import meta_dictionary as meta_dictionary_private
from hhAnalysis.multilepton.samples.metaDict_2017_hh import sum_events

import itertools, collections

meta_dictionary = collections.OrderedDict(itertools.chain(
  meta_dictionary_hh.items(), meta_dictionary_private.items()
))
