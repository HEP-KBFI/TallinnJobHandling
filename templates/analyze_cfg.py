import FWCore.ParameterSet.Config as cms

from TallinnAnalysis.HistogramTools.controlPlot1D_cfi import controlPlot1D as histograms_controlPlot1D
from TallinnAnalysis.HistogramTools.controlPlot2D_cfi import controlPlot2D as histograms_controlPlot2D
#from TallinnAnalysis.HistogramTools.datacard_HH_2lss_cfi import datacard_HH_2lss_nonresonant as histograms_datacard_HH_2lss_nonresonant, datacard_HH_2lss_resonant_spin0 as histograms_datacard_HH_2lss_resonant_spin0, datacard_HH_2lss_resonant_spin2 as histograms_datacard_HH_2lss_resonant_spin2
CHANNELINPUTS

process = cms.PSet()

process.fwliteInput = cms.PSet(
    fileNames = cms.vstring()
)

process.fwliteOutput = cms.PSet(
    fileName = cms.string('')
)

process.analyze = cms.PSet(
    treeName = cms.string('Events'),

    process = cms.string(''),
 
    central_or_shifts = cms.vstring('central'),

    selection = cms.string('nlep == 2 & ntau == 1'),

    histogramPlugins = cms.VPSet(
#        histograms_controlPlot1D,
#        histograms_controlPlot2D,
        histograms_datacard_HH_2lss_nonresonant,
#        histograms_datacard_HH_2lss_resonant_spin0,
#        histograms_datacard_HH_2lss_resonant_spin2
    ),

    evtWeights = cms.vstring([ "evtWeight" ]),

    selEventsFileName_input = cms.string(''),
    selEventsFileName_output = cms.string(''),

    isDEBUG = cms.bool(False)
)

process.fwliteInput.fileNames = cms.vstring(['INFILE'])
process.fwliteOutput.fileName = cms.string('ANOUTFILE')
process.analyze.process                                        = cms.string('PROCESS')
process.analyze.central_or_shifts                              = cms.vstring('SYSTORSHIFT')
process.analyze.selection                                      = cms.string('SELECTION')
process.analyze.evtWeights                                     = cms.vstring([ "evtWeight" ])
process.analyze.selEventsFileName_output                       = cms.string('SELEVOUTFILE')
process.analyze.histogramPlugins                               = cms.VPSet(HISTPLUGINS)
process.analyze.isDEBUG                                        = cms.bool(False)
