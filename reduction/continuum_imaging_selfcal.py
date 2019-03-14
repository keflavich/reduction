"""
Continuum imaging scripts.  There must be a ``continuum_mses.txt`` file in the
directory this is run in.  That file is produced by ``split_windows.py``.

You can set the following environmental variables for this script:
    EXCLUDE_7M=<boolean>
        If this parameter is set (to anything), the 7m data will not be
        included in the images if they are present.

The environmental variable ``ALMAIMF_ROOTDIR`` should be set to the directory
containing this file.
"""

import os
import copy
import sys

if os.getenv('ALMAIMF_ROOTDIR') is None:
    try:
        import metadata_tools
        os.environ['ALMAIMF_ROOTDIR'] = os.path.split(metadata_tools.__file__)[0]
    except ImportError:
        raise ValueError("metadata_tools not found on path; make sure to "
                         "specify ALMAIMF_ROOTDIR environment variable "
                         "or your PYTHONPATH variable to include the directory"
                         " containing the ALMAIMF code.")
else:
    sys.path.append(os.getenv('ALMAIMF_ROOTDIR'))

import numpy as np

from metadata_tools import determine_imsize, determine_phasecenter, logprint
from make_custom_mask import make_custom_mask
from imaging_parameters import imaging_parameters
from selfcal_heuristics import goodenough_field_solutions

from tasks import tclean, plotms, split

from gaincal_cli import gaincal_cli as gaincal
from rmtables_cli import rmtables_cli as rmtables
from applycal_cli import applycal_cli as applycal
from exportfits_cli import exportfits_cli as exportfits

from taskinit import msmdtool, iatool
msmd = msmdtool()
ia = iatool()

imaging_root = "imaging_results"
if not os.path.exists(imaging_root):
    os.mkdir(imaging_root)

if 'exclude_7m' not in locals():
    if os.getenv('EXCLUDE_7M') is not None:
        exclude_7m = bool(os.getenv('EXCLUDE_7M').lower() == 'true')
        array = '12M'
    else:
        exclude_7m = False
        array = '7M12M'


logprint("Beginning selfcal script", origin='contim_selfcal')


# load the list of continuum MSes from a file
# (this file has one continuum MS full path, e.g. /path/to/file.ms, per line)
with open('continuum_mses.txt', 'r') as fh:
    continuum_mses = [x.strip() for x in fh.readlines()]

for continuum_ms in continuum_mses:

    # strip off .cal.ms
    basename = os.path.split(continuum_ms[:-7])[1]

    band = 'B3' if 'B3' in basename else 'B6' if 'B6' in basename else 'ERROR'

    field = basename.split("_")[0]

    if exclude_7m:
        msmd.open(continuum_ms)
        antennae = ",".join([x for x in msmd.antennanames() if 'CM' not in x])
        msmd.close()
        arrayname = '12M'
    else:
        antennae = ""
        arrayname = '7M12M'

    # create a downsampled split MS
    # A different MS will be used for the 12M-only and 7M+12M data
    # (much of the processing time is writing models to the MS, which takes a
    # long time even if 7M antennae are selected out)
    selfcal_ms = basename+"_"+arrayname+"_selfcal.ms"
    if not os.path.exists(selfcal_ms):

        msmd.open(continuum_ms)
        fdm_spws = msmd.fdmspws()
        bws = msmd.bandwidths()[fdm_spws]
        spwstr = ",".join(map(str, fdm_spws))
        freqs = [msmd.reffreq(spw)['m0']['value'] for spw in fdm_spws]
        chwids = [np.mean(msmd.chanwidths(spw)) for spw in fdm_spws]

        # using Roberto's numbers
        # https://science.nrao.edu/facilities/vla/docs/manuals/oss2016A/performance/fov/bw-smearing
        Synth_HPBW = 0.3 # Smallest synth HPBW among target sample in arcsec
        #PB_HPBW = 21. * (300. / minfrq) # PB HPBW at lowest band freq (arcsec)
        #targetwidth = 0.25 * (Synth_HPBW / PB_HPBW) * minfrq # 98% BW smearing criterion

        width = [int(np.abs(0.25 * (Synth_HPBW / (21. * (300e9 / frq))) * frq / chwid))
                 for frq, chwid in zip(freqs, chwids)]
        # do not allow downsampling below 1/2 original width because that drops
        # edge channels sometimes
        width = [ww if float(ww)/msmd.nchan(spw) < 0.5 else int(msmd.nchan(spw)/2)
                 for ww, spw in zip(width, fdm_spws)]

        msmd.close()

        split(vis=continuum_ms,
              outputvis=selfcal_ms,
              datacolumn='data',
              antenna=antennae,
              spw=spwstr,
              width=width,
              field=field,
             )


    coosys,racen,deccen = determine_phasecenter(ms=selfcal_ms, field=field)
    phasecenter = "{0} {1}deg {2}deg".format(coosys, racen, deccen)
    (dra,ddec,pixscale) = list(determine_imsize(ms=selfcal_ms, field=field,
                                                phasecenter=(racen,deccen),
                                                exclude_7m=exclude_7m,
                                                spw=0, pixfraction_of_fwhm=1/4.))
    imsize = [dra, ddec]
    cellsize = ['{0:0.2f}arcsec'.format(pixscale)] * 2

    contimagename = os.path.join(imaging_root, basename) + "_" + arrayname

    if not os.path.exists(contimagename+".uvwave_vs_amp.png"):
        # make a diagnostic plot to show the UV distribution
        plotms(vis=selfcal_ms,
               xaxis='uvwave',
               yaxis='amp',
               avgchannel='1000', # minimum possible # of channels
               plotfile=contimagename+".uvwave_vs_amp.png",
               showlegend=True,
               showgui=False,
               antenna=antennae,
              )


    # only do robust = 0
    robust = 0

    impars = imaging_parameters["{0}_{1}_{2}_robust{3}".format(field, band,
                                                               arrayname,
                                                               robust)]
    dirty_impars = copy.copy(impars)
    dirty_impars['niter'] = 0

    imname = contimagename+"_robust{0}_dirty".format(robust)

    if not os.path.exists(imname+".image.tt0"):
        tclean(vis=continuum_ms,
               field=field.encode(),
               imagename=imname,
               phasecenter=phasecenter,
               outframe='LSRK',
               veltype='radio',
               usemask='pb',
               interactive=False,
               cell=cellsize,
               imsize=imsize,
               pbcor=True,
               antenna=antennae,
               datacolumn='data',
               **dirty_impars
              )

        ia.open(imname+".image.tt0")
        ia.sethistory(origin='almaimf_cont_selfcal',
                      history=["{0}: {1}".format(key, val) for key, val in
                               impars.items()])
        ia.close()

    maskname = make_custom_mask(field, imname+".image.tt0",
                                os.getenv('ALMAIMF_ROOTDIR'),
                                band,
                                rootdir=imaging_root,
                                suffix='_dirty_robust{0}_{1}'.format(robust,
                                                                     arrayname)
                               )
    imname = contimagename+"_robust{0}".format(robust)

    if not os.path.exists(imname+".image.tt0"):
        tclean(vis=continuum_ms,
               field=field.encode(),
               imagename=imname,
               phasecenter=phasecenter,
               outframe='LSRK',
               veltype='radio',
               usemask='user',
               mask=maskname,
               interactive=False,
               cell=cellsize,
               imsize=imsize,
               antenna=antennae,
               savemodel='modelcolumn',
               datacolumn='data',
               **impars
              )
        ia.open(imname+".image.tt0")
        ia.sethistory(origin='almaimf_cont_selfcal',
                      history=["{0}: {1}".format(key, val) for key, val in
                               impars.items()])
        ia.close()

        exportfits(imname+".image.tt0", imname+".image.tt0.fits")
        exportfits(imname+".image.tt0.pbcor", imname+".image.tt0.pbcor.fits")
    else:
        logprint("Skipping completed file {0}".format(imname), origin='almaimf_cont_selfcal')

    # make a custom mask
    maskname = make_custom_mask(field, imname+".image.tt0",
                                os.getenv('ALMAIMF_ROOTDIR'), band,
                                rootdir=imaging_root,
                               )

    selfcaliter = 1
    logprint("Gaincal iteration 1", origin='contim_selfcal')
    # iteration #1 of phase-only self-calibration
    caltable = '{0}_{1}_phase{2}_int.cal'.format(basename, array, selfcaliter)
    if not os.path.exists(caltable):
        gaincal(vis=selfcal_ms,
                caltable=caltable,
                solint='int',
                gaintype='G',
                calmode='p',
                solnorm=True)

    imname = contimagename+"_robust{0}_selfcal1".format(robust)

    if not os.path.exists(imname+".image.tt0"):
        okfields,notokfields = goodenough_field_solutions(caltable, minsnr=5)
        okfields_str = ",".join(["{0}".format(x) for x in okfields])
        logprint("Fields {0} had min snr 5, fields {1} did not"
                 .format(okfields, notokfields), origin='contim_selfcal')
        applycal(vis=selfcal_ms, field=okfields_str, gaintable=[caltable],
                 interp="linear", applymode='calonly', calwt=False)

        # do not run the clean if no mask exists
        assert os.path.exists(maskname)

        # do this even if the output file exists: we need to populate the
        # modelcolumn
        tclean(vis=selfcal_ms,
               field=field.encode(),
               imagename=imname,
               phasecenter=phasecenter,
               outframe='LSRK',
               veltype='radio',
               usemask='user',
               mask=maskname,
               interactive=False,
               cell=cellsize,
               imsize=imsize,
               antenna=antennae,
               savemodel='modelcolumn',
               datacolumn='corrected', # now use corrected data
               **impars
              )
        ia.open(imname+".image.tt0")
        ia.sethistory(origin='almaimf_cont_selfcal',
                      history=["{0}: {1}".format(key, val) for key, val in
                               impars.items()])
        ia.close()
        # overwrite=True because these could already exist
        exportfits(imname+".image.tt0", imname+".image.tt0.fits", overwrite=True)
        exportfits(imname+".image.tt0.pbcor", imname+".image.tt0.pbcor.fits", overwrite=True)


    # do a second iteration, because the first was pretty effective
    # This second iteration should have very little effect

    logprint("Gaincal iteration 2", origin='contim_selfcal')
    # iteration #2 of phase-only self-calibration
    selfcaliter = 2
    caltable = '{0}_{1}_phase{2}_int.cal'.format(basename, array, selfcaliter)
    if not os.path.exists(caltable):
        gaincal(vis=selfcal_ms,
                caltable=caltable,
                solint='int',
                gaintype='G',
                calmode='p',
                solnorm=True)

    maskname = make_custom_mask(field, imname+".image.tt0",
                                os.getenv('ALMAIMF_ROOTDIR'),
                                band,
                                rootdir=imaging_root,
                                suffix='_selfcal1_robust{0}_{1}'.format(robust,
                                                                        arrayname)
                               )

    imname = contimagename+"_robust{0}_selfcal{1}".format(robust, selfcaliter)

    if not os.path.exists(imname+".image.tt0"):
        okfields,notokfields = goodenough_field_solutions(caltable, minsnr=5)
        okfields_str = ",".join(["{0}".format(x) for x in okfields])
        logprint("Fields {0} had min snr 5, fields {1} did not"
                 .format(okfields, notokfields), origin='contim_selfcal')
        applycal(vis=selfcal_ms, field=okfields_str, gaintable=[caltable],
                 interp="linear", applymode='calonly', calwt=False)

        # do not run the clean if no mask exists
        assert os.path.exists(maskname)

        # do this even if the output file exists: we need to populate the
        # modelcolumn
        tclean(vis=selfcal_ms,
               field=field.encode(),
               imagename=imname,
               phasecenter=phasecenter,
               outframe='LSRK',
               veltype='radio',
               usemask='user',
               mask=maskname,
               interactive=False,
               cell=cellsize,
               imsize=imsize,
               antenna=antennae,
               savemodel='modelcolumn',
               datacolumn='corrected', # now use corrected data
               **impars
              )
        ia.open(imname+".image.tt0")
        ia.sethistory(origin='almaimf_cont_selfcal',
                      history=["{0}: {1}".format(key, val) for key, val in
                               impars.items()])
        ia.close()
        # overwrite=True because these could already exist
        exportfits(imname+".image.tt0", imname+".image.tt0.fits", overwrite=True)
        exportfits(imname+".image.tt0.pbcor", imname+".image.tt0.pbcor.fits", overwrite=True)
