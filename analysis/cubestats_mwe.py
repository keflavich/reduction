import os
import time
import numpy as np
from spectral_cube import SpectralCube,DaskSpectralCube

from casa_formats_io import Table as casaTable

if os.environ.get('SLURM_TMPDIR'):
    os.environ['TMPDIR'] = os.environ.get("SLURM_TMPDIR")
elif not os.environ.get("TMPDIR"):
    os.environ['TMPDIR'] ='/blue/adamginsburg/adamginsburg/tmp/'
print(f"TMPDIR = {os.environ.get('TMPDIR')}")

# Dask writes some log stuff; let's make sure it gets written on local scratch
# or in a blue drive and not on orange
os.chdir(os.getenv('TMPDIR'))

threads = int(os.getenv('DASK_THREADS') or os.getenv('SLURM_NTASKS'))
print(f"Using {threads} threads.")

global then
then = time.time()
def dt():
    global then
    now = time.time()
    print(f"Elapsed: {now-then}s", flush=True)
    then = now

num_workers = None
print(f"PID = {os.getpid()}")

cluster = os.getenv('LOCALCLUSTER')
print(f"cluster is set to be '{cluster}' ({bool(cluster)})")

if __name__ == "__main__":
    if threads:
        # try dask.distrib again
        from dask.distributed import Client, LocalCluster
        import dask

        mem_mb = int(os.getenv('SLURM_MEM_PER_NODE'))
        print("Threads was set", flush=True)

        try:
            nthreads = int(threads)
            #memlimit = f'{0.8 * int(mem_mb) / int(nthreads)}MB'
            memlimit = f'{0.4*int(mem_mb)}MB'
            if nthreads > 1 and not cluster:
                num_workers = nthreads
                scheduler = 'threads'
            elif cluster:
                print(f"nthreads = {nthreads} > 1, so starting a LocalCluster with memory limit {memlimit}", flush=True)
                #scheduler = 'threads'
                # set up cluster and workers
                cluster = LocalCluster(n_workers=1,
                                       threads_per_worker=int(nthreads),
                                       memory_target_fraction=0.60,
                                       memory_spill_fraction=0.65,
                                       memory_pause_fraction=0.7,
                                       #memory_terminate_fraction=0.9,
                                       memory_limit=memlimit,
                                       silence_logs=False, # https://stackoverflow.com/questions/58014417/seeing-logs-of-dask-workers
                                      )
                print(f"Created a cluster {cluster}", flush=True)
                client = Client(cluster)
                print(f"Created a client {client}", flush=True)
                scheduler = client
                # https://github.com/dask/distributed/issues/3519
                # https://docs.dask.org/en/latest/configuration.html
                dask.config.set({"distributed.workers.memory.terminate": 0.75})
                print(f"Started dask cluster {client} with mem limit {memlimit}", flush=True)
            else:
                scheduler = 'synchronous'
        except (TypeError,ValueError) as ex:
            print(f"Exception raised when creating scheduler: {ex}", flush=True)
            nthreads = 1
            scheduler = 'synchronous'
    else:
        nthreads = 1
        scheduler = 'synchronous'

    target_chunksize = int(1e8)
    print(f"Target chunk size = {target_chunksize} (log10={np.log10(target_chunksize)})", flush=True)

    print(f"Using scheduler {scheduler} with {nthreads} threads", flush=True)

    cwd = os.getcwd()
    basepath = '/orange/adamginsburg/ALMA_IMF/2017.1.01355.L/imaging_results'
    os.chdir(basepath)
    print(f"Changed from {cwd} to {basepath}, now running cube stats assembly", flush=True)

    fn = '/orange/adamginsburg/ALMA_IMF/2017.1.01355.L/imaging_results/W43-MM1_B3_spw2_12M_spw2.image'

    modfn = fn.replace(".image", ".model")

    logtable = casaTable.read(f'{fn}/logtable').as_astropy_tables()[0]
    hist = logtable['MESSAGE']

    #ia.open(fn)
    #hist = ia.history(list=False)
    history = {x.split(":")[0]:x.split(": ")[1]
               for x in hist if ':' in x}
    history.update({x.split("=")[0]:x.split("=")[1].lstrip()
                    for x in hist if '=' in x})
    #ia.close()

    if os.path.exists(fn+".fits"):
        cube = SpectralCube.read(fn+".fits", format='fits', use_dask=True)
        cube.use_dask_scheduler(scheduler=scheduler, num_workers=num_workers)
    else:
        cube = SpectralCube.read(fn, format='casa_image', target_chunksize=target_chunksize)
        cube.use_dask_scheduler(scheduler=scheduler, num_workers=num_workers)
        # print(f"Rechunking {cube} to tmp dir", flush=True)
        # cube = cube.rechunk(save_to_tmp_dir=True)
        # cube.use_dask_scheduler(scheduler)

    print(cube)

    # try this as an experiment?  Maybe it's statistics that causes problems?
    #print(f"Computing cube mean with scheduler {scheduler} and sched args {cube._scheduler_kwargs}", flush=True)
    #mean = cube.mean()
    print(f"Computing cube statistics with scheduler {scheduler} and sched args {cube._scheduler_kwargs}", flush=True)
    stats = cube.statistics()
    print("finished cube stats", flush=True)
    dt()

    print("Computing mean")
    cube.mean()
    dt()

    if os.path.exists(modfn+".fits"):
        modcube = SpectralCube.read(modfn+".fits", format='fits', use_dask=True)
        modcube.use_dask_scheduler(scheduler=scheduler, num_workers=num_workers)
    else:
        modcube = SpectralCube.read(modfn, format='casa_image', target_chunksize=target_chunksize)
        modcube.use_dask_scheduler(scheduler=scheduler, num_workers=num_workers)
        # print(f"Rechunking {modcube} to tmp dir", flush=True)
        # modcube = modcube.rechunk(save_to_tmp_dir=True)
        # modcube.use_dask_scheduler(scheduler)

    print(modcube, flush=True)
    print(f"Computing model cube statistics with scheduler {scheduler} and sched args {modcube._scheduler_kwargs}", flush=True)
    modstats = modcube.statistics()
    dt()

    os.chdir(cwd)

    if threads and nthreads > 1 and cluster:
        client.close()
