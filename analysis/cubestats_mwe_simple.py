from spectral_cube import SpectralCube
import time

fn = '/orange/adamginsburg/ALMA_IMF/2017.1.01355.L/imaging_results/W43-MM1_B3_spw2_12M_spw2.image'

for target_chunksize in (1e7,1e8,1e6):
    for scheduler, num_workers in (('synchronous', 1), ('threads', 8)):
        t0 = time.time()
        print(scheduler, num_workers, target_chunksize, flush=True)
        cube = SpectralCube.read(fn, format='casa_image', target_chunksize=target_chunksize)
        cube.use_dask_scheduler(scheduler=scheduler, num_workers=num_workers)
        stats = cube.statistics()
        print(f"success in {time.time()-t0}s", flush=True)

if __name__ == "__main__":

    import dask
    import dask.distributed
    print(f"dask version: {dask.__version__}")
    print(f"dask distrib version: {dask.distributed.__version__}")

    from dask.distributed import Client, LocalCluster

    for nthreads, n_workers in ((8,1), (4,2),):
        memlimit = '16GB'

        cluster = LocalCluster(n_workers=n_workers,
                               threads_per_worker=int(nthreads),
                               #memory_target_fraction=0.60,
                               #memory_spill_fraction=0.65,
                               #memory_pause_fraction=0.7,
                               memory_limit=memlimit,
                               silence_logs=False, # https://stackoverflow.com/questions/58014417/seeing-logs-of-dask-workers
                              )
        print(f"Created a cluster {cluster}", flush=True)
        scheduler = Client(cluster)
        print(f"Created a client {scheduler}", flush=True)
        # https://github.com/dask/distributed/issues/3519
        # https://docs.dask.org/en/latest/configuration.html
        #dask.config.set({"distributed.workers.memory.terminate": 0.75})
        print(f"Started dask cluster {scheduler} with mem limit {memlimit}", flush=True)

        t0 = time.time()
        print(scheduler, num_workers, target_chunksize)
        cube = SpectralCube.read(fn, format='casa_image', target_chunksize=target_chunksize)
        cube.use_dask_scheduler(scheduler=scheduler)
        stats = cube.statistics()
        print(f"success in {time.time()-t0}s")
