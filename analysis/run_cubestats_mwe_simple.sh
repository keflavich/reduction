#!/bin/bash
#SBATCH --mail-type=NONE          # Mail events (NONE, BEGIN, END, FAIL, ALL)
#SBATCH --mail-user=adamginsburg@ufl.edu     # Where to send mail
#SBATCH --ntasks=8                    # Run on a single CPU
#SBATCH --nodes=1
#SBATCH --mem=16gb                     # Job memory request
#SBATCH --time=96:00:00               # Time limit hrs:min:sec
#SBATCH --output=cube_stats_grid_%j.log
#SBATCH --export=ALL
#SBATCH --job-name=cube_stats_grid_mwe_simple
#SBATCH --qos=adamginsburg
#SBATCH --account=adamginsburg
pwd; hostname; date

export WORK_DIR="/orange/adamginsburg/ALMA_IMF/reduction/analysis"
export WORK_DIR="/orange/adamginsburg/ALMA_IMF/2017.1.01355.L/imaging_results"

module load git

which python
which git

git --version
echo $?



export CASA=/orange/adamginsburg/casa/casa-pipeline-release-5.6.1-8.el7/bin/casa
export IPYTHON=/orange/adamginsburg/miniconda3/envs/casa6_py36/bin/ipython 

# casatools are used
export IPYTHON=/orange/adamginsburg/miniconda3/envs/casa61_py36/bin/ipython

# but now they're obsolete!  Thanks casaformatsio!
export IPYTHON=/orange/adamginsburg/miniconda3/envs/python39/bin/ipython


cd ${WORK_DIR}
python /orange/adamginsburg/ALMA_IMF/reduction/reduction/getversion.py

cd ${WORK_DIR}
echo ${WORK_DIR}
echo ${LINE_NAME} ${BAND_NUMBERS}

export ALMAIMF_ROOTDIR="/orange/adamginsburg/ALMA_IMF/reduction/"
export SCRIPT_DIR="${ALMAIMF_ROOTDIR}/analysis"
export PYTHONPATH=$SCRIPT_DIR

echo $LOGFILENAME

export NO_PROGRESSBAR='True'
export ENVIRON='BATCH'
export JOBNAME=cube_stats_grid
export jobname=$JOBNAME

# TODO: get this from ntasks?
#export DASK_THREADS=8
export DASK_THREADS=$SLURM_NTASKS

env

${IPYTHON} ${SCRIPT_DIR}/cubestats_mwe_simple.py &
PID=$!
/orange/adamginsburg/miniconda3/bin/python ${ALMAIMF_ROOTDIR}/reduction/slurm_scripts/monitor_memory.py ${PID}

