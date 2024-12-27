#!/bin/bash
#SBATCH --account="COMP90024"
#SBATCH --job-name="TESTCASE3"        # Job name
#SBATCH --nodes=1                 # Number of nodes
#SBATCH --cpus-per-task=1         # Number of tasks per node
#SBATCH --mem-per-cpu=200M		  # Memory allocation per cpu
#SBATCH --time=1:00:00            # Maximum runtime
#SBATCH --mail-user=sthanadanfra@student.unimelb.edu.au
#SBATCH --mail-type=ALL


module purge

module load foss/2022a Python/3.10.4 SciPy-bundle/2022.05


srun -n 1 python shsa_comp90024_as1_v1.86.py

my-job-stats -a -n -s
