'''
Moved to xclimate
'''

import csv
import json
import pickle
from dask_jobqueue import PBSCluster
from dask.distributed import Client

import numpy as np
import matplotlib.pyplot as plt

################
##### Dask #####
################

def get_ClusterClient(
        ncores=1,
        nmem='25GB',
        walltime='01:00:00',
        account='UWAS0155'):
    """
    Code from Daniel Kennedy
    More info about Dask on HPC - https://ncar.github.io/dask-tutorial/notebooks/05-dask-hpc.html
    """
    cluster = PBSCluster(
        cores=ncores,              # The number of cores you want
        memory=nmem,               # Amount of memory
        processes=ncores,          # How many processes
        queue='casper',            # Queue name
        resource_spec='select=1:ncpus=' +\
        str(ncores)+':mem='+nmem,  # Specify resources
        account=account,           # Input your project ID here
        walltime=walltime,         # Amount of wall time
        interface='ext',           # Interface to use
    )

    client = Client(cluster)
    return cluster, client

###################
##### General #####
###################

def save_dict(d, fname):
    '''
    Add docstring
    '''
    if fname.split('.')[-1] == 'csv':
        with open(fname, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            for key, value in d.items():
                writer.writerow([key, value])

    elif fname.split('.')[-1] == 'json':
        with open(fname, 'w', encoding='utf-8') as f:
            json.dump(d, f, indent=4)

    elif fname.split('.')[-1] == 'pickle' or fname.split('.')[-1] == 'pkl':
        with open(fname, 'wb') as f:
            pickle.dump(d, f)

    print('saved', fname)


def load_dict(fname):
    '''
    Add docstring
    '''
    loaded_data = None
    if fname.split('.')[-1] == 'csv':
        with open(fname, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            loaded_data = {rows[0]: rows[1] for rows in reader}

    elif fname.split('.')[-1] == 'json':
        with open(fname, 'r', encoding='utf-8') as f:
            loaded_data = json.load(f)

    elif fname.split('.')[-1] == 'pickle' or fname.split('.')[-1] == 'pkl':
        with open(fname, 'rb') as f:
            loaded_data = pickle.load(f)

    # print('loaded', fname)
    return loaded_data


####################
##### Plotting #####
####################

def get_discrete_colors(cmap_name, n, trim=0.15):
    ''' Get n discrete colors from a colormap '''
    cmap = plt.get_cmap(cmap_name)
    colors = [cmap(i) for i in np.linspace(trim, 1-trim, n)]
    return colors


def match_axlim(axs, xy):
    ''' Match x or y limits of multiple axes '''
    lims = []
    for ax in axs:
        lims.append(ax.get_xlim() if xy == 'x' else ax.get_ylim())
    lims = np.array(lims)
    lims = [lims.min(), lims.max()]
    for ax in axs:
        if xy == 'x':
            ax.set_xlim(lims)
        else:
            ax.set_ylim(lims)


def symmetric_axlim(ax, xy):
    ''' Set x or y limits symmetrically '''
    lim = ax.get_xlim() if xy == 'x' else ax.get_ylim()
    lim = np.abs(lim).max()
    if xy == 'x':
        ax.set_xlim(-lim, lim)
    else:
        ax.set_ylim(-lim, lim)


###########################################
##### Parameter Ranking and Selection #####
###########################################
