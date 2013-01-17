'''Utilities for manipulating datasets.'''

import os
import numpy as np
import h5py

class Cut(dict):
    '''Cut to be applied to data.

    All cuts are (min_value, max_value) tuples.

    :param e: Fit energy cut (MeV)
    :param r: Fit radius cut (mm)
    :param x: Fit X cut (mm)
    :param y: Fit Y cut (mm)
    :param z: Fit Z cut (mm)
    '''
    def __init__(self, e=None, r=None, x=None, y=None, z=None):
        self['e'] = e
        self['r'] = r
        self['x'] = x
        self['y'] = y
        self['z'] = z

    def as_tuple(self):
        '''Represent the cut as a tuple.

        :returns: (e, r, x, y, z, t, nhits, fitter) tuple, where all but
                  fitter are (min, max) tuples
        '''
        return (self['e'], self['r'], self['x'], self['y'], self['z'])

    def apply(self, vertex):
        '''Apply this cut.

        :param vertex: A RAT.DS.FitVertex
        :returns: True or False
        '''
        # we really should catch NoValueErrors thrown by the FitVertex getters
        if self['e'] is not None:
            e = vertex.GetEnergy()
            if e < self['e'][0] or e > self['e'][1]:
                return False

        if self['r'] is not None:
            r = vertex.GetPosition().Mag()
            if r < self['r'][0] or r > self['r'][1]:
                return False

        if self['x'] is not None:
            x = vertex.GetPosition().X()
            if x < self['x'][0] or x > self['x'][1]:
                return False

        if self['y'] is not None:
            y = vertex.GetPosition().Y()
            if y < self['y'][0] or y > self['y'][1]:
                return False

        if self['z'] is not None:
            z = vertex.GetPosition().Z()
            if z < self['z'][0] or z > self['z'][1]:
                return False

        return True


def create(h5file, name, files, cut=None, parallel=True):
    '''Create a new dataset from the given files, inside h5file.

    :param h5file: HDF5 file to add the dataset to
    :param name: Name of the new dataset
    :param files: List of ROOT file names
    :param cut: Cut to apply to the whole dataset
    :param parallel: If True, use a cluster to extract ROOT events
    :returns: The newly-created HDF5 group
    '''
    if name in list(h5file):
        raise Exception('Group %s already exists!' % name)

    h5_ds = h5file.create_group(name)
    h5_ds.attrs['events_total'] = 0
    h5_ds.attrs['events_triggered'] = 0
    h5_ds.attrs['events_reconstructed'] = 0
    h5_ds.attrs['events_pass'] = 0
    h5_ds_fit = h5_ds.create_dataset('fit', (1, 6), 'f', chunks=True)  # x y z r t e
    h5_ds_pmt = h5_ds.create_group('pmt')
    h5_ds_pmt_t = h5_ds_pmt.create_dataset('t', (1, 10000), 'f', chunks=True, compression='lzf')
    h5_ds_pmt_q = h5_ds_pmt.create_dataset('q', (1, 10000), 'f', chunks=True, compression='lzf')
    h5_ds_pmt_t_res = h5_ds_pmt.create_dataset('tres', (1, 10000), 'f', chunks=True, compression='lzf')

    def append_to_h5(event_tuple, ds):
        '''Resize h5 ds to fit new event data, and append it.'''
        counters, fit, pmt_t, pmt_q = event_tuple
        n, _ = ds['fit'].shape
        n = 0 if n == 1 else n  # gotta start somewhere
        nvalid = len(fit)

        ds.attrs['events_total'] += counters['events_total']
        ds.attrs['events_triggered'] += counters['events_triggered']
        ds.attrs['events_reconstructed'] += counters['events_reconstructed']
        ds.attrs['events_pass'] += counters['events_pass']

        h5_ds_fit.resize((n+nvalid, 6))
        h5_ds_fit[-nvalid:] = fit

        h5_ds_pmt_t.resize((n+nvalid, 10000))
        h5_ds_pmt_t[-nvalid:] = pmt_t

        h5_ds_pmt_q.resize((n+nvalid, 10000))
        h5_ds_pmt_q[-nvalid:] = pmt_q

    callback = lambda x: append_to_h5(x, h5_ds)

    if parallel:
        from utils import convert_events_parallel
        convert_events_parallel(files, cut, callback=callback)
    else:
        from utils import convert_events
        convert_events(files, cut, callback=callback)

    return h5_ds


if __name__ == '__main__':
    #if cache_dir is None:
    cache_dir = os.path.join(os.path.expanduser('~/.lxid'))

    if os.path.exists(cache_dir):
        if not os.path.isdir(cache_dir):
            raise Exception('cache location %s exists and is not a directory' % cache_dir)
    else:
        os.mkdir(cache_dir)

    h5file = h5py.File(os.path.join(cache_dir, 'lxid.hdf5'), 'a')
    cut = Cut(e=(2.555, 2.718))

    name = 'tl208'
    files = ['/home/mastbaum/snoplus/tl208/data/pdf/tl208/run0/av_tl208-0.root']
    ds = create(h5file, name, files, cut)

    print ds

