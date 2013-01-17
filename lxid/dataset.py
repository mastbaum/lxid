'''Utilities for manipulating datasets.'''

from rat import dsreader, ROOT
import numpy as np

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


def create(h5file, name, files, fitter='scintFitter'):
    '''Create a new dataset from the given files, inside h5file.

    :param h5file: HDF5 file to add the dataset to
    :param name: Name of the new dataset
    :param files: List of ROOT file names
    :param fitter: Fit result to extract
    :returns: The newly-created HDF5 group
    '''
    if name in list(f):
        raise Exception('Group %s already exists!' % name)

    h5_ds = f.create_group(name)
    h5_ds_fit = h5_ds.create_dataset('fit', (1, 6), 'f', chunks=True)  # x y z r t e
    h5_ds_pmt = h5_ds.create_group('pmt')
    h5_ds_pmt_t = h5_ds_pmt.create_dataset('t', (1, 10000), 'f', chunks=True, compression='lzf')
    h5_ds_pmt_q = h5_ds_pmt.create_dataset('q', (1, 10000), 'f', chunks=True, compression='lzf')
    h5_ds_pmt_t_res = h5_ds_pmt.create_dataset('tres', (1, 10000), 'f', chunks=True, compression='lzf')

    i

    runt = ROOT.TChain('runT')

    ds = ROOT.RAT.DS.Root()
    t.SetBranchAddress('ds', ds)

    run = ROOT.RAT.DS.Run()
    runt.SetBranchAddress('run', run)
    runt.GetEvent(0)

    nevents = t.GetEntries()
    print '%i entries' % nevents

    # allocate enough for all events, crop later
    valid = np.zeros(shape=(nevents), dtype=np.bool)
    fit = np.zeros(shape=(nevents, 6), dtype=np.float32)
    pmt_t = np.zeros(shape=(nevents, 10000), dtype=np.float32)
    pmt_q = np.zeros(shape=(nevents, 10000), dtype=np.float32)
    pmt_t_res = np.zeros(shape=(nevents, 10000), dtype=np.float32)

    for i in range(nevents):
        t.GetEntry(i)

        if ds.GetEVCount() < 1:
            continue

        try:
            vertex = ds.GetEV(0).GetFitResult(fitter).GetVertex(0)
            pos = vertex.GetPosition()
            fit[i] = np.array([pos.X(), pos.Y(), pos.Z(), pos.Mag(), vertex.GetTime(), vertex.GetEnergy()])
            for ipmt in range(ds.GetEV(0).GetPMTCalCount()):
                pmt = ds.GetEV(0).GetPMTCal(ipmt)
                pmt_t[i][pmt.id] = pmt.sPMTt
                pmt_q[i][pmt.id] = pmt.sQHS
                #pmt_t_res[pmt.id] = pmt.sPMTt
            valid[i] = 1

        except Exception: #FitUnavailable:
            raise
            print 'warning: no fit %s available' % fitter
            continue

    n, _ = h5_ds_fit.shape
    n = 0 if n == 1 else n  # gotta start somewhere
    nvalid = len(valid[valid > 0])

    h5_ds_fit.resize((n+nvalid, 6))
    h5_ds_fit[-nvalid:] = fit[valid > 0]

    h5_ds_pmt_t.resize((n+nvalid, 10000))
    h5_ds_pmt_t[-nvalid:] = pmt_t[valid > 0]

    h5_ds_pmt_q.resize((n+nvalid, 10000))
    h5_ds_pmt_q[-nvalid:] = pmt_q[valid > 0]

    return h5_ds

if __name__ == '__main__':
    import h5py

    f = h5py.File('lxid.hdf5', 'a')
    filenames = ['/home/mastbaum/snoplus/tl208/data/pdf/tl208/run0/av_tl208-0.root']

    ds = create_dataset(f, 'tl208', filenames)

    print ds

