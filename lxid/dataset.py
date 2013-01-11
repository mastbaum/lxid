'''Utilities for defining data sets.'''

import os
import uuid
import pickle
import numpy as np

class FitUnavailable(Exception):
    '''Raised when a fit is not available for an event.'''
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)


class Event(dict):
   '''Representation of an event in numpy arrays.

   :param event: RAT.DS.EV object
   '''
   def __init__(self, event, fitter='scintFitter'):
       try:
           fitter_valid = event.GetFitResult(fitter).GetValid()
       except Exception:
           fitter_valid = False

       if not fitter_valid:
           raise FitUnavailable('No valid %s fit for event' % fitter)

       vertex = event.GetFitResult(fitter).GetVertex(0)

       self['x'] = np.float32(vertex.GetPosition().X())
       self['y'] = np.float32(vertex.GetPosition().Y())
       self['z'] = np.float32(vertex.GetPosition().Z())
       self['r'] = np.float32(vertex.GetPosition().Mag())
       self['t'] = np.float32(vertex.GetTime())
       self['e'] = np.float32(vertex.GetEnergy())

       self['pmt_t'] = np.empty(shape=(event.GetPMTCalCount(),), dtype=np.float32)
       self['pmt_q'] = np.empty_like(self['pmt_t'])

       for i in range(event.GetPMTCalCount()):
           pmt = event.GetPMTCal(i)
           self['pmt_t'][i] = pmt.sPMTt
           self['pmt_q'][i] = pmt.sQHS

class Cut(dict):
    '''Cut to be applied to data.

    All cuts are (min_value, max_value) tuples.

    :param e: Fit energy cut (MeV)
    :param r: Fit radius cut (mm)
    :param x: Fit X cut (mm)
    :param y: Fit Y cut (mm)
    :param z: Fit Z cut (mm)
    :param t: Fit time cut (ns)
    :param nhits: NHITs cut
    '''
    def __init__(self, e=None, r=None, x=None, y=None, z=None, t=None, nhits=None, fitter='scintFitter'):
        self['e'] = e
        self['r'] = r
        self['x'] = x
        self['y'] = y
        self['z'] = z
        self['t'] = t
        self['nhits'] = nhits
        self['fitter'] = fitter

    def as_tuple(self):
        '''Represent the cut as a tuple.

        :returns: (e, r, x, y, z, t, nhits, fitter) tuple, where all but
                  fitter are (min, max) tuples
        '''
        return (self['e'], self['r'], self['x'], self['y'], self['z'], self['t'], self['nhits'], self['fitter'])

    def apply(self, event):
        '''Apply this cut to an event.

        Extend this to handle other data formats (e.g. ntuples).

        :param event: A RAT.DS.EV
        :returns: Event properties if it passes, None if not
        :raises FitUnavailable: When event does not have a valid fit
        '''
        if self['nhits'] is not None:
            nhits = event.nhits
            if nhits < self['nhits'][0] or nhits > self['nhits'][1]:
                return False

        fitter = self['fitter']

        try:
            fitter_valid = event.GetFitResult(fitter).GetValid()
        except Exception:
            fitter_valid = False

        if not fitter_valid:
            raise FitUnavailable('No valid %s fit for event' % fitter)

        vertex = event.GetFitResult(fitter).GetVertex(0)

        # we really should catch NoValueErrors thrown by the FitVertex getters
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

        if self['t'] is not None:
            r = vertex.GetTime()
            if t < self['t'][0] or t > self['t'][1]:
                return False

        if self['e'] is not None:
            e = vertex.GetEnergy()
            if e < self['e'][0] or e > self['e'][1]:
                return False

        return True


class Dataset(object):
    '''A collection of files, with some cuts applied.

    A Dataset object organizes a collection of RAT files, applies cuts to the
    events, and caches the cut data sets.

    For now, a Dataset must fit into memory.

    :param name: String name of the data set
    :param filenames: A list of RAT files to add
    :param cache_dir: Directory where data sets are stored, default ~/.lxid
    '''
    def __init__(self, name, filenames=None, cache_dir=None):
        if cache_dir is None:
            cache_dir = os.path.join(os.path.expanduser('~/.lxid'))
        if os.path.exists(cache_dir):
            if not os.path.isdir(cache_dir):
                raise Exception('cache location %s exists and is not a directory' % cache_dir)
        else:
            os.mkdir(cache_dir)

        self.cache_dir = cache_dir
        self.name = name
        self.filenames = set(filenames)

        self.files_processed = []
        self.events_total = None
        self.events_triggered = None
        self.events_reconstructed = None

        self.cut = {}

        self._write()

    def append(self, filenames):
        '''Add more files to the data set.

        Existing cuts will be applied to the new files.

        :param filenames: List of new filenames
        '''
        self.filenames = self.filenames.union(set(filenames))
        self.apply_cuts([Cut(*cut) for cut in self.cut])

    def apply_cuts(self, cuts):
        '''Apply cuts to the dataset.

        The returned cut event dictionary is also stored in
        self.cut[cut.as_tuple()] and cached.

        :param cut: A list of Cut objects
        :returns: A dictionary with filtered events and some statistics
        '''
        from rat import dsreader

        if self.events_total is None:
            self.events_total = 0
        if self.events_triggered is None:
            self.events_triggered = 0
        if self.events_reconstructed is None:
            self.events_reconstructed = 0

        for cut in cuts:
            cut_tuple = cut.as_tuple()
            if cut_tuple not in self.cut:
                self.cut[cut_tuple] = {}

        for name in self.filenames:
            if name in self.files_processed:
                print '*',
                continue

            print '.',
            for ds in dsreader(name):
                self.events_total += 1
                if ds.GetEVCount() < 1:
                    continue

                # loop over EVs? retriggers, etc.? sigh...
                event = ds.GetEV(0)
                self.events_triggered += 1

                try:
                    for cut in cuts:
                        cut_tuple = cut.as_tuple()
                        if cut.apply(event):
                            self.cut[cut_tuple].setdefault('events', []).append(Event(event))
                    self.events_reconstructed += 1
                except FitUnavailable:
                    pass

            self.files_processed.append(name)

        self._write()

        print

    def _write(self, filename=None):
        '''Dump this Dataset object to disk.

        :param filename: Where to write the file, default: cache_dir/name.pickle
        '''
        with open(os.path.join(self.cache_dir, '%s.pickle' % self.name), 'wb') as f:
            pickle.dump(self, f)

