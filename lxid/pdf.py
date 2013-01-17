'''Probability distribution functions.'''

import itertools
import numpy as np


def flatten(listOfLists):
    "Flatten one level of nesting"
    return itertools.chain.from_iterable(listOfLists)


def intlen(x):
    try:
        return len(x)
    except TypeError:
        return 0


def make_pdf(data, observables, bins):
    assert(len(observables) == len(bins))

    # turn something like [1,2], [[1,2,3], [4,5]], [3,4] into
    # [1,1,3], [1,2,3], [1,3,3], [2,4,4], [2,5,4]
    # yeah...
    a = [map(lambda x: x[o], data) for o in observables]
    b = np.array(list(flatten(map(lambda x: zip(*x), [[np.tile(z[i], max(1, sum(map(intlen, z[:i]+z[i+1:])))) for i in range(len(z))] for z in zip(*a)]))))

    h, e = np.histogramdd(b, bins=bins)

    # normalize within r bin... generalize me
    h = np.apply_along_axis(lambda x: x/np.sum(x), 1, h)

    # set zero-content bins to 0.1 * minimum nonzero bin
    min_val = np.min(h[h > 0])
    h[(h == 0)] = min_val / 10

    return h, e


if __name__ == '__main__':
    import dataset
    import glob

    try:
        d = dataset.load('tl208')
    except IOError:
        d = dataset.Dataset('tl208', filenames=glob.glob('/home/mastbaum/snoplus/tl208/data/pdf/tl208/run0/av_tl208-0.root'))

    d.append(glob.glob('/home/mastbaum/snoplus/tl208/data/pdf/tl208/run1/av_tl208-*.root'))

    cut = dataset.Cut(e=(2.555,2.718))
    d.apply_cuts([cut])

    events = d.cut[cut.as_tuple()]['events']

    h, e = make_pdf(events, ['r', 'pmt_t_res'], (10, 500,))

    import matplotlib.pyplot as plt
    import matplotlib.cm as cm
    from matplotlib.colors import LogNorm
    xedges, yedges = e
    extent = [yedges[0], yedges[-1], xedges[-1], xedges[0]]
    import matplotlib.pyplot as plt
    plt.imshow(h, aspect='auto', extent=extent, cmap=cm.jet, norm=LogNorm(), interpolation='nearest')
    plt.colorbar()
    plt.show()

