'''Various utilities'''

def events_from_ds(filename, cut=None, fitter='scintFitter'):
    '''Convert a ROOT file to a numpy array format.

    In the returned tuple:

        counters: dict with events_total, events_triggered,
            events_reconstructed, and events_pass (passing cut, if any)

        fit: ndarray of shape (nevents, 6), where columns are fit X Y Z R T E

        pmt_t: ndarray of shape (nevents, 10000) with per-PMT hit times

        pmt_q: ndarray of shape (nevents, 10000) with per-PMT QHS charge

        pmt_tres: ndarray of shape (nevents, 10000) with PMT time residuals

    :param filename: Name of ROOT file to extract
    :param cut: *optional* Cut object to apply to data
    :param fitter: Name of fit result to extract
    :returns: (counters, fit, pmt_t, pmt_q) tuple
    '''
    from rat import dsreader, ROOT
    import numpy as np

    counters = {
        'events_total': 0,
        'events_triggered': 0,
        'events_reconstructed': 0,
        'events_pass': 0
    }

    tree = ROOT.TChain('T')
    tree.Add(filename)

    nevents = tree.GetEntries()
    ds = ROOT.RAT.DS.Root()
    tree.SetBranchAddress('ds', ds)

    runtree = ROOT.TChain('runT')
    runtree.Add(filename)

    run = ROOT.RAT.DS.Run()
    runtree.SetBranchAddress('run', run)
    runtree.GetEvent(0)

    dscint, dav, dwater = ROOT.Double(0), ROOT.Double(0), ROOT.Double(0)

    # allocate enough for all events, crop later
    valid = np.zeros(shape=(nevents), dtype=np.bool)
    fit = np.zeros(shape=(nevents, 6), dtype=np.float32)
    pmt_t = np.zeros(shape=(nevents, 10000), dtype=np.float32)
    pmt_q = np.zeros(shape=(nevents, 10000), dtype=np.float32)
    pmt_tres = np.zeros(shape=(nevents, 10000), dtype=np.float32)

    for i in range(nevents):
        tree.GetEvent(i)

        counters['events_total'] += 1
        if ds.GetEVCount() < 1:
            continue

        counters['events_triggered'] += 1

        try:
            if not ds.GetEV(0).GetFitResult(fitter).GetValid():
                continue

            vertex = ds.GetEV(0).GetFitResult(fitter).GetVertex(0)
            pos = vertex.GetPosition()
            this_fit = np.array([pos.X(), pos.Y(), pos.Z(), pos.Mag(), vertex.GetTime(), vertex.GetEnergy()], dtype=np.float32)

            counters['events_reconstructed'] += 1

            if cut is not None and not cut.apply(vertex):
                continue

            counters['events_pass'] += 1
            fit[i] = this_fit

            for ipmt in range(ds.GetEV(0).GetPMTCalCount()):
                pmt = ds.GetEV(0).GetPMTCal(ipmt)
                pmt_t[i][pmt.id] = pmt.sPMTt
                pmt_q[i][pmt.id] = pmt.sQHS

                # time residuals
                pmt_pos = run.GetPMTProp().GetPos(pmt.id)
                run.GetStraightLinePath().CalcByPosition(vertex.GetPosition(), pmt_pos, dscint, dav, dwater)
                tof = run.GetEffectiveVelocityTime().CalcByDistance(dscint, dav, dwater)
                pmt_tres[i][pmt.id] = pmt.sPMTt - vertex.GetTime() - tof

            valid[i] = 1

        except Exception as e:
            print 'warning: no fit %s available (%s)' % (fitter, e)
            continue

    fit = fit[valid > 0]
    pmt_t = pmt_t[valid > 0]
    pmt_q = pmt_q[valid > 0]
    pmt_tres = pmt_tres[valid > 0]

    return counters, fit, pmt_t, pmt_q, pmt_tres


def convert_events(files, cut=None, callback=None):
    '''Read ROOT files and convert their events to a numpy array format.

    This is done one file at a time -- SLOWLY. Consider using
    convert_events_parallel.

    :param files: A list of ROOT file names
    :param cut: Cut to apply to events
    :param callback: Called with each event
    :returns: Converted events if callback is not provided
    '''
    from lxid.dataset import Cut

    if cut is None:
        cut = Cut()

    results = None
    if callback is None:
        results = []
        callback = results.append

    for filename in files:
        o = events_from_ds(filename, cut)
        callback(o)

    return results


def convert_events_parallel(files, cut=None, callback=None, context=None):
    '''Read ROOT files and convert their events to a numpy array format.

    This is done in parallel across a cluster. Worker processes
    (i.e. bin/convert_events.py) should be started before calling this.

    :param files: A list of ROOT file names
    :param cut: Cut to apply to events
    :param callback: Called with each event
    :param context: A ZeroMQ context
    :returns: Converted events if callback is not provided
    '''
    from lxid.parallel import Ventilator, Sink
    from lxid.dataset import Cut
    import zmq

    _ = raw_input('Press enter to continue when workers are running...')

    if context is None:
        context = zmq.Context()

    ipc_address = 'ipc:///tmp/lxid0'

    sink_data = context.socket(zmq.PULL)
    sink_data.bind(ipc_address)

    if cut is None:
        cut = Cut()

    tasks = [(filename, cut) for filename in files]

    v = Ventilator(tasks)
    v.start()

    s = Sink(len(tasks), output_address=ipc_address)
    s.start()

    results = None
    if callback is None:
        results = []
        callback = results.append

    # listen for processed events from the sink
    while True:
        try:
            o = sink_data.recv_pyobj()
        except Exception:
            o = sink_data.recv_pyobj()
        if o == None:
            break
        callback(o)

    v.join()
    s.join()

    return results

