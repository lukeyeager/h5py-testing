#!/usr/bin/env python2

import argparse
import math
import progressbar
import time

import h5py
import numpy as np

DSET_NAME = 'dset'


def _calc_batches(count, batch_size):
    return int(math.ceil(count / float(batch_size)))


def write(filename, batched=False, batch_size=32):
    data_count = 10000
    data_shape = (3, 256, 256)
    data_type = np.uint8
    total_shape = (data_count,) + data_shape

    if batched:
        print 'Writing in batches ...'

        with h5py.File(filename, 'w') as f:
            dset = f.create_dataset(
                DSET_NAME,
                shape=(batch_size,) + data_shape,
                maxshape=total_shape,
                dtype=data_type,
                chunks=(batch_size,) + data_shape,
            )
        batches = _calc_batches(data_count, batch_size)
        count = 0
        pbar = progressbar.ProgressBar(
            widgets=[
                progressbar.Percentage(),
                progressbar.Bar(),
            ],
            maxval=data_count,
        ).start()
        for i in xrange(batches):
            with h5py.File(filename, 'a') as f:
                dset = f[DSET_NAME]

                start = i * batch_size
                stop = (i + 1) * batch_size
                if stop > data_count:
                    stop = data_count
                length = stop - start
                count += length
                dset.resize(stop, axis=0)
                for i in xrange(start, stop):
                    dset[i] = np.ones(data_shape, dtype=data_type) * (i % 255)
            pbar.update(count)
        pbar.finish()
        assert count == data_count
    else:
        print 'Writing all at once ...'
        with h5py.File(filename, 'w') as f:
            dset = f.create_dataset(
                DSET_NAME,
                shape=total_shape,
                maxshape=total_shape,
                dtype=data_type,
                chunks=(batch_size,) + data_shape,
            )
            for i in xrange(data_count):
                dset[i] = np.ones(data_shape, dtype=data_type) * (i % 255)


def read(filename, batched=False, batch_size=32):
    with h5py.File(filename, 'r') as f:
        dset = f[DSET_NAME]
        if batched:
            print 'Reading in batches ...'
            batches = _calc_batches(len(dset), batch_size)
            count = 0
            pbar = progressbar.ProgressBar(
                widgets=[
                    progressbar.Percentage(),
                    progressbar.Bar(),
                ],
                maxval=len(dset),
            ).start()
            for i in xrange(batches):
                start = i * batch_size
                stop = (i + 1) * batch_size
                if stop > len(dset):
                    stop = len(dset)
                for j in xrange(start, stop):
                    data = dset[j]
                    assert data[0][0][0] == (j % 255)
                    count += 1
                pbar.update(count)
            pbar.finish()
            assert count == len(dset), '%d != %d' % (count, len(dset))
        else:
            print 'Reading all at once ...'
            data = dset[...]
            for i in xrange(len(data)):
                assert data[i][0][0][0] == (i % 255)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('action')
    parser.add_argument('--batched', action='store_true')
    parser.add_argument('-f', '--filename', default='data.h5')
    args = parser.parse_args()

    start_time = time.time()
    if args.action == 'write':
        write(args.filename, batched=args.batched)
    elif args.action == 'read':
        read(args.filename, batched=args.batched)
    else:
        raise ValueError('Unknown action "%s"' % args.action)

    print 'Done in %f seconds.' % (time.time() - start_time,)
    print
