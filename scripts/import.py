#!/usr/bin/env python

import os
import sys
import ssl
import glob
import yaml
import dppylib
import dpimport
import logging
import argparse as ap
import collections as col
import dpimport.importer as importer
from dpimport.database import Database
from pymongo import DeleteMany, UpdateMany
from pymongo.errors import BulkWriteError

from multiprocessing import Pool
import signal

logger = logging.getLogger(__name__)

def RAISE(err):
    raise err


def _main(db1,f):
    dirname = os.path.dirname(f)
    basename = os.path.basename(f)
    # probe for dpdash-compatibility and gather information
    probe = dpimport.probe(f)
    if not probe:
        logger.debug('document is unknown %s', basename)
        return

    db=db1.connect()
    # nothing to be done
    if db.exists(probe):
        logger.info('document exists and is up to date %s', probe['path'])
        return
    logger.info('document does not exist or is out of date %s', probe['path'])
    # import the file
    logger.info('importing file %s', f)
    dppylib.import_file(db.db, probe)



def main():
    parser = ap.ArgumentParser()
    parser.add_argument('-c', '--config')
    parser.add_argument('-d', '--dbname', default='dpdata')
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('-n','--ncpu', type= int, default= 1,
        help='number of processes/threads to use')
    parser.add_argument('expr')
    args = parser.parse_args()

    level = logging.INFO
    if args.verbose:
        level = logging.DEBUG
    logging.basicConfig(level=level)

    with open(os.path.expanduser(args.config), 'r') as fo:
        config = yaml.load(fo, Loader=yaml.SafeLoader)
    
    db1 = Database(config, args.dbname)
    
    files=glob.glob(args.expr, recursive=True)

    if args.ncpu==1:

        for f in files:
            _main(db1,f)

    elif args.ncpu>1:

        sigint_handler= signal.signal(signal.SIGINT, signal.SIG_IGN)
        pool= Pool(args.ncpu)
        signal.signal(signal.SIGINT, sigint_handler)

        try:
            for f in files:
                pool.apply_async(_main, (db1,f), error_callback= RAISE)
        except KeyboardInterrupt:
            pool.terminate()
        else:
            pool.close()
        pool.join()


    db=db1.connect()
    logger.info('cleaning metadata')
    lastday = get_lastday(db.db)
    if lastday:
        clean_metadata(db.db, lastday)

def clean_metadata(db, max_days):
    studies = col.defaultdict()
    subjects = list()

    for subject in max_days:
        if subject['_id']['study'] not in studies:
            studies[subject['_id']['study']] = {}
            studies[subject['_id']['study']]['subject'] = []
            studies[subject['_id']['study']]['max_day'] = 0

            # if there are more than 2, drop unsynced
            metadata = list(db.metadata.find(
                {
                    'study' : subject['_id']['study']
                },
                {
                    '_id' : True,
                    'collection' : True,
                    'synced' : True
                }
            ))

            if len(metadata) > 1:
                for doc in metadata:
                    if doc['synced'] is False and 'collection' in doc:
                        db[doc['collection']].drop()
                    if doc['synced'] is False:
                        db.metadata.delete_many(
                            {
                                '_id': doc['_id']
                            }
                        )

        subject_metadata = col.defaultdict()
        subject_metadata['subject'] = subject['_id']['subject']
        subject_metadata['synced'] = subject['synced']
        subject_metadata['days'] = subject['days']
        subject_metadata['study'] = subject['_id']['study']

        studies[subject['_id']['study']]['max_day'] = studies[subject['_id']['study']]['max_day'] if (studies[subject['_id']['study']]['max_day'] >= subject['days'] ) else subject['days']

        studies[subject['_id']['study']]['subject'].append(subject_metadata)

    for study, subject in iter(studies.items()):
        bulk_metadata = []
        bulk_metadata = bulk_metadata + [UpdateMany({'study' : study}, {'$set' :
            {
                'synced' : True,
                'subjects' : studies[study]['subject'],
                'days' : studies[study]['max_day']
            }
        }, upsert=True)]
        bulk_metadata = bulk_metadata + [DeleteMany({'study' : study, 'synced' : False})]
        bulk_metadata = bulk_metadata + [UpdateMany({'study' : study }, {'$set' : {'synced' : False}})]
        try:
            db.metadata.bulk_write(bulk_metadata)
        except BulkWriteError as e:
            logger.error(e)

def get_lastday(db):
    return list(db.toc.aggregate([
        {
            '$group' : {
                '_id' : {
                    'study': '$study',
                    'subject' : '$subject'
                },
                'days' : {
                    '$max' : '$time_end'
                },
                'synced' : {
                    '$max' : '$updated'
                }
            }
        }
    ]))

def clean_toc(db):
    logger.info('cleaning table of contents')
    out_of_sync_tocs = db.toc.find(
        {
            'synced' : False
        },
        {
            '_id' : False,
            'collection' : True,
            'path' : True 
        }
    )
    
    for doc in out_of_sync_tocs:
        db[doc['collection']].delete_many(
            {
                'path' : doc['path']
            }
        )

    bulk = [DeleteMany({ 'synced' : False })]
    try:
        db.toc.bulk_write(bulk)
    except BulkWriteError as e:
        logger.error(e)

def clean_toc_study(db, study):
    logger.info('cleaning table of contents for {0}'.format(study))
    out_of_sync_tocs = db.toc.find(
        {
            'study' : study,
            'synced' : False
        },
        {
            '_id' : False,
            'collection' : True,
            'path' : True 
        }
    )
    for doc in out_of_sync_tocs:
        db[doc['collection']].delete_many(
            {
                'path' : doc['path']
            }
        )

    bulk = [DeleteMany({ 'study' : study, 'synced' : False })]
    try:
        db.toc.bulk_write(bulk)
    except BulkWriteError as e:
        logger.error(e)

if __name__ == '__main__':
    main()

