#!/usr/bin/env python
import os
import requests
import json
import re
import subprocess as sp
import argparse
'''
To run this, please first backup the acquisition elasticsearch indices with:
IN GRQ:
python ./elasticsearch-backup/backup_custom.py /data/acq_backup/ grq_v2.0_acquisition-s1-iw_slc '{"query":{"match_all":{}}}'

You will then get the /data/acq_backup/grq_v2.0_acquisition-s1-iw_slc/grq_v2.0_acquisition-s1-iw_slc.docs file for this script to input
'''
previous_id_regex = re.compile('acquisition-Sentinel.*_IW-esa_scihub')
new_id_fmt = 'acquisition-{}-esa_scihub'

def rename_acquisitions(from_bucket=None, to_bucket=None, backup_dir='', target_grq_ip='', new_idx ='grq_v2.1_acquisition-s1-iw_slc', met_updates={'version':'2.1'},dry_run=True, num_entries=None):
    """Restore ES index from backup docs and mapping."""

    id_key = 'id'

    idx = os.path.basename(backup_dir)

    if not new_idx:
        new_idx = idx

    # get files
    docs_file = os.path.join(backup_dir, '%s.docs' % idx)
    if not os.path.isfile(docs_file):
        raise RuntimeError("Failed to find docs file %s" % docs_file)
    mapping_file = os.path.join(backup_dir, '%s.mapping' % idx)
    if not os.path.isfile(mapping_file):
        raise RuntimeError("Failed to find mapping file %s" % mapping_file)
    settings_file = os.path.join(backup_dir, '%s.settings' % idx)
    if not os.path.isfile(settings_file):
        raise RuntimeError("Failed to find settings file %s" % settings_file)

    # import docs
    line_ind = 0
    with open(docs_file) as f:
        for l in f:
            line_ind +=1

            # break out if num_entries requested reached
            if num_entries:
                num_entries = int(num_entries)
                if line_ind > num_entries:
                    print("Breaking out as line_index : %s  > num_entries: %s" % (line_ind, num_entries))
                    break

            # execute setup if we are at first iteration
            if line_ind == 1:
                # create index
                if not dry_run:
                    r = requests.put('http://%s:9200/%s' % (target_grq_ip,new_idx) )
                    if r.status_code != 200:
                        j = r.json()
                        if r.status_code == 400 and j.get('error', '').startswith("IndexAlreadyExists"):
                            print("Created index %s " % new_idx)
                            pass
                        else:
                            r.raise_for_status()

                # put mapping and settings
                with open(mapping_file) as f:
                    mapping = json.load(f)
                if len(mapping[idx]['mappings']) > 2:
                    raise RuntimeError("More than two doctype found. Will not be able to know which to restore to.")
                with open(settings_file) as f:
                    settings = json.load(f)

                doctype = None
                for dt in mapping[idx]['mappings']:
                    m = mapping[idx]['mappings'][dt]
                    if idx not in settings or 'settings' not in settings.get(idx, {}):
                        raise RuntimeError("Failed to find settings for index %s." % idx)
                    s = settings[idx]['settings']
                    if not dry_run:
                        r = requests.put('http://%s:9200/%s/_mapping/%s' % (target_grq_ip,idx, dt), data=json.dumps(m))
                        r.raise_for_status()
                        print("Updated mapping for %s " % idx)

                        r = requests.put('http://%s:9200/%s/_settings' % (target_grq_ip, idx), data=json.dumps(s))
                        print("Updated settings for %s " % idx)

                    if "default" not in dt:
                        doctype = dt
                        print("Updated doctype from mapping as %s " % doctype)

                if doctype is None:
                    raise RuntimeError("Failed to find doctype for index %s." % idx)


            dataset_md = json.loads(l)

            # DO NOT backup opendataset!
            tags = dataset_md["metadata"].get("tags", '')

            if "opendataset" in tags:
                print("Skipping opendataset data: %s, %s" % (dataset_md[id_key],dataset_md.get("browse_urls", [""])[0]))
                continue


            print("Migrating data: %s" % (dataset_md[id_key]))
            old_bucket_url = {"prod":"", "browse":""}

            # 1. edit metadata elasticsearch

            # This is for acquisitions to rename and update versions
            match = previous_id_regex.search(dataset_md["id"])
            if match:
                dataset_md.update({"id": new_id_fmt.format(dataset_md['metadata']['title'])})
                dataset_md.update(met_updates)

            # This is for browse urls to change buckets
            if len(dataset_md["urls"])  == 0:
                print("Skipping s3 bucket url update for %s since %s contains only ES metadata" % (
                dataset_md[id_key], idx ))

            for i in range(len(dataset_md["browse_urls"])):
                old_url = dataset_md["browse_urls"][i]
                new_url = old_url.replace(from_bucket, to_bucket)
                dataset_md["browse_urls"][i]=new_url

                print("Updated url: \n %s to \n %s" % (old_url, new_url))
                # get old browse url
                if "s3" in old_url:
                    old_bucket_url["browse"] = old_url

            for i in range(len(dataset_md["urls"])):
                old_url = dataset_md["urls"][i]
                new_url = old_url.replace(from_bucket, to_bucket)
                dataset_md["urls"][i]=new_url
                print("Updated url: \n %s to \n %s" % (old_url, new_url))

                # get old product url
                if "s3" in old_url:
                    old_bucket_url["prod"] = old_url

            # 2. aws s3 sync to transfer payload data across buckets
            for key,old_url in old_bucket_url.items():
                if old_url:
                    url_regex = re.compile('(s3:\/\/).*:80\/(.*)')
                    match = url_regex.search(old_url)
                    if match:
                        old_real_url = "{}{}".format(match.group(1), match.group(2))
                        new_real_url = old_real_url.replace(from_bucket, to_bucket)
                        if not dry_run:
                            sp.check_call("aws s3 sync %s %s" % (old_real_url, new_real_url), shell=True)
                    else:
                        raise RuntimeError("Problem getting s3 %s url from ES metadata: %s, url: %s" % (key,dataset_md[id_key],old_url))
                elif len(dataset_md["urls"])  == 0:
                    print("Skipping aws s3 sync bucket migration for %s since %s contains only ES metadata" % (dataset_md[id_key],idx))
                else:
                    raise RuntimeError("Problem getting s3 product url from ES metadata: %s" % (dataset_md[id_key]))


            # 3. restore the updated indices in the target cluster grq
            es_put_url = 'http://%s:9200/%s/%s/%s' % (target_grq_ip, new_idx, doctype, dataset_md[id_key])
            print("Putting metadata into %s " % es_put_url)
            if not dry_run:
                r = requests.put(es_put_url, data=json.dumps(dataset_md))
                print("Updated ES metadata: %s " % dataset_md[id_key])
                if r.status_code != 201:
                    print(r.status_code)
                    print(r.json())
                    continue
                else: r.raise_for_status()


def main():
    parser = argparse.ArgumentParser(description="Backup all ElasticSearch indexes.")
    parser.add_argument('--grqip', dest='target_grq_ip', default='localhost',
                        help="ElasticSearch IP address to migrate to")
    parser.add_argument('--new_idx', dest='new_idx', default='grq_v2.1_acquisition-s1-iw_slc',
                        help="new grq index to transfer to")
    parser.add_argument('--backup_dir', dest='backup_dir', required=True,
                        help="the directory index to backup")
    parser.add_argument('--force', dest='force', action="store_true",
                        help="will execute if flag is up, if not, a dry run is performed")
    parser.add_argument('--num_limit', dest='num_entries', default=None,
                        help="number of first x entries to limit the transfer (for testing)")
    args = parser.parse_args()
    dry_run = not args.force


    # migrate_buckets(args.from_bucket, args.to_bucket,args.backup_dir, args.target_grq_ip,
    #                 dry_run=dry_run, num_entries=args.num_entries)

    rename_acquisitions(backup_dir=args.backup_dir, target_grq_ip=args.target_grq_ip,
                        new_idx=args.new_idx, met_updates={'version': 'v2.1', 'system_version':'v2.1'}, dry_run=dry_run,
                        num_entries=args.num_entries)


if __name__ == "__main__":
    main()
