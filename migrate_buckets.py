#!/usr/bin/env python
import os, requests, json, argparse, subprocess as sp



def migrate_buckets(from_bucket, to_bucket, backup_dir, target_grq_ip, dry_run=True, num_entries=None):
    """Restore ES index from backup docs and mapping."""
    id_key = 'id'

    # get files
    idx = os.path.basename(backup_dir)
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
            if line_ind > num_entries:
                print("Breaking out as line_index : %s  > num_entries: %s" % (line_ind, num_entries))
                break

            # execute setup if we are at first iteration
            if line_ind == 1 and not dry_run:
                # create index
                r = requests.put('http://%s:9200/%s' % (target_grq_ip,idx) )
                if r.status_code != 200:
                    j = r.json()
                    if r.status_code == 400 and j.get('error', '').startswith("IndexAlreadyExists"):
                        print("Created index %s " % idx)
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
                    r = requests.put('http://%s:9200/%s/_mapping/%s' % (target_grq_ip,idx, dt), data=json.dumps(m))
                    r.raise_for_status()
                    print("Updated mapping for %s " % idx)
                    r = requests.put('http://%s:9200/%s/_settings' % (target_grq_ip, idx), data=json.dumps(s))
                    r.raise_for_status()
                    print("Updated settings for %s " % idx)
                    doctype = dt


            dataset_md = json.loads(l)
            old_prod_url = ""
            new_prod_url = ""


            # 1. edit metadata elasticsearch
            for i in range(len(dataset_md["browse_urls"])):
                old_url = dataset_md["browse_urls"][i]
                new_url = old_url.replace(from_bucket, to_bucket)
                dataset_md["browse_urls"][i]=new_url
                print("Updated url: \n %s to \n %s".format(old_url, new_url))

            for i in range(len(dataset_md["urls"])):
                old_url = dataset_md["urls"][i]
                new_url = old_url.replace(from_bucket, to_bucket)
                dataset_md["urls"][i]=new_url
                print("Updated url: \n %s to \n %s".format(old_url, new_url))

                # get old and new product url
                if "s3" in old_url:
                    old_prod_url = old_url
                    new_prod_url = new_url

            # 2. aws s3 sync to transfer payload data across buckets
            if not dry_run:
                if old_prod_url:
                    sp.check_call("aws s3 sync %s %s".format(old_prod_url, new_prod_url), shell=True)
                else:
                    raise RuntimeError("Problem getting s3 product url from ES metadata: %s". format(dataset_md[id_key]))

            # 3. restore the updated indices in the target cluster grq
            if not dry_run:
                r = requests.put('http://%s:9200/%s/%s/%s' % (target_grq_ip, idx, doctype, dataset_md[id_key]), data=l)
                if r.status_code != 201:
                    print(r.status_code)
                    print(r.json())
                    continue
                else: r.raise_for_status()



def main():
    parser = argparse.ArgumentParser(description="Backup all ElasticSearch indexes.")
    parser.add_argument('--grqip', dest='target_grq_ip', default='localhost',
                        help="ElasticSearch IP address to migrate to")
    parser.add_argument('--from_bucket', dest='from_bucket', required=True,
                        help="s3 bucket to transfer from")
    parser.add_argument('--to_bucket', dest='to_bucket', required=True,
                        help="s3 bucket to transfer to")
    parser.add_argument('--backup_dir', dest='backup_dir', required=True,
                        help="the directory index to backup")
    parser.add_argument('--force', dest='force', action="store_true",
                        help="will execute if flag is up, if not, a dry run is performed")
    parser.add_argument('--num_limit', dest='num_entries', default=None,
                        help="number of first x entries to limit the transfer (for testing)")
    args = parser.parse_args()
    dry_run = not args.force
    migrate_buckets(args.from_bucket, args.to_bucket,args.backup_dir, args.target_grq_ip,
                    dry_run=dry_run, num_entries=int(args.num_entries))


if __name__ == "__main__":
    main()
