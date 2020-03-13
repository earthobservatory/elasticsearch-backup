set -e
while read p; do
  index_dir="/data/backup_frm_stage/${p}"
  if [ ! -z "$p" ] || [ -d $index_dir ]
  then
    cmd="~/elasticsearch-backup/migrate_buckets.py --grqip localhost --from_bucket ntu-hysds-dataset-stage --to_bucket ntu-hysds-dataset-hysds3 --backup_dir /data/backup_frm_stage/${p} --num_limit '' --force"
    echo "$cmd"
    eval $cmd
  fi
done < $1
