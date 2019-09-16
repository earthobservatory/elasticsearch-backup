#!/bin/bash
set -ex && \
/home/ops/sciflo/backup_scripts/elasticsearch-backup/backup_custom.py /data/backup_slc_opds_only grq_v1.1_s1-iw_slc '{"query":{"bool":{"must":[{"term":{"metadata.tags.raw":"opendataset"}}]}}}' && \
aws s3 sync /data/backup_slc_opds_only/ s3://ntu-hysds-code/OPDS_SLC_Backup/ && \
/home/ops/sciflo/backup_scripts/elasticsearch-backup/backup_all.py /data/backup && \
aws s3 sync /data/backup/ s3://ntu-hysds-code/ALL_Backup/
