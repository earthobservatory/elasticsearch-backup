#!/bin/bash
/home/ops/sciflo/backup_scripts/elasticsearch-backup/backup_custom.py /home/ops/sciflo/backup_scripts/backup_slc_opds_only grq_v1.1_s1-iw_slc '{"query":{"bool":{"must":[{"term":{"metadata.tags.raw":"opendataset"}}]}}}'
aws s3 sync /home/ops/sciflo/backup_scripts/backup_slc_opds_only s3://ntu-hysds-code/OPDS_SLC_Backup
