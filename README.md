# HiveView2DremioVDS
Retrieve views from Hive database and push to Dremio as Virtual Datasets

Update `dremio_config.ini`

Run with 
```
python create_dremio_views.py
```
Output and ID of each vds created is stored in `vds_create_status.json` file. Any errors during creation also stored in same file.
