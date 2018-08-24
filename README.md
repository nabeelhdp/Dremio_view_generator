# HiveView2DremioVDS
Retrieve views from Hive database and push to Dremio as Virtual Datasets

Update `dremio_config.ini`

Set passwords for mysql and Dremio using the set password script
```
set_password_in_config.py <mysql|dremio> <your_password>
```

Run with 
```
python create_dremio_views.py
```
Output and ID of each vds created is stored in `vds_create_status.json` file. Any errors during creation also stored in same file.
