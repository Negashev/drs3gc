# drs3gc

Docker registry s3 garbage collect

remove overwritten tags artifacts

tested only on minio

```bash
docker run -it \
--name drs3gc \
--restart=always \
-d -p 80:80 \
-e ENDPOINT='s3.minio.io' \
-e ACCESS_KEY='ACCESS_KEY' \
-e SECRET_KEY='SECRET_KEY' \
-e SECURE='0' \
negash/drs3gc
```