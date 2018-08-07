import os
import logging
import re

from apscheduler.triggers.cron import CronTrigger
from japronto import Application
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from minio import Minio
from minio.error import ResponseError
from rgc.registry.api import RegistryApi

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(u'%(levelname)-8s [%(asctime)s]  %(message)s'))
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Initialize minioClient with an endpoint and access/secret keys.
minioClient = Minio(os.getenv('ENDPOINT'),
                    access_key=os.getenv('ACCESS_KEY'),
                    secret_key=os.getenv('SECRET_KEY'),
                    secure=bool(os.getenv('SECURE', 1)))

registryClient = RegistryApi(
    user=os.getenv('REGISTRY_LOGIN'),
    token=os.getenv('REGISTRY_TOKEN')
)

BUCKET = os.getenv('BUCKET', 'registry')
REGISTRY_URL = os.getenv('REGISTRY_URL')
REGISTRY_PATH = os.getenv('REGISTRY_PATH', 'docker/registry/v2/repositories/')
REGISTRY_LATEST = []
WORK_QUEUE_REGISTRY_LATEST = []
SCAN_PATHS = [REGISTRY_PATH]
WORK_QUEUE_SCAN_PATHS = []


async def list_objects(prefix=None, recursive=False):
    return minioClient.list_objects_v2(BUCKET, prefix=prefix, recursive=recursive)


async def scan_bucket():
    global SCAN_PATHS
    global REGISTRY_LATEST
    global WORK_QUEUE_SCAN_PATHS
    global WORK_QUEUE_REGISTRY_LATEST
    if not len(SCAN_PATHS):
        return
    this_path = SCAN_PATHS.pop(0)
    if this_path in WORK_QUEUE_SCAN_PATHS:
        return
    WORK_QUEUE_SCAN_PATHS.append(this_path)
    try:
        for i in await list_objects(prefix=this_path):
            if i.object_name in SCAN_PATHS \
                    and i.object_name not in REGISTRY_LATEST \
                    and i.object_name not in WORK_QUEUE_REGISTRY_LATEST:
                continue
            if i.object_name.endswith('_layers/'):
                continue
            if i.object_name.endswith('_uploads/'):
                continue
            if i.object_name.endswith('_manifests/revisions/sha256/'):
                continue
            if i.object_name.endswith('index/sha256/'):
                continue
            if i.object_name.endswith('/current/link'):
                REGISTRY_LATEST.append(i.object_name)
                continue
            SCAN_PATHS.append(i.object_name)
            print(f"find: {i.object_name}")
    except Exception as e:
        print(e)
    WORK_QUEUE_SCAN_PATHS.remove(this_path)


async def add_registry_path():
    global SCAN_PATHS
    if REGISTRY_PATH not in SCAN_PATHS:
        SCAN_PATHS.append(REGISTRY_PATH)


async def cleanup_tag():
    global REGISTRY_LATEST
    if not len(REGISTRY_LATEST):
        return
    this_registry = REGISTRY_LATEST.pop(0)
    if this_registry in WORK_QUEUE_REGISTRY_LATEST:
        return
    WORK_QUEUE_REGISTRY_LATEST.append(this_registry)
    try:
        data = minioClient.get_object(BUCKET, this_registry)
        tmp_sha256 = ''
        for d in data.stream(32 * 1024):
            tmp_sha256 = d
        sha256 = tmp_sha256.decode('utf-8').replace('sha256:', '')
        print(this_registry)
        findall = re.findall(f'({REGISTRY_PATH})(.*)\/_manifests\/tags\/(.*)\/current\/link', this_registry)
        image, tag = ['regex-error', 'regex-error']
        if findall:
            this_path, image, tag = findall[0]
        print(f"cleanup ===> {image}:{tag}@{sha256}")
        index = this_registry[:-12] + 'index/sha256/'
        gc = False
        for to_remove_link in await list_objects(index):
            try:
                if index + sha256 + '/' != to_remove_link.object_name:
                    findall = re.findall('.*\/index\/sha256\/(.*)\/', to_remove_link.object_name)
                    if findall:
                        sha256_to_remove = findall[0]
                    print(f"remove ===> {image}:{tag}@{sha256_to_remove}")
                    registryClient.query(f"{REGISTRY_URL}/v2/{image}/manifests/sha256:{sha256_to_remove}", 'delete')
                    minioClient.remove_object(BUCKET, to_remove_link.object_name)
                    gc = True
                else:
                    print(f"stay ===> {to_remove_link.object_name}")
            except ResponseError as e:
                print(e)
        if gc:
            print(f"please run registry GC for remove blobs for {image}:{tag}")
    except Exception as e:
        print(e)
    WORK_QUEUE_REGISTRY_LATEST.remove(this_registry)


async def connect_scheduler():
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(add_registry_path, CronTrigger.from_crontab(os.getenv('DRS3GC_CRON_PATH', '0 0 * * 0')), max_instances=1)
    scheduler.add_job(cleanup_tag, 'interval', seconds=int(os.getenv('DRS3GC_SECONDS_CLEANUP', 5)), max_instances=10)
    scheduler.add_job(scan_bucket, 'interval', seconds=int(os.getenv('DRS3GC_SECONDS_SCAN', 1)), max_instances=10)
    scheduler.start()


async def index(request):
    global REGISTRY_LATEST
    return request.Response(json=REGISTRY_LATEST)


app = Application()
app.loop.run_until_complete(scan_bucket())
app.loop.run_until_complete(connect_scheduler())
router = app.router
router.add_route('/', index)
app.run(host='0.0.0.0', port=80, debug=True)
