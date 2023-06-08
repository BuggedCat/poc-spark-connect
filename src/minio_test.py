from minio import Minio
from settings import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, DatalakeZones

client = Minio(
    endpoint="localhost:9000",
    access_key=AWS_ACCESS_KEY_ID,
    secret_key=AWS_SECRET_ACCESS_KEY,
    secure=False,
)

# Make 'asiatrip' bucket if not exist.
found = client.bucket_exists(DatalakeZones.BRONZE)
if found:
    print(f"Bucket '{DatalakeZones.BRONZE}' already exists")
else:
    client.make_bucket(DatalakeZones.BRONZE)


objs = client.list_objects(DatalakeZones.BRONZE)

for obj in objs:
    print(obj)
