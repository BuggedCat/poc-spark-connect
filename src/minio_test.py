from minio import Minio
from settings import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

client = Minio(
    "localhost:9000",
    access_key=AWS_ACCESS_KEY_ID,
    secret_key=AWS_SECRET_ACCESS_KEY,
    secure=False,
)

# Make 'asiatrip' bucket if not exist.
found = client.bucket_exists("teste")
if found:
    print("Bucket 'teste' already exists")
else:
    client.make_bucket("teste")


list(client.list_objects("teste"))
