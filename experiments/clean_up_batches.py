import subprocess

bashCommand = 'gcloud dataproc batches list --region="europe-west3"'
result = subprocess.run([bashCommand], capture_output=True, text=True, shell=True)
for i, l in enumerate(result.stdout.split("\n")):
    if len(l) == 0:
        continue
    if "BATCH_ID" in l:
        continue
    batch_id = l.split(" ")[0]
    status = l.split(" ")[-1]
    print(f"Removing {batch_id}")
    if status in ["SUCCEEDED", "FAILED"]:
        try:
            bashCommand = f'gcloud dataproc batches delete {batch_id} --region="europe-west3" --async'
            result = subprocess.run(
                [bashCommand], capture_output=True, text=True, shell=True
            )
        except Exception as e:
            print(e)
            break
